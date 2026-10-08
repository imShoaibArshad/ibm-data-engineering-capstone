"""Stage the UCI Online Retail II workbook into the transaction fact feed.

Why this is the hard part
-------------------------
1,067,365 rows across two worksheets, with the sort of dirt that actually
turns up in a vendor export rather than in a textbook:

* ``StockCode`` and ``Invoice`` mix integers with strings (``85048`` vs
  ``'79323P'``, ``489434`` vs ``'C489434'``) -- a plain ``astype(str)`` on a
  column Excel filled numerically can yield ``'85048.0'``.
* 22.8% of rows have no ``Customer ID``.
* 19,494 rows are cancellations (invoice number prefixed ``C``).
* 22,950 rows carry negative quantities (returns).
* 6,207 rows have a non-positive price.
* 4,382 descriptions are blank or whitespace-only (' WHITE CHERRY LIGHTS').

Design decision: **keep every business event, and flag it.**
Dropping cancellations and returns at staging time is a common mistake -- it
makes revenue unreconcilable, because the returns that offset it have been
destroyed. Instead the fact table carries ``is_cancelled`` / ``is_return`` /
``is_valid_sale`` so downstream marts can choose their own definition of a
sale while the audit trail survives.

Duplicates are the one category that *is* removed, because they are not
business events at all -- they are a defect in the published file. Profiling
found two distinct causes, reported separately rather than lumped together:

* **Worksheet overlap.** The vendor splits the log into ``Year 2009-2010``
  (2009-12-01 .. 2010-12-09) and ``Year 2010-2011`` (2010-12-01 ..
  2011-12-09). Those ranges **overlap by nine days**, so every transaction
  in 2010-12-01..2010-12-09 is published twice. 22,202 duplicate groups
  straddle the two sheets.
* **Within-sheet repetition.** 10,705 more groups repeat inside a single
  sheet, concentrated in large invoices (mean 128 duplicate rows per
  invoice) where many lines duplicate together -- an export artefact rather
  than a customer buying the same line twice.

Both are counted and reported; neither is silently discarded.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from ..config import AppConfig

# Published worksheet columns -> snake_case warehouse columns.
COLUMN_MAP: dict[str, str] = {
    "Invoice": "invoice",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}

SOURCE_XLSX = "online_retail_II.xlsx"

# The workbook takes ~25 s to parse. Read it once, cache as Parquet, and let
# every later stage/quality/report run iterate in milliseconds. The fingerprint
# invalidates the cache automatically if the vendor re-publishes the file.
SOURCE_CACHE = "_source_transactions.parquet"
SOURCE_FP = "_source_transactions.fingerprint.json"
STAGING_NAME = "stg_transactions.parquet"
STATS_NAME = "stg_transactions.stats.json"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _fingerprint(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def to_id_string(series: pd.Series) -> pd.Series:
    """Coerce a mixed Excel id column to clean nullable strings.

    Handles the three shapes such columns arrive in: native strings, whole
    numbers Excel stored numerically, and blanks. The ``85048.0`` repair is
    the point -- without it, product codes silently become floats and stop
    joining to anything.
    """
    out = series.astype("string")
    floaty = out.str.match(r"^-?\d+\.0$", na=False)
    if bool(floaty.any()):
        out = out.mask(floaty, out.str.replace(r"\.0$", "", regex=True))
    out = out.str.strip()
    return out.replace("", pd.NA)


def _arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce heterogeneous identifier columns before any Parquet write.

    ``Invoice`` and ``StockCode`` are genuinely mixed -- ``489434`` alongside
    ``'C489449'``, ``85048`` alongside ``'79323P'``. Left alone, Arrow infers
    ``int64`` from the leading values and raises ``ArrowInvalid`` halfway
    through writing a 1M-row file. Normalising here (rather than only in
    ``clean``) keeps the intermediate cache serialisable too.
    """
    for col in ("Invoice", "StockCode"):
        if col in df.columns:
            df[col] = to_id_string(df[col])
    if "Customer ID" in df.columns:
        df["Customer ID"] = pd.to_numeric(df["Customer ID"], errors="coerce")
    for col in ("Description", "Country"):
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def read_source(cfg: AppConfig, force: bool = False) -> pd.DataFrame:
    """Union both worksheets into one frame, with a Parquet cache."""
    xlsx = cfg.raw_dir / SOURCE_XLSX
    if not xlsx.exists():
        raise FileNotFoundError(
            f"{xlsx} not found -- run `retailde extract` first to populate the raw zone."
        )

    cache = cfg.staging_dir / SOURCE_CACHE
    fp_file = cfg.staging_dir / SOURCE_FP

    if not force and cache.exists() and fp_file.exists():
        try:
            if json.loads(fp_file.read_text(encoding="utf-8")) == _fingerprint(xlsx):
                return pd.read_parquet(cache)
        except (json.JSONDecodeError, OSError):
            pass  # corrupt cache -> rebuild

    sheets: dict[str, pd.DataFrame] = pd.read_excel(xlsx, sheet_name=None)
    frames = []
    for name, frame in sheets.items():
        copy = frame.copy()
        copy["source_sheet"] = name
        frames.append(copy)
    combined = _arrow_safe(pd.concat(frames, ignore_index=True))

    cache.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(cache, index=False)
    fp_file.write_text(json.dumps(_fingerprint(xlsx)), encoding="utf-8")
    return combined


# ---------------------------------------------------------------------------
# cleaning
# ---------------------------------------------------------------------------

def clean(source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Apply the staging rules. Returns (frame, rejection statistics)."""
    raw_rows = len(source)

    unknown = [c for c in source.columns if c not in COLUMN_MAP and c != "source_sheet"]
    if unknown:
        raise ValueError(
            f"source gained unexpected columns {unknown}; COLUMN_MAP must be updated "
            f"before this data can be trusted downstream"
        )
    missing = [c for c in COLUMN_MAP if c not in source.columns]
    if missing:
        raise ValueError(f"source is missing expected columns {missing}")

    df = source.rename(columns=COLUMN_MAP).copy()

    # -- identities ---------------------------------------------------------
    df["invoice"] = to_id_string(df["invoice"])
    df["stock_code"] = to_id_string(df["stock_code"])
    df["country"] = df["country"].astype("string").str.strip().replace("", pd.NA)

    # -- numerics -----------------------------------------------------------
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["customer_id"] = (
        pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    )

    # -- free text ----------------------------------------------------------
    desc = df["description"].astype("string").str.strip()
    df["description"] = desc.replace("", pd.NA)

    # -- time ---------------------------------------------------------------
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df["date"] = df["invoice_date"].dt.normalize()

    # -- exact-duplicate removal, split by cause ---------------------------
    # Business events are never deleted (see module docstring) -- duplicates
    # are, because they are a defect in the published file rather than a real
    # second invoice line. The two causes are separated so the worksheet
    # overlap is visible as the source defect it is.
    key_cols = list(COLUMN_MAP.values())
    dup_mask = df.duplicated(subset=key_cols, keep="first")

    # Capture the worksheet date ranges BEFORE dedup. Removing the duplicates
    # erases the very overlap we are trying to measure: once the repeated rows
    # are gone the two sheets look perfectly contiguous, and the defect
    # disappears from the profile.
    sheet_bounds = {name: (grp["date"].min(), grp["date"].max())
                    for name, grp in df.groupby("source_sheet")}

    if dup_mask.any():
        # Does a duplicate group draw its rows from more than one worksheet?
        # If yes, the vendor published the same transaction twice across the
        # two year-files; if no, the repetition happened inside one file.
        spanning = (
            df.groupby(key_cols, dropna=False, observed=True)["source_sheet"]
            .transform("nunique")
            .gt(1)
        )
        duplicates_cross_sheet = int((dup_mask & spanning).sum())
        duplicates_within_sheet = int((dup_mask & ~spanning).sum())
    else:
        duplicates_cross_sheet = duplicates_within_sheet = 0
    duplicates = int(dup_mask.sum())
    df = df.loc[~dup_mask]

    # Quantify the worksheet overlap from the pre-dedup ranges captured above.
    overlap_start = overlap_end = None
    if len(sheet_bounds) >= 2:
        ranges = list(sheet_bounds.values())
        lo = max(r[0] for r in ranges)
        hi = min(r[1] for r in ranges)
        if lo is not None and hi is not None and lo <= hi:
            overlap_start, overlap_end = lo, hi
    overlap_days = (
        int((overlap_end - overlap_start).days + 1)
        if overlap_start is not None else 0
    )

    # -- derived classification flags --------------------------------------
    invoice = df["invoice"].fillna("")
    df["is_cancelled"] = invoice.str.upper().str.startswith("C")
    df["is_return"] = df["quantity"].lt(0)
    df["is_nonpositive_price"] = df["unit_price"].le(0)
    df["is_valid_sale"] = (
        ~df["is_cancelled"]
        & df["quantity"].gt(0)
        & df["unit_price"].gt(0)
        & df["invoice_date"].notna()
    )
    df["line_revenue"] = (df["quantity"].astype("float64") * df["unit_price"]).round(4)

    # -- warehouse key ------------------------------------------------------
    df.insert(0, "transaction_id", pd.RangeIndex(1, len(df) + 1).astype("int64"))
    df = df.sort_values(["invoice_date", "transaction_id"], kind="stable")
    df = df.reset_index(drop=True)
    df["transaction_id"] = pd.RangeIndex(1, len(df) + 1).astype("int64")

    stats: dict[str, Any] = {
        "rows_in": raw_rows,
        "rows_out": int(len(df)),
        "dropped_exact_duplicates": duplicates,
        "duplicate_rows_cross_sheet": duplicates_cross_sheet,
        "duplicate_rows_within_sheet": duplicates_within_sheet,
        "worksheet_overlap_start": overlap_start.date().isoformat() if overlap_start is not None else None,
        "worksheet_overlap_end": overlap_end.date().isoformat() if overlap_end is not None else None,
        "worksheet_overlap_days": overlap_days,
        "source_rows_by_sheet": {k: int(v) for k, v in source["source_sheet"].value_counts().items()},
        "null_customer_id": int(df["customer_id"].isna().sum()),
        "null_customer_id_rate": round(float(df["customer_id"].isna().mean()), 6),
        "null_description": int(df["description"].isna().sum()),
        "null_invoice_date": int(df["invoice_date"].isna().sum()),
        "null_country": int(df["country"].isna().sum()),
        "null_stock_code": int(df["stock_code"].isna().sum()),
        "cancelled_rows": int(df["is_cancelled"].sum()),
        "negative_quantity_rows": int(df["is_return"].sum()),
        "nonpositive_price_rows": int(df["is_nonpositive_price"].sum()),
        "valid_sale_rows": int(df["is_valid_sale"].sum()),
        "distinct_invoices": int(df["invoice"].nunique(dropna=True)),
        "distinct_stock_codes": int(df["stock_code"].nunique(dropna=True)),
        "distinct_customers": int(df["customer_id"].nunique(dropna=True)),
        "distinct_countries": int(df["country"].nunique(dropna=True)),
        "date_min": df["date"].min().date().isoformat() if len(df) else None,
        "date_max": df["date"].max().date().isoformat() if len(df) else None,
        "by_sheet": {k: int(v) for k, v in df["source_sheet"].value_counts().items()},
    }

    keep = [
        "transaction_id", "invoice", "stock_code", "description", "quantity",
        "invoice_date", "date", "unit_price", "line_revenue", "customer_id",
        "country", "source_sheet", "is_cancelled", "is_return",
        "is_nonpositive_price", "is_valid_sale",
    ]
    return df[keep], stats


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def stage(cfg: AppConfig, force: bool = False) -> dict[str, Any]:
    """Read the workbook, clean it, write ``stg_transactions.parquet``."""
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    source = read_source(cfg, force=force)
    df, stats = clean(source)

    out = cfg.staging_dir / STAGING_NAME
    df.to_parquet(out, index=False)

    stats.update({
        "table": STAGING_NAME,
        "source_rows": int(len(source)),
        "source_columns": list(source.columns),
        "output_columns": list(df.columns),
        "elapsed": round(time.perf_counter() - t0, 3),
        "bytes": out.stat().st_size,
    })
    (cfg.staging_dir / STATS_NAME).write_text(
        json.dumps(stats, indent=2, default=str), encoding="utf-8"
    )
    return stats


def load(cfg: AppConfig) -> pd.DataFrame:
    """Convenience reader for tests and downstream stages."""
    return pd.read_parquet(cfg.staging_dir / STAGING_NAME)
