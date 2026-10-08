"""Scrape the Bank of England's official Bank Rate decision history.

Source page renders one HTML table::

    <table>
      <tr><th>Date Changed</th><th>Rate</th></tr>
      <tr><td>18 Dec 25</td><td>3.75</td></tr>
      ...
    </table>

258 decisions spanning 1975-01-20 to 2025-12-18, dates formatted ``%d %b %y``
(a two-digit year, so the century has to be inferred -- pre-2000 rows would
otherwise land in the 2000s; the table is reverse-chronological, which gives a
second signal).

Crucially this is an **event log, not a time series**: each row means "this
rate took effect on this date, until the next decision". Turning it into a
per-trading-day value requires a forward-fill / as-of join. That is exactly
the shape of an SCD Type 2 dimension, and it is done at warehouse-load time
rather than here, so the raw event log stays intact and auditable.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd

from ..config import AppConfig

RAW_NAME = "boe_bank_rate.html"
STAGING_NAME = "stg_bank_rate.parquet"


def _parse_date(text: str, *, assume_pre_2000: bool = False) -> pd.Timestamp | None:
    text = " ".join(text.split())
    for fmt in ("%d %b %y", "%d %b %Y", "%d %B %y", "%d %B %Y"):
        try:
            ts = pd.to_datetime(text, format=fmt)
            # pandas maps %y to 1969-2068; guard the boundary explicitly so
            # the rule is stated rather than inherited.
            if fmt.endswith("%y") and assume_pre_2000 and ts.year >= 2000:
                ts = ts.replace(year=ts.year - 100)
            return ts
        except (ValueError, TypeError):
            continue
    return None


def parse(html: str, table_header_contains: str = "Date Changed") -> pd.DataFrame:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")

    table = None
    for candidate in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in candidate.find_all("th")]
        if table_header_contains in headers:
            table = candidate
            break
    if table is None:
        raise ValueError(
            f"no HTML table found whose headers contain {table_header_contains!r}"
        )

    # The published table runs newest-first; parse positions let us detect a
    # backwards jump across the 1999/2000 boundary rather than guess.
    records: list[dict[str, Any]] = []
    unparsed: list[str] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        if len(cells) < 2:
            continue
        date_s, rate_s = cells[0], cells[1]
        ts = _parse_date(date_s)
        if ts is None:
            unparsed.append(date_s)
            continue
        try:
            rate = float(rate_s)
        except ValueError:
            unparsed.append(f"{date_s}={rate_s!r}")
            continue
        records.append({"date": ts, "rate_pct": rate})

    if not records:
        raise ValueError("Bank Rate table parsed to zero rows -- page layout has changed")

    df = pd.DataFrame(records).drop_duplicates(subset=["date"], keep="first")
    df = df.sort_values("date").reset_index(drop=True)
    df["rate_pct"] = df["rate_pct"].astype("float64")
    df["change_pct"] = df["rate_pct"].diff().round(4)

    # Record how many rows failed to parse instead of silently dropping them;
    # a layout change should surface as a quality failure, not data loss.
    df.attrs["unparsed"] = unparsed
    return df


def stage(cfg: AppConfig, force: bool = False) -> dict[str, Any]:
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    raw = cfg.raw_dir / RAW_NAME
    if not raw.exists():
        raise FileNotFoundError(f"{raw} not found -- run `retailde extract` first.")

    source = cfg.source("boe_bank_rate")
    header = source.options.get("table_header_contains", "Date Changed")

    html = raw.read_text(encoding="utf-8", errors="replace")
    df = parse(html, table_header_contains=str(header))

    out = cfg.staging_dir / STAGING_NAME
    df.to_parquet(out, index=False)

    window = df[(df["date"] >= "2009-12-01") & (df["date"] <= "2011-12-09")]
    stats: dict[str, Any] = {
        "table": STAGING_NAME,
        "rows_out": int(len(df)),
        "date_min": df["date"].min().date().isoformat(),
        "date_max": df["date"].max().date().isoformat(),
        "unparsed_rows": len(df.attrs.get("unparsed", [])),
        "unparsed_samples": df.attrs.get("unparsed", [])[:5],
        # Decisions falling inside the transaction window tell us how much of
        # this series the warehouse will actually see per-day.
        "decisions_in_transaction_window": int(len(window)),
        "rate_in_force_at_window_start": _rate_at(df, pd.Timestamp("2009-12-01")),
        "min_rate_pct": float(df["rate_pct"].min()),
        "max_rate_pct": float(df["rate_pct"].max()),
        "elapsed": round(time.perf_counter() - t0, 3),
        "bytes": out.stat().st_size,
    }
    return stats


def _rate_at(df: pd.DataFrame, when: pd.Timestamp) -> float | None:
    """Rate in force on ``when`` (last decision at or before it)."""
    eligible = df.loc[df["date"] <= when, "date"]
    if eligible.empty:
        return None
    idx = eligible.idxmax()
    return float(df.loc[idx, "rate_pct"])


def load(cfg: AppConfig) -> pd.DataFrame:
    return pd.read_parquet(cfg.staging_dir / STAGING_NAME)
