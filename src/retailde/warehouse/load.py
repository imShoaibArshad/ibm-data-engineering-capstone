"""Load staging Parquet into the DuckDB star schema.

Two modes, one code path
------------------------
``full``         drops every table, re-creates the schema, and loads all
                 1,033,036 fact rows plus every dimension. Reproducible: the
                 same staging input always yields byte-identical keys.

``incremental``  keeps the warehouse, assigns keys for genuinely new members,
                 rebuilds the (small) dimensions, and appends only fact rows
                 above the persisted watermark.

Dimensions are rebuilt wholesale even in incremental mode. That is a real
trade-off, made deliberately: a dimension here is at most ~6,000 rows, so
rebuilding it costs milliseconds, while the fact table is 1M rows where the
savings are real. Key stability -- the thing that would actually break if we
renumbered -- is protected by the ``map_*_key`` tables, which are append-only
and never rebuilt.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..config import AppConfig
from . import schema

STAGING_TABLES = ("stg_transactions", "stg_weather", "stg_fx", "stg_bank_rate")
REGION_CSV = Path("config") / "country_regions.csv"


class WarehouseError(RuntimeError):
    """Raised when the warehouse cannot be loaded."""


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _sql_path(path: Path) -> str:
    """Path safe to inline in a SQL string literal (forward slashes only)."""
    return path.as_posix().replace("'", "''")


def _register_staging(con: duckdb.DuckDBPyConnection, cfg: AppConfig) -> None:
    """Expose staging Parquet as views.

    DuckDB reads Parquet directly and out-of-core -- the 13 MB fact feed is
    never pulled into pandas memory, which is the whole point of using a
    columnar engine here rather than a dataframe round-trip.
    """
    for name in STAGING_TABLES:
        path = cfg.staging_dir / f"{name}.parquet"
        if not path.exists():
            raise WarehouseError(
                f"missing staging table {path} -- run `retailde stage` first."
            )
        con.execute(
            f'CREATE OR REPLACE VIEW "{name}" AS '
            f"SELECT * FROM read_parquet('{_sql_path(path)}')"
        )


def _regions(cfg: AppConfig) -> pd.DataFrame:
    path = cfg.root / REGION_CSV
    if not path.exists():
        raise WarehouseError(f"reference file missing: {path}")
    df = pd.read_csv(path)
    required = {"country", "region"}
    if not required.issubset(df.columns):
        raise WarehouseError(f"{path} must have columns {sorted(required)}")
    return df[["country", "region"]]


def _drop_all(con: duckdb.DuckDBPyConnection) -> None:
    for table in ("fact_transaction", "dim_weather", "dim_fx", "dim_bank_rate",
                  "dim_date", "dim_customer", "dim_product", "dim_country",
                  "map_customer_key", "map_product_key", "map_country_key",
                  "etl_watermark", "etl_run"):
        con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute("DROP VIEW IF EXISTS v_transactions")


def _create_schema(con: duckdb.DuckDBPyConnection) -> None:
    for statement in schema.SCHEMA_STATEMENTS:
        con.execute(statement)


# ---------------------------------------------------------------------------
# surrogate-key assignment (append-only, the load-bearing piece)
# ---------------------------------------------------------------------------

KEY_MAP_SPECS = [
    # (map table, natural-key column, surrogate-key column, staging expression)
    ("map_customer_key", "customer_id", "customer_key",
     "COALESCE(customer_id, -1)"),
    ("map_product_key", "stock_code", "product_key", "stock_code"),
    ("map_country_key", "country", "country_key",
     "COALESCE(country, 'Unspecified')"),
]


def _assign_keys(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Give every unseen natural key its next surrogate id.

    Existing members are never renumbered -- that is the invariant that makes
    an incremental fact load safe. New members continue from
    ``max(key) + 1`` rather than being re-ranked, so a dimension can only
    ever grow.

    Read-then-write across two statements rather than one
    ``INSERT ... SELECT`` that reads the table it is inserting into: the
    self-reference is fragile in every SQL engine, and this way the base and
    the new-member set are both pinned down before anything is mutated.
    """
    assigned: dict[str, int] = {}
    for table, key_col, val_col, expr in KEY_MAP_SPECS:
        new_members = con.execute(
            f"""
            SELECT DISTINCT {expr} AS nk
            FROM stg_transactions
            WHERE {expr} NOT IN (SELECT {key_col} FROM {table})
            ORDER BY nk
            """
        ).fetchall()

        if new_members:
            base = int(
                con.execute(f"SELECT COALESCE(MAX({val_col}), 0) FROM {table}").fetchone()[0]
            )
            con.executemany(
                f"INSERT INTO {table} ({key_col}, {val_col}) VALUES (?, ?)",
                [(nk, base + i + 1) for i, (nk,) in enumerate(new_members)],
            )

        assigned[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return assigned


# ---------------------------------------------------------------------------
# dimension builders
# ---------------------------------------------------------------------------

def _build_date(con: duckdb.DuckDBPyConnection) -> None:
    # Rebuilt each run: the calendar is a pure function of the fact range, so
    # recomputing it is both cheap and the only way to widen it correctly when
    # a later source file extends the window.
    con.execute("DELETE FROM dim_date")
    con.execute(
        """
        INSERT INTO dim_date (
            date_key, date, year, quarter, month, month_name, day_of_month,
            day_of_week, day_name, week_of_year, is_weekend, is_month_start,
            is_month_end, season, year_month
        )
        WITH bounds AS (
            SELECT MIN(date) AS d0, MAX(date) AS d1 FROM stg_transactions
        ),
        days AS (
            -- generate_series is a scalar array function here, so it has to
            -- be unnested; without UNNEST every strftime below would be
            -- handed a TIMESTAMP[] and fail to bind.
            SELECT CAST(UNNEST(GENERATE_SERIES(d0, d1, INTERVAL '1 day')) AS DATE) AS d
            FROM bounds
        )
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            d,
            CAST(strftime(d, '%Y') AS SMALLINT),
            CAST(CEIL(EXTRACT(month FROM d) / 3.0) AS TINYINT),
            CAST(EXTRACT(month FROM d) AS TINYINT),
            strftime(d, '%B'),
            CAST(EXTRACT(day FROM d) AS TINYINT),
            CAST(isodow(d) AS TINYINT),
            strftime(d, '%A'),
            CAST(strftime(d, '%V') AS SMALLINT),
            isodow(d) >= 6,
            d = CAST(date_trunc('month', d) AS DATE),
            d = CAST(date_trunc('month', d) + INTERVAL 1 MONTH - INTERVAL 1 DAY AS DATE),
            CASE
                WHEN EXTRACT(month FROM d) IN (12, 1, 2) THEN 'Winter'
                WHEN EXTRACT(month FROM d) IN (3, 4, 5)  THEN 'Spring'
                WHEN EXTRACT(month FROM d) IN (6, 7, 8)  THEN 'Summer'
                ELSE 'Autumn'
            END,
            strftime(d, '%Y-%m')
        FROM days
        """
    )


def _build_country(con: duckdb.DuckDBPyConnection, cfg: AppConfig) -> None:
    # Rebuilt, but keys come from the append-only map, so the rebuild cannot
    # renumber anything a fact row already points at.
    con.execute("DELETE FROM dim_country")
    con.register("_regions", _regions(cfg))
    con.execute(
        """
        INSERT INTO dim_country (country_key, country, region)
        SELECT k.country_key, k.country, COALESCE(r.region, 'Unmapped')
        FROM map_country_key k
        LEFT JOIN _regions r ON r.country = k.country
        """
    )


def _build_customer(con: duckdb.DuckDBPyConnection) -> None:
    # Rebuilt each run (small) but keys come from the append-only map, so
    # renumbering is impossible.
    #
    # The unknown member is deliberately *conditional*: it exists only when
    # staging actually contains rows with a missing Customer ID, and it takes
    # whatever key the map assigned to natural key -1. Hardcoding key 0 here
    # would desynchronise from the map the moment key assignment changes.
    con.execute("DELETE FROM dim_customer")
    con.execute(
        """
        INSERT INTO dim_customer (
            customer_key, customer_id, country, is_unknown,
            first_order_date, last_order_date, distinct_orders, lifetime_value
        )
        WITH unknown AS (
            SELECT
                m.customer_key,
                CAST(NULL AS BIGINT) AS customer_id,
                MIN(t.country) AS country,
                MIN(t.date) AS first_order_date,
                MAX(t.date) AS last_order_date,
                COUNT(DISTINCT t.invoice) AS distinct_orders,
                SUM(CASE WHEN t.is_valid_sale THEN t.line_revenue ELSE 0 END) AS lifetime_value
            FROM stg_transactions t
            JOIN map_customer_key m ON m.customer_id = -1
            WHERE t.customer_id IS NULL
            GROUP BY m.customer_key
            HAVING COUNT(*) > 0
        ),
        known AS (
            SELECT
                m.customer_key,
                t.customer_id,
                MIN(t.country) AS country,
                MIN(t.date) AS first_order_date,
                MAX(t.date) AS last_order_date,
                COUNT(DISTINCT t.invoice) AS distinct_orders,
                SUM(CASE WHEN t.is_valid_sale THEN t.line_revenue ELSE 0 END) AS lifetime_value
            FROM stg_transactions t
            JOIN map_customer_key m ON m.customer_id = COALESCE(t.customer_id, -1)
            WHERE t.customer_id IS NOT NULL
            GROUP BY m.customer_key, t.customer_id
        )
        SELECT customer_key, customer_id, country,
               CAST(TRUE AS BOOLEAN) AS is_unknown,
               first_order_date, last_order_date, distinct_orders, lifetime_value
        FROM unknown
        UNION ALL BY NAME
        SELECT customer_key, customer_id, country,
               CAST(FALSE AS BOOLEAN) AS is_unknown,
               first_order_date, last_order_date, distinct_orders, lifetime_value
        FROM known
        """
    )


def _build_product(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DELETE FROM dim_product")
    con.execute(
        """
        INSERT INTO dim_product (
            product_key, stock_code, description, first_seen, last_seen,
            net_units, revenue, distinct_invoices
        )
        WITH agg AS (
            SELECT
                t.stock_code,
                MIN(t.date) AS first_seen,
                MAX(t.date) AS last_seen,
                SUM(t.quantity) AS net_units,
                COALESCE(SUM(CASE WHEN t.is_valid_sale THEN t.line_revenue ELSE 0 END), 0) AS revenue,
                COUNT(DISTINCT t.invoice) AS distinct_invoices
            FROM stg_transactions t
            GROUP BY t.stock_code
        ),
        -- A stock code's description drifts over two years; pick the most
        -- frequently published non-null value rather than an arbitrary one.
        picked AS (
            SELECT stock_code, description,
                   ROW_NUMBER() OVER (
                       PARTITION BY stock_code
                       ORDER BY c DESC, description
                   ) AS rn
            FROM (
                SELECT stock_code, description, COUNT(*) AS c
                FROM stg_transactions
                WHERE description IS NOT NULL
                GROUP BY stock_code, description
            ) d
        )
        SELECT m.product_key, a.stock_code, p.description,
               a.first_seen, a.last_seen, a.net_units, a.revenue, a.distinct_invoices
        FROM agg a
        JOIN map_product_key m ON m.stock_code = a.stock_code
        LEFT JOIN picked p ON p.stock_code = a.stock_code AND p.rn = 1
        """
    )


def _build_context(con: duckdb.DuckDBPyConnection) -> None:
    """Date-grained context tables: weather, FX, and the SCD2 bank rate."""
    con.execute("DELETE FROM dim_weather")
    con.execute(
        """
        INSERT INTO dim_weather (date_key, tmax_c, tmin_c, precip_mm, is_wet)
        SELECT d.date_key, w.tmax_c, w.tmin_c, w.precip_mm, w.is_wet
        FROM stg_weather w
        JOIN dim_date d ON d.date = w.date
        """
    )

    con.execute("DELETE FROM dim_fx")
    con.execute(
        """
        INSERT INTO dim_fx (date_key, gbp_to_usd, gbp_to_eur)
        SELECT d.date_key, x.gbp_to_usd, x.gbp_to_eur
        FROM stg_fx x
        JOIN dim_date d ON d.date = x.date
        """
    )

    con.execute("DELETE FROM dim_bank_rate")
    con.execute(
        """
        INSERT INTO dim_bank_rate (bank_rate_key, rate_pct, effective_from, effective_to)
        SELECT
            ROW_NUMBER() OVER (ORDER BY date),
            rate_pct,
            date,
            -- "in force until the day before the next decision"
            CAST(LEAD(date) OVER (ORDER BY date) - INTERVAL 1 DAY AS DATE)
        FROM stg_bank_rate
        """
    )


def _build_facts(con: duckdb.DuckDBPyConnection, *, incremental: bool) -> int:
    before = int(con.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0])

    where = ""
    if incremental:
        where = """
            WHERE t.transaction_id > COALESCE(
                (SELECT loaded_upto FROM etl_watermark WHERE source_id = 'uci_online_retail'),
                -1
            )
        """

    con.execute(
        f"""
        INSERT INTO fact_transaction (
            transaction_id, invoice, date_key, customer_key, product_key,
            country_key, quantity, unit_price, line_revenue,
            is_cancelled, is_return, is_valid_sale
        )
        SELECT
            t.transaction_id,
            t.invoice,
            d.date_key,
            mk.customer_key,
            pk.product_key,
            ck.country_key,
            CAST(t.quantity AS INTEGER),
            CAST(t.unit_price AS DOUBLE),
            CAST(t.line_revenue AS DOUBLE),
            t.is_cancelled, t.is_return, t.is_valid_sale
        FROM stg_transactions t
        JOIN dim_date       d  ON d.date  = t.date
        JOIN map_customer_key mk ON mk.customer_id = COALESCE(t.customer_id, -1)
        JOIN map_product_key  pk ON pk.stock_code  = t.stock_code
        JOIN map_country_key  ck ON ck.country     = COALESCE(t.country, 'Unspecified')
        {where}
        ON CONFLICT (transaction_id) DO NOTHING
        """
    )

    after = int(con.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0])
    return after - before


def _write_watermark(con: duckdb.DuckDBPyConnection) -> None:
    # LOCALTIMESTAMP, not CURRENT_TIMESTAMP: the column is TIMESTAMP without
    # a zone, and passing a TIMESTAMPTZ would depend on implicit casting rules.
    con.execute(
        """
        INSERT INTO etl_watermark (source_id, loaded_upto, loaded_at, rows_total)
        SELECT 'uci_online_retail', MAX(transaction_id), LOCALTIMESTAMP, COUNT(*)
        FROM fact_transaction
        ON CONFLICT (source_id) DO UPDATE SET
            loaded_upto = excluded.loaded_upto,
            loaded_at   = excluded.loaded_at,
            rows_total  = excluded.rows_total
        """
    )


def _log_run(con: duckdb.DuckDBPyConnection, mode: str, rows: int,
             started: datetime, status: str, detail: str = "") -> None:
    con.execute(
        """
        INSERT INTO etl_run (run_id, started_at, finished_at, mode, status, rows_loaded, detail)
        SELECT COALESCE(MAX(run_id), 0) + 1, ?, ?, ?, ?, ?, ?
        FROM etl_run
        """,
        [started, datetime.now(timezone.utc).replace(tzinfo=None), mode, status, rows, detail],
    )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def load(cfg: AppConfig, *, mode: str = "full") -> dict[str, Any]:
    """Populate the warehouse. ``mode`` is ``full`` or ``incremental``."""
    if mode not in ("full", "incremental"):
        raise ValueError(f"unknown mode {mode!r}; expected 'full' or 'incremental'")

    cfg.ensure_dirs()
    cfg.warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    # Naive UTC: etl_run.started_at is a TIMESTAMP column, and a tz-aware
    # datetime would depend on implicit conversion.
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    t0 = time.perf_counter()

    con = duckdb.connect(str(cfg.warehouse_path))
    try:
        _register_staging(con, cfg)

        if mode == "full":
            _drop_all(con)
        _create_schema(con)

        key_counts = _assign_keys(con)
        _build_date(con)
        _build_country(con, cfg)
        _build_customer(con)
        _build_product(con)
        _build_context(con)
        rows_loaded = _build_facts(con, incremental=(mode == "incremental"))
        _write_watermark(con)

        con.execute("CHECKPOINT")  # flush the WAL so a close is durable
        _log_run(con, mode, rows_loaded, started, "success")
    except Exception as exc:
        try:
            _log_run(con, mode, 0, started, "failed", f"{type(exc).__name__}: {exc}")
        except Exception:
            pass  # the warehouse may not have got far enough to log
        raise
    finally:
        con.close()

    elapsed = time.perf_counter() - t0
    return {
        "mode": mode,
        "rows_loaded": rows_loaded,
        "warehouse": str(cfg.warehouse_path.relative_to(cfg.root)),
        "key_counts": key_counts,
        "elapsed": round(elapsed, 3),
    }


def counts(cfg: AppConfig) -> dict[str, int]:
    """Row counts for every warehouse table -- used by tests and the README."""
    if not cfg.warehouse_path.exists():
        return {}
    con = duckdb.connect(str(cfg.warehouse_path), read_only=True)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()]
        return {t: int(con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0])
                for t in tables}
    finally:
        con.close()
