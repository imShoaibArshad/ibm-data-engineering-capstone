"""Build the business-facing marts on top of the star schema.

A mart answers a question someone actually asked. Each one here is defined
over ``v_transactions`` filtered to ``is_valid_sale``, so the definition of
"sale" lives in exactly one place (the fact table's flags) instead of being
re-derived -- differently, each time -- in five SQL files.

Marts are rebuilt wholesale from the fact table rather than maintained
incrementally. That is safe even in incremental load mode because they are
pure functions of ``fact_transaction``: the fact table's current state already
reflects whatever was appended, so recomputing is always correct, and each
mart is a couple of milliseconds.
"""

from __future__ import annotations

import time
from typing import Any

import duckdb

from ..config import AppConfig

MARTS: dict[str, str] = {
    # Question: how did trading evolve, by region?
    "mart_monthly_revenue": """
        SELECT
            v.year_month,
            v.year,
            v.month,
            v.month_name,
            v.region,
            COUNT(*)                                   AS order_lines,
            COUNT(DISTINCT v.invoice)                  AS orders,
            COUNT(DISTINCT v.customer_id)              AS identified_customers,
            SUM(v.quantity)                            AS units,
            ROUND(SUM(v.line_revenue), 2)              AS revenue
        FROM v_transactions v
        WHERE v.is_valid_sale
        GROUP BY v.year_month, v.year, v.month, v.month_name, v.region
        ORDER BY v.year_month, revenue DESC
    """,

    # Question: is trading seasonal, and does weather move it?
    "mart_daily_kpis": """
        SELECT
            v.date,
            v.year,
            v.month,
            v.day_name,
            v.is_weekend,
            v.season,
            COUNT(DISTINCT v.invoice)                            AS orders,
            COUNT(DISTINCT v.customer_id)                        AS identified_customers,
            SUM(CASE WHEN v.is_valid_sale THEN v.quantity ELSE 0 END) AS units,
            ROUND(SUM(CASE WHEN v.is_valid_sale
                           THEN v.line_revenue ELSE 0 END), 2)   AS revenue,
            -- cost side: returns and cancellations subtract from gross
            ROUND(SUM(CASE WHEN NOT v.is_valid_sale
                           THEN v.line_revenue ELSE 0 END), 2)   AS excluded_revenue,
            MAX(v.tmax_c)                                        AS tmax_c,
            MAX(v.precip_mm)                                     AS precip_mm,
            MAX(v.is_wet)                                        AS is_wet,
            MAX(v.gbp_to_usd)                                    AS gbp_to_usd,
            MAX(v.bank_rate_pct)                                 AS bank_rate_pct
        FROM v_transactions v
        GROUP BY v.date, v.year, v.month, v.day_name, v.is_weekend, v.season
        ORDER BY v.date
    """,

    # Question: which products actually earn?
    "mart_product_performance": """
        WITH base AS (
            SELECT
                v.stock_code,
                v.description,
                COUNT(DISTINCT v.invoice)      AS orders,
                SUM(v.quantity)                AS units,
                ROUND(SUM(v.line_revenue), 2)  AS revenue,
                COUNT(DISTINCT v.customer_id)  AS buying_customers,
                ROUND(AVG(v.unit_price), 4)    AS avg_unit_price
            FROM v_transactions v
            WHERE v.is_valid_sale
            GROUP BY v.stock_code, v.description
        )
        SELECT
            *,
            ROUND(revenue / NULLIF(orders, 0), 2)      AS revenue_per_order,
            ROW_NUMBER() OVER (ORDER BY revenue DESC)  AS revenue_rank,
            -- share of total revenue, so "top 10" can be read as a percentage
            ROUND(100.0 * revenue
                  / NULLIF(SUM(revenue) OVER (), 0), 3) AS revenue_share_pct
        FROM base
        ORDER BY revenue DESC
    """,

    # Question: who are the customers, and who is drifting away?
    "mart_customer_segments": """
        WITH base AS (
            SELECT
                v.customer_id,
                v.is_unknown_customer,
                MIN(v.date)                       AS first_order,
                MAX(v.date)                       AS last_order,
                COUNT(DISTINCT v.invoice)         AS frequency,
                ROUND(SUM(v.line_revenue), 2)     AS monetary,
                SUM(v.quantity)                   AS units
            FROM v_transactions v
            WHERE v.is_valid_sale
            GROUP BY v.customer_id, v.is_unknown_customer
        ),
        scored AS (
            SELECT
                *,
                -- NTILE(5) ASC puts the smallest value in bucket 1, so for
                -- recency (newer = better) we order ascending to land the
                -- most recent buyer in bucket 5.
                NTILE(5) OVER (ORDER BY last_order ASC)   AS recency_score,
                NTILE(5) OVER (ORDER BY frequency ASC)    AS frequency_score,
                NTILE(5) OVER (ORDER BY monetary ASC)     AS monetary_score
            FROM base
        )
        SELECT
            *,
            recency_score + frequency_score + monetary_score AS rfm_score,
            CASE
                WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'Champions'
                WHEN recency_score >= 4 AND frequency_score >= 2 THEN 'Loyal'
                WHEN recency_score >= 3                          THEN 'Potential'
                WHEN recency_score = 1 AND frequency_score >= 3 THEN 'At Risk'
                WHEN recency_score = 1 AND frequency_score <= 2 THEN 'Lost'
                ELSE 'Needs Attention'
            END AS segment
        FROM scored
        ORDER BY rfm_score DESC, monetary DESC
    """,

    # Question: which markets matter, outside the UK?
    "mart_region_summary": """
        SELECT
            v.region,
            v.country,
            COUNT(*)                              AS order_lines,
            COUNT(DISTINCT v.invoice)             AS orders,
            COUNT(DISTINCT v.customer_id)         AS identified_customers,
            SUM(v.quantity)                       AS units,
            ROUND(SUM(v.line_revenue), 2)         AS revenue,
            ROUND(AVG(v.line_revenue), 2)         AS avg_line_value
        FROM v_transactions v
        WHERE v.is_valid_sale
        GROUP BY v.region, v.country
        ORDER BY revenue DESC
    """,
}


def build(cfg: AppConfig) -> dict[str, Any]:
    """(Re)create every mart and export each to Parquet."""
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    con = duckdb.connect(str(cfg.warehouse_path))
    written: dict[str, Any] = {}
    try:
        for name, sql in MARTS.items():
            m0 = time.perf_counter()
            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            rows = int(con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])

            out = cfg.marts_dir / f"{name}.parquet"
            con.execute(
                f"COPY (SELECT * FROM {name}) TO '{out.as_posix()}' (FORMAT PARQUET)"
            )
            written[name] = {
                "rows": rows,
                "bytes": out.stat().st_size,
                "elapsed": round(time.perf_counter() - m0, 4),
            }
    finally:
        con.close()

    return {
        "marts": written,
        "count": len(written),
        "elapsed": round(time.perf_counter() - t0, 3),
    }


def rows(cfg: AppConfig, mart: str, limit: int = 10) -> list[tuple]:
    """Read a mart -- convenience for tests and the CLI."""
    if mart not in MARTS:
        raise KeyError(f"unknown mart {mart!r}; have {sorted(MARTS)}")
    con = duckdb.connect(str(cfg.warehouse_path), read_only=True)
    try:
        return con.execute(f"SELECT * FROM {mart} LIMIT {int(limit)}").fetchall()
    finally:
        con.close()
