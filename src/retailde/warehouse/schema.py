"""Star-schema DDL for the DuckDB warehouse.

Shape is classic Kimball: one fact table, conformed dimensions, and
date-grained context tables hung off ``dim_date``'s key.

    dim_date ──┬── fact_transaction ──┬── dim_customer
               ├── dim_weather        ├── dim_product
               ├── dim_fx             └── dim_country
               └── dim_bank_rate (as-of join, not equality)

Notes on two deliberate choices:

**Surrogate keys are dense integers, not hashes.** ``customer_key`` 1..N reads
far better in a query plan and a dashboard than a 20-digit hash. Stability
under incremental load is preserved by the ``map_*_key`` tables: existing
members keep their key forever, new members take ``max + 1``. Rebuilding
therefore never renumbers a dimension underneath a fact that references it.

**The unknown member is real.** 235,151 transaction rows (22.8%) have no
``Customer ID``. Rather than dropping them -- which would delete real revenue
along with the missing identifier -- ``dim_customer`` carries key ``0`` with
``is_unknown = true`` so those rows join to something and stay visible instead
of silently vanishing through an inner join.
"""

from __future__ import annotations

# Reference / mapping tables -----------------------------------------------
# Append-only key assignment. Never rebuilt, because rebuilding would
# renumber dimensions and orphan every foreign key already written.
MAP_TABLES = """
CREATE TABLE IF NOT EXISTS map_customer_key (
    customer_id  BIGINT PRIMARY KEY,
    customer_key BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS map_product_key (
    stock_code  VARCHAR PRIMARY KEY,
    product_key BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS map_country_key (
    country     VARCHAR PRIMARY KEY,
    country_key BIGINT NOT NULL
);
"""

# Conformed dimensions ------------------------------------------------------
DIMENSIONS = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,   -- yyyymmdd, the Kimball date key
    date          DATE NOT NULL,
    year          SMALLINT NOT NULL,
    quarter       TINYINT NOT NULL,
    month         TINYINT NOT NULL,
    month_name    VARCHAR NOT NULL,
    day_of_month  TINYINT NOT NULL,
    day_of_week   TINYINT NOT NULL,      -- 1 = Monday .. 7 = Sunday
    day_name      VARCHAR NOT NULL,
    week_of_year  SMALLINT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_month_start BOOLEAN NOT NULL,
    is_month_end  BOOLEAN NOT NULL,
    season        VARCHAR NOT NULL,      -- Northern-hemisphere meteorological
    year_month    VARCHAR NOT NULL        -- '2010-12'
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    BIGINT PRIMARY KEY,  -- 0 = unknown member
    customer_id     BIGINT,              -- NULL for the unknown member
    country         VARCHAR,
    is_unknown      BOOLEAN NOT NULL,
    first_order_date DATE,
    last_order_date  DATE,
    distinct_orders  BIGINT,
    lifetime_value   DOUBLE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key  BIGINT PRIMARY KEY,
    stock_code   VARCHAR NOT NULL,
    description  VARCHAR,
    first_seen   DATE,
    last_seen    DATE,
    net_units    BIGINT,      -- sum(quantity): returns subtract
    revenue      DOUBLE,
    distinct_invoices BIGINT
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_key BIGINT PRIMARY KEY,
    country     VARCHAR NOT NULL,
    region      VARCHAR NOT NULL
);

-- Date-grained context, each keyed back to dim_date.
CREATE TABLE IF NOT EXISTS dim_weather (
    date_key  INTEGER PRIMARY KEY,
    tmax_c    DOUBLE,
    tmin_c    DOUBLE,
    precip_mm DOUBLE,
    is_wet    BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_fx (
    date_key   INTEGER PRIMARY KEY,
    gbp_to_usd DOUBLE,
    gbp_to_eur DOUBLE
);

-- Slowly-changing (SCD2) event log: "rate R in force from date D".
-- effective_to is NULL while the rate is current.
CREATE TABLE IF NOT EXISTS dim_bank_rate (
    bank_rate_key  INTEGER PRIMARY KEY,
    rate_pct       DOUBLE NOT NULL,
    effective_from DATE NOT NULL,
    effective_to   DATE
);
"""

# Fact ---------------------------------------------------------------------
FACT = """
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_id BIGINT PRIMARY KEY,
    invoice        VARCHAR NOT NULL,
    date_key       INTEGER NOT NULL,
    customer_key   BIGINT  NOT NULL,
    product_key    BIGINT  NOT NULL,
    country_key    BIGINT  NOT NULL,
    quantity       INTEGER NOT NULL,
    unit_price     DOUBLE  NOT NULL,
    line_revenue   DOUBLE  NOT NULL,
    is_cancelled   BOOLEAN NOT NULL,
    is_return      BOOLEAN NOT NULL,
    is_valid_sale  BOOLEAN NOT NULL
);
"""

# Pipeline metadata --------------------------------------------------------
# Not a warehouse table in the modelling sense -- it is the orchestration
# state that makes an incremental load possible and auditable.
METADATA = """
CREATE TABLE IF NOT EXISTS etl_run (
    run_id        BIGINT PRIMARY KEY,
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,
    mode          VARCHAR NOT NULL,
    status        VARCHAR NOT NULL,
    rows_loaded   BIGINT NOT NULL DEFAULT 0,
    detail        VARCHAR
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    source_id   VARCHAR PRIMARY KEY,
    loaded_upto BIGINT,
    loaded_at   TIMESTAMP NOT NULL,
    rows_total  BIGINT
);
"""

# Convenience view: the whole star joined, ready for reporting.
VIEWS = """
CREATE OR REPLACE VIEW v_transactions AS
SELECT
    f.transaction_id,
    f.invoice,
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    d.season,
    d.year_month,
    c.customer_id,
    c.is_unknown        AS is_unknown_customer,
    cu.country,
    cu.region,
    p.stock_code,
    p.description,
    f.quantity,
    f.unit_price,
    f.line_revenue,
    f.is_cancelled,
    f.is_return,
    f.is_valid_sale,
    w.tmax_c,
    w.precip_mm,
    w.is_wet,
    x.gbp_to_usd,
    x.gbp_to_eur,
    bk.rate_pct         AS bank_rate_pct
FROM fact_transaction f
JOIN dim_date     d  ON d.date_key  = f.date_key
JOIN dim_customer c  ON c.customer_key = f.customer_key
JOIN dim_product  p  ON p.product_key  = f.product_key
JOIN dim_country  cu ON cu.country_key = f.country_key
LEFT JOIN dim_weather w ON w.date_key = f.date_key
LEFT JOIN dim_fx      x ON x.date_key = f.date_key
LEFT JOIN (
    -- as-of join: the rate in force on each transaction's date. Bank Rate
    -- changes only a handful of times a year, so an equality join would
    -- match almost nothing -- each calendar day has to inherit the most
    -- recent decision at or before it.
    SELECT cal.date_key, br.rate_pct
    FROM dim_date cal
    JOIN dim_bank_rate br
      ON br.effective_from <= cal.date
     AND (br.effective_to IS NULL OR br.effective_to >= cal.date)
) bk ON bk.date_key = f.date_key;
"""

SCHEMA_STATEMENTS: list[str] = [MAP_TABLES, DIMENSIONS, FACT, METADATA, VIEWS]

__all__ = ["MAP_TABLES", "DIMENSIONS", "FACT", "METADATA", "VIEWS",
           "SCHEMA_STATEMENTS"]
