"""Warehouse tests: schema, key stability, referential integrity, incremental."""

from __future__ import annotations

import duckdb
import pytest

from retailde.warehouse import counts, load


@pytest.fixture()
def loaded(staged):
    load(staged, mode="full")
    return staged


def _con(cfg):
    return duckdb.connect(str(cfg.warehouse_path), read_only=True)


def _one(cfg, sql, params=None):
    con = _con(cfg)
    try:
        return con.execute(sql, params or []).fetchone()[0]
    finally:
        con.close()


def _row(cfg, sql, params=None):
    """For queries returning more than one column -- _one() truncates to [0]."""
    con = _con(cfg)
    try:
        return con.execute(sql, params or []).fetchone()
    finally:
        con.close()


def _key_fingerprint(cfg) -> tuple:
    """A digest of every dimension's key -> natural-key assignment.

    If a rebuild renumbers a dimension, fact rows written earlier point at the
    wrong member; this detects that directly rather than hoping a row count
    happens to notice.
    """
    con = _con(cfg)
    try:
        cust = con.execute(
            "SELECT COALESCE(SUM(customer_key * 1000003 + COALESCE(customer_id, -1)), 0) "
            "FROM dim_customer").fetchone()[0]
        prod = con.execute(
            "SELECT COALESCE(SUM(product_key * 1000003 + hash(stock_code)), 0) "
            "FROM dim_product").fetchone()[0]
        ctry = con.execute(
            "SELECT COALESCE(SUM(country_key * 1000003 + hash(country)), 0) "
            "FROM dim_country").fetchone()[0]
        return cust, prod, ctry
    finally:
        con.close()


class TestSchema:
    def test_all_tables_exist(self, loaded):
        got = counts(loaded)
        for table in ("fact_transaction", "dim_date", "dim_customer",
                      "dim_product", "dim_country", "dim_weather", "dim_fx",
                      "dim_bank_rate", "etl_run", "etl_watermark"):
            assert table in got, f"missing table {table}"

    def test_fact_rows_equal_staged_rows(self, loaded):
        assert _one(loaded, "SELECT COUNT(*) FROM fact_transaction") == 13
        assert _one(loaded, "SELECT COUNT(*) FROM stg_transactions") == 13

    def test_fact_ids_match_staging_exactly(self, loaded):
        gap = _one(loaded, """
            SELECT
              (SELECT COUNT(*) FROM stg_transactions)
            - (SELECT COUNT(*) FROM fact_transaction)
        """)
        assert gap == 0

    @pytest.mark.parametrize("dim", ["dim_date", "dim_customer", "dim_product", "dim_country"])
    def test_no_orphan_foreign_keys(self, loaded, dim):
        col = {"dim_date": "date_key", "dim_customer": "customer_key",
               "dim_product": "product_key", "dim_country": "country_key"}[dim]
        orphans = _one(loaded, f"""
            SELECT COUNT(*) FROM fact_transaction f
            LEFT JOIN {dim} d ON d.{col} = f.{col}
            WHERE d.{col} IS NULL
        """)
        assert orphans == 0, f"{orphans} fact rows point at a missing {dim}"

    def test_date_dimension_is_continuous(self, loaded):
        days, span = _row(loaded, """
            SELECT COUNT(*), CAST(MAX(date) - MIN(date) AS INTEGER) + 1
            FROM dim_date
        """)
        assert days == span, f"dim_date has gaps: {days} rows over a {span}-day span"

    def test_date_key_is_yyyymmdd(self, loaded):
        bad = _one(loaded, """
            SELECT COUNT(*) FROM dim_date
            WHERE date_key <> CAST(strftime(date, '%Y%m%d') AS INTEGER)
        """)
        assert bad == 0

    def test_context_tables_key_back_to_dim_date(self, loaded):
        for table in ("dim_weather", "dim_fx"):
            orphans = _one(loaded, f"""
                SELECT COUNT(*) FROM {table} t
                LEFT JOIN dim_date d ON d.date_key = t.date_key
                WHERE d.date_key IS NULL
            """)
            assert orphans == 0, f"{table} has rows outside dim_date"


class TestUnknownMember:
    def test_unknown_customer_exists_and_is_flagged(self, loaded):
        rows = _one(loaded, """
            SELECT COUNT(*) FROM dim_customer WHERE is_unknown
        """)
        assert rows == 1, "the 2 null-Customer-ID rows need a member to join to"

    def test_rows_without_customer_id_land_on_the_unknown_member(self, loaded):
        """NULL never equals NULL in SQL, so a naive
        ``dim_customer.customer_id = stg.customer_id`` matches *nothing* for
        unidentified customers and they would fall out of every join. The
        loader substitutes ``COALESCE(customer_id, -1)`` instead; this asserts
        that substitution actually took effect on every affected row."""
        misplaced = _one(loaded, """
            SELECT COUNT(*)
            FROM fact_transaction f
            JOIN stg_transactions s ON s.transaction_id = f.transaction_id
            JOIN dim_customer c     ON c.customer_key = f.customer_key
            WHERE s.customer_id IS NULL AND NOT c.is_unknown
        """)
        assert misplaced == 0, "a null-customer row joined to a real customer"

        affected = _one(loaded,
                        "SELECT COUNT(*) FROM stg_transactions WHERE customer_id IS NULL")
        assert affected == 2, "fixture has exactly 2 rows without a Customer ID"
        assert _one(loaded, "SELECT COUNT(*) FROM fact_transaction") == 13, \
            "those 2 rows must still be in the fact table"

    def test_unknown_member_keeps_its_revenue_visible(self, loaded):
        revenue = _one(loaded, """
            SELECT ROUND(COALESCE(SUM(f.line_revenue), 0), 2)
            FROM fact_transaction f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            WHERE c.is_unknown AND f.is_valid_sale
        """)
        assert revenue > 0, "deleting unidentified customers would hide revenue"


class TestBankRateScd2:
    def test_effective_ranges_are_contiguous(self, loaded):
        gaps = _one(loaded, """
            SELECT COUNT(*) FROM dim_bank_rate a
            JOIN dim_bank_rate b ON b.bank_rate_key = a.bank_rate_key + 1
            WHERE a.effective_to IS NULL
               OR CAST(a.effective_to AS DATE) <> CAST(b.effective_from AS DATE) - 1
        """)
        assert gaps == 0, "every rate must end the day before the next begins"

    def test_as_of_join_resolves_every_fact_row(self, loaded):
        nulls = _one(loaded, """
            SELECT COUNT(*) FROM v_transactions WHERE bank_rate_pct IS NULL
        """)
        assert nulls == 0

    def test_only_the_latest_rate_has_no_end_date(self, loaded):
        open_ranges = _one(loaded,
            "SELECT COUNT(*) FROM dim_bank_rate WHERE effective_to IS NULL")
        assert open_ranges == 1, "exactly one rate is 'currently in force'"


class TestIncremental:
    def test_second_load_adds_nothing_and_renumbers_nothing(self, staged):
        load(staged, mode="full")
        before = _key_fingerprint(staged)
        before_facts = _one(staged, "SELECT COUNT(*) FROM fact_transaction")

        stats = load(staged, mode="incremental")

        assert stats["rows_loaded"] == 0, "no new source rows -> nothing to add"
        assert _key_fingerprint(staged) == before, "keys must never renumber"
        assert _one(staged, "SELECT COUNT(*) FROM fact_transaction") == before_facts

    def test_watermark_advances(self, staged):
        load(staged, mode="full")
        load(staged, mode="incremental")
        upto, total = None, None
        con = _con(staged)
        try:
            upto, total = con.execute(
                "SELECT loaded_upto, rows_total FROM etl_watermark "
                "WHERE source_id = 'uci_online_retail'").fetchone()
        finally:
            con.close()
        assert upto == total == 13

    def test_run_history_is_appended_not_replaced(self, staged):
        load(staged, mode="full")
        load(staged, mode="incremental")
        modes = _one(staged, "SELECT COUNT(*) FROM etl_run")
        assert modes == 2
        failed = _one(staged, "SELECT COUNT(*) FROM etl_run WHERE status <> 'success'")
        assert failed == 0

    def test_full_rebuild_is_reproducible(self, staged):
        load(staged, mode="full")
        first = _key_fingerprint(staged)
        load(staged, mode="full")
        assert _key_fingerprint(staged) == first, \
            "the same staging input must always yield the same keys"

    def test_unknown_mode_rejected(self, staged):
        with pytest.raises(ValueError, match="unknown mode"):
            load(staged, mode="sideways")
