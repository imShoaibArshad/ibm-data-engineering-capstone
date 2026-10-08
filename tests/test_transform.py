"""Transform tests: cleaning rules and the three context parsers.

All pure functions over data already on disk -- no network, no warehouse.
"""

from __future__ import annotations

import pandas as pd
import pytest

from retailde.transform import bank_rate, fx, transactions, weather


# ---------------------------------------------------------------------------
# identifier normalisation
# ---------------------------------------------------------------------------

class TestToIdString:
    def test_whole_number_becomes_plain_string(self):
        out = transactions.to_id_string(pd.Series([489434, 79323]))
        assert out.tolist() == ["489434", "79323"]

    def test_float_artefact_from_excel_is_repaired(self):
        # This is the whole reason the helper exists: an Excel column holding
        # both ints and text arrives as floats, and 85048.0 would never join.
        out = transactions.to_id_string(pd.Series([85048.0, 79323.0]))
        assert out.tolist() == ["85048", "79323"], "must not leak '.0'"

    def test_mixed_int_and_string_column(self):
        out = transactions.to_id_string(pd.Series([489434, "C489449", 85048.0]))
        assert out.tolist() == ["489434", "C489449", "85048"]

    def test_blanks_become_missing(self):
        out = transactions.to_id_string(pd.Series([None, "", "  22633  "]))
        assert pd.isna(out[0]) and pd.isna(out[1])
        assert out[2] == "22633", "whitespace must be stripped"

    def test_negative_and_decimal_values(self):
        out = transactions.to_id_string(pd.Series([-1.0, 1.5]))
        assert out.tolist() == ["-1", "1.5"]


# ---------------------------------------------------------------------------
# transaction cleaning
# ---------------------------------------------------------------------------

class TestCleanTransactions:
    @pytest.fixture()
    def cleaned(self, project):
        src = transactions.read_source(project, force=True)
        return transactions.clean(src)

    def test_source_reads_both_worksheets(self, project):
        src = transactions.read_source(project, force=True)
        assert set(src["source_sheet"]) == {"Year 2009-2010", "Year 2010-2011"}
        assert len(src) == 15, "10 rows in sheet 1, 5 in sheet 2"

    def test_expected_columns_after_rename(self, cleaned):
        df, _ = cleaned
        for col in ("invoice", "stock_code", "description", "quantity",
                    "invoice_date", "unit_price", "customer_id", "country",
                    "line_revenue", "is_cancelled", "is_return",
                    "is_valid_sale", "transaction_id"):
            assert col in df.columns, f"missing {col}"

    def test_duplicates_split_by_cause(self, cleaned):
        _, stats = cleaned
        assert stats["dropped_exact_duplicates"] == 2
        assert stats["duplicate_rows_cross_sheet"] == 1, \
            "the sheet-1/sheet-2 overlap row must be attributed to the overlap"
        assert stats["duplicate_rows_within_sheet"] == 1
        assert (stats["duplicate_rows_cross_sheet"]
                + stats["duplicate_rows_within_sheet"]
                == stats["dropped_exact_duplicates"])

    def test_worksheet_overlap_is_measured_before_dedup(self, cleaned):
        # Measuring after removal would show perfectly contiguous sheets and
        # the defect would vanish from the profile.
        _, stats = cleaned
        assert stats["worksheet_overlap_start"] == "2010-12-05"
        assert stats["worksheet_overlap_end"] == "2010-12-06"
        assert stats["worksheet_overlap_days"] == 2

    def test_row_counts(self, cleaned):
        df, stats = cleaned
        assert stats["rows_in"] == 15
        assert stats["rows_out"] == 13 == len(df)

    def test_business_events_are_retained_not_deleted(self, cleaned):
        _, stats = cleaned
        # returns + cancellations survive; only exact duplicates are removed
        assert stats["cancelled_rows"] >= 1, "the 'C489449' credit note"
        assert stats["negative_quantity_rows"] >= 1
        assert stats["nonpositive_price_rows"] >= 2, "two zero-price lines"

    def test_null_customer_ids_survive_and_are_counted(self, cleaned):
        _, stats = cleaned
        assert stats["null_customer_id"] == 2
        assert stats["rows_out"] == 13, "missing Customer ID must NOT drop rows"

    def test_description_whitespace_stripped_and_blanks_nulled(self, cleaned):
        df, stats = cleaned
        stripped = df["description"].dropna()
        assert not stripped.str.match(r"^\s|\s$").any(), "must be trimmed"
        assert stats["null_description"] == 1, "sheet-1 row with no description"
        assert int(df["description"].isna().sum()) == stats["null_description"]

    def test_stock_codes_are_strings_not_floats(self, cleaned):
        df, _ = cleaned
        codes = df["stock_code"].dropna().tolist()
        assert not any("." in c for c in codes), f"float leakage: {codes}"
        assert "85048" in codes and "79323P" in codes

    def test_is_valid_sale_excludes_cancellations_returns_and_free_lines(self, cleaned):
        df, _ = cleaned
        bad = df["is_cancelled"] | df["is_return"] | df["unit_price"].le(0)
        assert not (df["is_valid_sale"] & bad).any(), \
            "a valid sale can never be a cancellation, return, or free item"

    def test_line_revenue_is_quantity_times_price(self, cleaned):
        df, _ = cleaned
        expected = df["quantity"].astype(float) * df["unit_price"]
        assert (df["line_revenue"] - expected).abs().max() < 1e-9

    def test_transaction_ids_are_unique_and_dense(self, cleaned):
        df, _ = cleaned
        assert df["transaction_id"].is_unique
        assert sorted(df["transaction_id"]) == list(range(1, len(df) + 1))

    def test_dates_are_sorted(self, cleaned):
        df, _ = cleaned
        assert df["invoice_date"].is_monotonic_increasing


# ---------------------------------------------------------------------------
# weather
# ---------------------------------------------------------------------------

class TestWeather:
    def test_positional_arrays_become_rows(self, project):
        weather.stage(project)
        df = weather.load(project)
        assert len(df) == 9
        assert list(df.columns) == ["date", "tmax_c", "tmin_c", "precip_mm", "is_wet"]

    def test_misaligned_arrays_are_rejected(self):
        with pytest.raises(ValueError, match="not aligned"):
            weather.parse({"daily": {"time": ["2010-12-02", "2010-12-03"],
                                     "temperature_2m_max": [1.0],
                                     "temperature_2m_min": [1.0, 2.0],
                                     "precipitation_sum": [0.0, 0.0]}})

    def test_missing_daily_block_is_rejected(self):
        with pytest.raises(ValueError, match="no 'daily' block"):
            weather.parse({"latitude": 51.5})

    def test_wet_day_flag(self, project):
        weather.stage(project)
        df = weather.load(project)
        # index by ISO string: .dt.date yields datetime.date objects, which a
        # string key will not match
        wet = df.assign(day=df["date"].dt.strftime("%Y-%m-%d")).set_index("day")["is_wet"]
        assert bool(wet["2010-12-07"]) is True, "12.4mm is wet"
        assert bool(wet["2010-12-03"]) is False, "0.0mm is dry"
        assert bool(wet["2010-12-02"]) is False, \
            "0.3mm is below the 0.5mm threshold -- boundary just inside 'dry'"


# ---------------------------------------------------------------------------
# FX
# ---------------------------------------------------------------------------

class TestFx:
    def test_dict_projected_to_rows(self, project):
        fx.stage(project)
        df = fx.load(project)
        assert len(df) == 7
        assert list(df.columns) == ["date", "gbp_to_usd", "gbp_to_eur"]

    def test_weekday_count_excludes_weekends(self):
        # 2010-12-02 (Thu) .. 2010-12-10 (Fri) = 9 days, 7 weekdays
        n = fx.weekday_count(pd.Timestamp("2010-12-02"), pd.Timestamp("2010-12-10"))
        assert n == 7

    def test_coverage_is_measured_against_weekdays_not_calendar_days(self, project):
        stats = fx.stage(project)
        assert stats["weekdays_in_window"] == 7
        assert stats["weekday_coverage"] == 1.0
        # the ECB skips weekends, so calendar-day coverage would be ~78%
        assert stats["weekday_coverage"] > 7 / 9


# ---------------------------------------------------------------------------
# bank rate (HTML scrape)
# ---------------------------------------------------------------------------

class TestBankRate:
    def test_table_selected_by_header(self, project):
        bank_rate.stage(project)
        df = bank_rate.load(project)
        assert len(df) == 6, "the cookie table must not be parsed as data"

    def test_two_digit_year_dates_parse(self, project):
        bank_rate.stage(project)
        df = bank_rate.load(project)
        assert df["date"].min().year == 2008
        assert df["date"].max().year == 2025

    def test_rates_are_floats_and_sorted(self, project):
        bank_rate.stage(project)
        df = bank_rate.load(project)
        assert df["date"].is_monotonic_increasing
        assert df["rate_pct"].dtype.kind == "f"

    def test_unparsed_rows_are_reported_not_silently_dropped(self, project):
        stats = bank_rate.stage(project)
        assert stats["unparsed_rows"] == 0

    def test_rate_in_force_at_window_start_is_resolved(self, project):
        stats = bank_rate.stage(project)
        # last decision at or before 2010-12-02 is 05 Mar 09 -> 0.50%
        assert stats["rate_in_force_at_window_start"] == 0.5

    def test_missing_table_raises(self):
        with pytest.raises(ValueError, match="no HTML table found"):
            bank_rate.parse("<html><body><p>layout changed</p></body></html>")

    def test_effective_date_logic(self):
        df = bank_rate.parse(BANK_RATE_SAMPLE)
        # a date after a decision inherits it
        assert bank_rate._rate_at(df, pd.Timestamp("2010-06-01")) == 0.5
        assert bank_rate._rate_at(df, pd.Timestamp("2009-02-01")) == 1.5
        # ...but a date before the *first* decision has no rate at all.
        # Returning None rather than defaulting to the first value is the
        # correct behaviour: no decision had been made yet.
        assert bank_rate._rate_at(df, pd.Timestamp("2009-01-01")) is None


BANK_RATE_SAMPLE = """
<table><tr><th>Date Changed</th><th>Rate</th></tr>
<tr><td>05 Mar 09</td><td>0.50</td></tr>
<tr><td>05 Feb 09</td><td>1.00</td></tr>
<tr><td>08 Jan 09</td><td>1.50</td></tr>
</table>
"""
