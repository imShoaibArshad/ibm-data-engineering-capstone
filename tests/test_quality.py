"""Quality-gate tests.

Two things matter about a gate: it passes on good data, and -- far more
importantly -- it *fails* on bad data. A suite that only asserts the first is
testing nothing.
"""

from __future__ import annotations

import pytest

from retailde import quality
from retailde.warehouse import load


class TestStagingGates:
    def test_all_pass_on_clean_fixture(self, staged):
        results = quality.staging_checks(staged)
        failed = [r.name for r in results if not r.passed]
        assert not failed, f"unexpected failures: {failed}"

    def test_gate_fails_when_volume_drops(self, staged):
        staged.quality["min_transaction_rows"] = 10_000_000
        results = quality.staging_checks(staged)
        hit = next(r for r in results if r.name == "transaction row count")
        assert not hit.passed, "a 10M minimum must fail against 13 rows"
        assert hit.observed == 13
        assert ">= 10,000,000" in hit.expected

    def test_gate_fails_when_null_rate_exceeds_tolerance(self, staged):
        staged.quality["null_customer_id_rate_max"] = 0.01
        results = quality.staging_checks(staged)
        hit = next(r for r in results if r.name == "null customer_id rate")
        assert not hit.passed, "0.1538 must fail a 1% ceiling"

    def test_gate_is_loose_enough_for_the_real_source_profile(self, staged):
        # The thresholds are derived from the source, not invented: the real
        # file ships 22.8% null customer IDs, so 0.25 is a meaningful ceiling.
        staged.quality["null_customer_id_rate_max"] = 0.25
        results = quality.staging_checks(staged)
        hit = next(r for r in results if r.name == "null customer_id rate")
        assert hit.passed

    def test_fx_gate_measures_weekdays_not_calendar_days(self, staged):
        results = quality.staging_checks(staged)
        hit = next(r for r in results if r.name == "FX coverage of weekdays")
        assert hit.passed, "7/7 weekdays must pass"
        assert "calendar-day coverage" in hit.detail, \
            "the detail should explain why the denominator was chosen"

    def test_missing_staging_artefacts_fails_loudly(self, project):
        results = quality.staging_checks(project)
        assert any(not r.passed for r in results)


class TestWarehouseGates:
    def test_all_pass_after_a_good_load(self, staged):
        load(staged, mode="full")
        results = quality.warehouse_checks(staged)
        failed = [r.name for r in results if not r.passed]
        assert not failed, f"unexpected failures: {failed}"

    def test_missing_warehouse_is_reported_not_crashed(self, project):
        results = quality.warehouse_checks(project)
        assert len(results) == 1
        assert not results[0].passed

    def test_referential_integrity_check_uses_zero_tolerance(self, staged):
        load(staged, mode="full")
        staged.quality["require_referential_integrity"] = True
        results = quality.warehouse_checks(staged)
        ri = next(r for r in results if r.name == "referential integrity")
        assert ri.expected == "0 orphans"
        assert ri.passed


class TestReporting:
    def test_report_summarises_both_phases(self, staged):
        load(staged, mode="full")
        text = quality.report(quality.run_all(staged))
        assert "STAGING CHECKS" in text
        assert "WAREHOUSE CHECKS" in text
        assert "passed" in text

    def test_json_output_is_serialisable(self, staged):
        load(staged, mode="full")
        import json
        payload = json.loads(quality.to_json(quality.run_all(staged)))
        assert isinstance(payload, list) and payload
        assert {"name", "passed", "observed", "expected", "phase"} <= set(payload[0])

    def test_assert_passing_raises_with_failures_attached(self, staged):
        staged.quality["min_transaction_rows"] = 10_000_000
        results = quality.staging_checks(staged)
        with pytest.raises(quality.QualityError) as exc:
            quality.assert_passing(results)
        assert exc.value.failures
        assert "quality check(s) failed" in str(exc.value)
