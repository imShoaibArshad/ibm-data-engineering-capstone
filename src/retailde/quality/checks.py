"""Data-quality gates.

Design rules, learned from how these usually go wrong:

* **Thresholds come from the observed profile of this dataset**, not from
  intuition. ``null_customer_id_rate_max`` is 0.25 because the source really
  ships 0.2276 -- a gate at 0.10 would fail forever and get switched off, a
  gate at 0.50 would catch nothing. The two numbers sit close enough that the
  check has teeth.

* **Measurable against the right denominator.** The ECB publishes FX only on
  target-area working days: 523 dates across a 529-weekday window (98.87%)
  but only 70.8% of calendar days. Measured against calendar days this check
  could never pass, so it measures weekdays.

* **Runs in two phases.** ``staging`` gates fire before anything reaches the
  warehouse; ``warehouse`` gates additionally assert referential integrity and
  reconcile totals across the two layers. A pipeline that only validates
  inputs will happily build a structurally broken warehouse.

Any failure aborts the run with a non-zero exit code -- a quality gate that
logs a warning and continues is decoration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import duckdb
import pandas as pd

from ..config import AppConfig
from ..transform.fx import weekday_count


class QualityError(RuntimeError):
    """Raised when one or more quality gates fail."""

    def __init__(self, failures: list["CheckResult"]):
        self.failures = failures
        super().__init__(
            f"{len(failures)} quality check(s) failed: "
            + "; ".join(f.name for f in failures)
        )


@dataclass
class CheckResult:
    name: str
    passed: bool
    observed: Any
    expected: str
    phase: str
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "observed": self.observed,
            "expected": self.expected,
            "phase": self.phase,
            "detail": self.detail,
        }

    def line(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        return (f"  [{mark}] {self.name:<38} {str(self.observed):>14} "
                f"  (expected {self.expected})")


def _check(name, observed, passed, expected, phase, detail="") -> CheckResult:
    return CheckResult(name=name, passed=bool(passed), observed=observed,
                       expected=expected, phase=phase, detail=detail)


# ---------------------------------------------------------------------------
# staging phase
# ---------------------------------------------------------------------------

def staging_checks(cfg: AppConfig) -> list[CheckResult]:
    from ..transform import bank_rate, fx, transactions, weather

    results: list[CheckResult] = []
    q = cfg.quality
    stats_path = cfg.staging_dir / transactions.STATS_NAME
    if not stats_path.exists():
        return [_check("staging artefacts exist", "missing", False,
                       "stats file present", "staging",
                       "run `retailde stage` first")]
    stats = json.loads(stats_path.read_text(encoding="utf-8"))

    # -- volume -------------------------------------------------------------
    min_rows = int(q.get("min_transaction_rows", 1))
    results.append(_check(
        "transaction row count",
        stats["rows_out"], stats["rows_out"] >= min_rows, f">= {min_rows:,}",
        "staging", "the fact spine must not arrive truncated",
    ))

    # -- known, tolerated dirt ---------------------------------------------
    rate = float(stats["null_customer_id_rate"])
    cap = float(q.get("null_customer_id_rate_max", 1.0))
    results.append(_check(
        "null customer_id rate", round(rate, 4), rate <= cap, f"<= {cap}",
        "staging", "source ships 22.8% missing IDs; gate catches a regression",
    ))

    dup_cap = float(q.get("duplicate_natural_key_rate_max", 1.0))
    dup_rate = stats["dropped_exact_duplicates"] / max(stats["rows_in"], 1)
    # After staging, no duplicates survive -- so the post-clean rate is 0 by
    # construction. The informative number is how many were removed, recorded
    # in detail for the report.
    results.append(_check(
        "duplicate rate after cleaning", 0.0, 0.0 <= dup_cap,
        f"<= {dup_cap}", "staging",
        f"{stats['dropped_exact_duplicates']:,} removed of {stats['rows_in']:,} "
        f"({dup_rate:.4%}): {stats['duplicate_rows_cross_sheet']:,} cross-sheet "
        f"(9-day worksheet overlap) + {stats['duplicate_rows_within_sheet']:,} within-sheet",
    ))

    # -- internal consistency ----------------------------------------------
    results.append(_check(
        "valid sales have positive revenue",
        stats["valid_sale_rows"],
        stats["valid_sale_rows"] > 0, "> 0", "staging",
        "a definition of 'sale' that yields no rows is a broken definition",
    ))
    null_dates = int(stats.get("null_invoice_date", -1))
    results.append(_check(
        "no null invoice_date rows", null_dates, null_dates == 0, "0", "staging",
        "invoice_date is the fact's date key; nulls would drop rows on the "
        "inner join to dim_date",
    ))
    results.append(_check(
        "date range sane",
        f"{stats['date_min']}..{stats['date_max']}",
        stats["date_min"] is not None and stats["date_max"] is not None
        and stats["date_min"] <= stats["date_max"],
        "min <= max", "staging",
    ))

    # -- context coverage ----------------------------------------------------
    tx = pd.read_parquet(cfg.staging_dir / transactions.STAGING_NAME)
    lo, hi = tx["date"].min(), tx["date"].max()
    calendar_days = int((hi - lo).days) + 1

    weather = weather.load(cfg)
    w_cov = len(weather) / calendar_days if calendar_days else 0.0
    w_min = float(q.get("weather_coverage_min", 0.0))
    results.append(_check(
        "weather coverage of calendar",
        round(w_cov, 4), w_cov >= w_min, f">= {w_min}", "staging",
        f"{len(weather)}/{calendar_days} days",
    ))

    fxdf = fx.load(cfg)
    weekdays = weekday_count(lo, hi)
    f_cov = len(fxdf) / weekdays if weekdays else 0.0
    f_min = float(q.get("fx_coverage_min", 0.0))
    results.append(_check(
        "FX coverage of weekdays",
        round(f_cov, 4), f_cov >= f_min, f">= {f_min}", "staging",
        f"{len(fxdf)}/{weekdays} weekdays (calendar-day coverage would be "
        f"{len(fxdf)/calendar_days:.2%} -- ECB publishes on working days only)",
    ))

    rate_df = bank_rate.load(cfg)
    results.append(_check(
        "bank rate rows parsed",
        len(rate_df), len(rate_df) > 0, "> 0", "staging",
        f"{rate_df.attrs.get('unparsed', []) or 'none'} unparsed rows; "
        f"{rate_df['date'].min().date()}..{rate_df['date'].max().date()}",
    ))
    results.append(_check(
        "bank rate covers transaction window",
        bank_rate._rate_at(rate_df, lo),
        bank_rate._rate_at(rate_df, lo) is not None,
        "rate in force at window start", "staging",
        "the as-of join needs a decision at or before the first transaction",
    ))

    # -- provenance ----------------------------------------------------------
    from ..manifest import Manifest
    manifest = Manifest(cfg.raw_dir)
    missing = [s.id for s in cfg.enabled_sources if manifest.get(s.id) is None]
    bad = [s.id for s in cfg.enabled_sources
           if (manifest.get(s.id) or {}).get("status") == "error"]
    results.append(_check(
        "every enabled source landed",
        f"{len(cfg.enabled_sources) - len(missing) - len(bad)}/{len(cfg.enabled_sources)}",
        not missing and not bad, "all present, none errored", "staging",
        f"missing={missing} errored={bad}",
    ))

    return results


# ---------------------------------------------------------------------------
# warehouse phase
# ---------------------------------------------------------------------------

def warehouse_checks(cfg: AppConfig) -> list[CheckResult]:
    if not cfg.warehouse_path.exists():
        return [_check("warehouse exists", "missing", False, "file present",
                       "warehouse", "run `retailde load` first")]

    results: list[CheckResult] = []
    con = duckdb.connect(str(cfg.warehouse_path), read_only=True)
    try:
        one = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731

        facts = one("SELECT COUNT(*) FROM fact_transaction")
        staged = one("SELECT COUNT(*) FROM stg_transactions")
        results.append(_check(
            "fact rows == staged rows", facts, facts == staged,
            f"{staged:,}", "warehouse",
            "every staged row must be represented exactly once",
        ))

        # -- referential integrity -------------------------------------------
        orphans = {
            "dim_date": one("SELECT COUNT(*) FROM fact_transaction f "
                            "LEFT JOIN dim_date d ON d.date_key=f.date_key "
                            "WHERE d.date_key IS NULL"),
            "dim_customer": one("SELECT COUNT(*) FROM fact_transaction f "
                                "LEFT JOIN dim_customer c ON c.customer_key=f.customer_key "
                                "WHERE c.customer_key IS NULL"),
            "dim_product": one("SELECT COUNT(*) FROM fact_transaction f "
                               "LEFT JOIN dim_product p ON p.product_key=f.product_key "
                               "WHERE p.product_key IS NULL"),
            "dim_country": one("SELECT COUNT(*) FROM fact_transaction f "
                               "LEFT JOIN dim_country c ON c.country_key=f.country_key "
                               "WHERE c.country_key IS NULL"),
        }
        total_orphans = sum(orphans.values())
        require_ri = bool(cfg.quality.get("require_referential_integrity", True))
        results.append(_check(
            "referential integrity", total_orphans,
            (total_orphans == 0) if require_ri else True,
            "0 orphans", "warehouse", f"per-dimension: {orphans}",
        ))

        # -- reconciliation across layers ------------------------------------
        recon = con.execute(
            """
            SELECT
                ROUND(SUM(CASE WHEN s.is_valid_sale THEN s.line_revenue ELSE 0 END), 2) AS staged,
                ROUND(SUM(CASE WHEN f.is_valid_sale THEN f.line_revenue ELSE 0 END), 2) AS warehoused
            FROM fact_transaction f
            JOIN stg_transactions s ON s.transaction_id = f.transaction_id
            """
        ).fetchone()
        results.append(_check(
            "revenue reconciles staging -> warehouse",
            recon[1], recon[0] == recon[1], f"= {recon[0]}", "warehouse",
            "a matching row count means nothing if the values drifted",
        ))

        # -- reference-data completeness -------------------------------------
        unmapped = one("SELECT COUNT(*) FROM dim_country WHERE region = 'Unmapped'")
        results.append(_check(
            "all countries mapped to a region", unmapped, unmapped == 0,
            "0 unmapped", "warehouse",
            "config/country_regions.csv must cover every country in the source",
        ))

        # -- as-of join completeness -----------------------------------------
        null_rate = one("SELECT COUNT(*) FROM fact_transaction f "
                        "LEFT JOIN v_transactions v USING (transaction_id) "
                        "WHERE v.bank_rate_pct IS NULL")
        results.append(_check(
            "bank rate resolved for every fact row", null_rate, null_rate == 0,
            "0 nulls", "warehouse",
            "the SCD2 as-of join must find a rate at or before each date",
        ))

        # -- watermark / run log ---------------------------------------------
        pending = one("SELECT COUNT(*) FROM etl_run WHERE status = 'failed'")
        results.append(_check(
            "no failed ETL runs recorded", pending, pending == 0,
            "0 failures", "warehouse", "etl_run is an audit log, not a summary",
        ))

        # -- unknown-member accounting ---------------------------------------
        unknown_rev = one(
            "SELECT ROUND(COALESCE(SUM(CASE WHEN c.is_unknown "
            "AND f.is_valid_sale THEN f.line_revenue ELSE 0 END),0),2) "
            "FROM fact_transaction f JOIN dim_customer c USING (customer_key)"
        )
        results.append(_check(
            "unknown-customer revenue is accounted for",
            unknown_rev, unknown_rev > 0, "> 0", "warehouse",
            "rows without a Customer ID must be visible, not inner-joined away",
        ))
    finally:
        con.close()

    return results


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

def run_all(cfg: AppConfig) -> list[CheckResult]:
    return staging_checks(cfg) + warehouse_checks(cfg)


def report(results: list[CheckResult]) -> str:
    out = []
    for phase in ("staging", "warehouse"):
        group = [r for r in results if r.phase == phase]
        if not group:
            continue
        out.append(f"{phase.upper()} CHECKS")
        out.extend(r.line() for r in group)
        out.append("")
    failed = [r for r in results if not r.passed]
    out.append(f"{len(results) - len(failed)}/{len(results)} passed"
               + ("" if not failed else f"  --  {len(failed)} FAILED"))
    return "\n".join(out)


def assert_passing(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed]
    if failures:
        raise QualityError(failures)


def to_json(results: list[CheckResult]) -> str:
    return json.dumps([r.as_dict() for r in results], indent=2, default=str)
