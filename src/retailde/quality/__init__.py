"""Data-quality gates: measurable assertions that abort the pipeline."""

from __future__ import annotations

from .checks import (CheckResult, QualityError, assert_passing, report,
                     run_all, staging_checks, to_json, warehouse_checks)

__all__ = ["CheckResult", "QualityError", "assert_passing", "report",
           "run_all", "staging_checks", "warehouse_checks", "to_json"]
