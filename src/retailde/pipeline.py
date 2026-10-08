"""Pipeline wiring: turn the stages into a real DAG and run it.

Task graph (arrows mean "must complete first")::

    extract.uci_online_retail   extract.open_meteo_london
           |                     extract.ecb_fx_gbp
           |                     extract.boe_bank_rate
           v                            |
    stage.transactions          stage.weather / stage.fx / stage.bank_rate
           |                            |
           +-----------+----------------+
                       v
                 quality.staging          <- gates the raw->staged boundary
                       |
                       v
                 load.warehouse
                       |
                       v
                 quality.warehouse         <- gates structure + reconciliation
                       |
                       v
                   marts.build
                       |
                       v
                report.dashboard

Fan-out at the top (four independent sources), fan-in at ``quality.staging``,
then a straight spine. Every task is idempotent, so re-running from any point
is safe -- which is what ``start``/``stop`` on :meth:`DAG.run` rely on.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .config import AppConfig
from .orchestrator import DAG, TaskResult, summarise


@dataclass
class RunReport:
    mode: str
    started_at: float
    results: list[TaskResult] = field(default_factory=list)
    quality: list[dict] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(r.status != "failed" for r in self.results) and \
            all(q.get("passed", True) for q in self.quality)

    @property
    def elapsed(self) -> float:
        return round(time.perf_counter() - self.started_at, 3)

    @property
    def loaded_rows(self) -> int:
        load = next((r for r in self.results if r.name == "load.warehouse"), None)
        return int((load.value or {}).get("rows_loaded", 0)) if load and load.value else 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mode": self.mode,
            "elapsed": self.elapsed,
            "loaded_rows": self.loaded_rows,
            "tasks": [r.as_dict() for r in self.results],
            "quality": self.quality,
            "stats": self.stats,
        }


def build_dag(cfg: AppConfig) -> DAG:
    """Assemble the DAG for this configuration."""
    from . import marts, quality, report, transform, warehouse
    from .extract import extract as run_extract
    from .manifest import Manifest

    cfg.ensure_dirs()
    dag = DAG()

    # -- extract: one independent task per source --------------------------
    manifest = Manifest(cfg.raw_dir)

    def _extract_task(src):
        """Build a zero-arg callable closing over one source.

        Written as a factory rather than a lambda in the loop: a lambda that
        captures ``src`` by reference would run the *last* source four times.
        """
        def _run() -> dict[str, Any]:
            return run_extract(src, cfg, manifest).as_dict()
        return _run

    for src in cfg.enabled_sources:
        dag.add(
            f"extract.{src.id}",
            _extract_task(src),
            retries=src.retries,
            backoff=1.0,
            description=f"{src.name} [{src.adapter}]",
        )

    # -- stage: fan out from the source each transform actually needs ------
    stage_deps = {
        "stage.transactions": "extract.uci_online_retail",
        "stage.weather": "extract.open_meteo_london",
        "stage.fx": "extract.ecb_fx_gbp",
        "stage.bank_rate": "extract.boe_bank_rate",
    }
    stage_fns = {
        "stage.transactions": lambda: transform.transactions.stage(cfg),
        "stage.weather": lambda: transform.weather.stage(cfg),
        "stage.fx": lambda: transform.fx.stage(cfg),
        "stage.bank_rate": lambda: transform.bank_rate.stage(cfg),
    }
    for name, fn in stage_fns.items():
        dag.add(name, fn, depends_on=(stage_deps[name],), retries=1,
                description="clean + type + deduplicate to Parquet")

    all_staging = tuple(stage_fns)

    # -- quality gates ------------------------------------------------------
    dag.add(
        "quality.staging",
        lambda: _quality_block(cfg, quality.staging_checks),
        depends_on=all_staging, retries=1,
        description="assert source volume, dirt tolerances, coverage",
    )
    dag.add(
        "quality.warehouse",
        lambda: _quality_block(cfg, quality.warehouse_checks),
        depends_on=("load.warehouse",), retries=1,
        description="assert referential integrity + cross-layer reconciliation",
    )

    # -- warehouse ----------------------------------------------------------
    dag.add(
        "load.warehouse",
        lambda: warehouse.load(cfg, mode="full"),
        depends_on=("quality.staging",), retries=1,
        description="build star schema in DuckDB",
    )

    # -- marts + report -----------------------------------------------------
    dag.add("marts.build", lambda: marts.build(cfg),
            depends_on=("quality.warehouse",), retries=1,
            description="5 business marts -> Parquet",
    )
    dag.add("report.dashboard", lambda: report.build(cfg),
            depends_on=("marts.build",), retries=1,
            description="KPI dashboard + charts -> data/reports",
    )

    return dag


def _quality_block(cfg: AppConfig, fn: Callable) -> dict[str, Any]:
    """Run a set of checks, raise if any fail (aborts the DAG)."""
    from .quality import assert_passing, report as render, to_json

    results = fn(cfg)
    assert_passing(results)
    return {"results": [r.as_dict() for r in results],
            "text": render(results),
            "json": to_json(results)}


def run(cfg: AppConfig, *, start: str | None = None, stop: str | None = None,
        dry_run: bool = False,
        on_event: Callable[[str, str], None] | None = None) -> RunReport:
    """Execute the pipeline and return a report of what happened."""
    from .quality import run_all

    dag = build_dag(cfg)
    report_obj = RunReport(mode="full" if start is None else f"from {start}",
                           started_at=time.perf_counter())

    results = dag.run(start=start, stop=stop, dry_run=dry_run, on_event=on_event,
                      sleep=time.sleep)
    report_obj.results = results

    # Collect every quality result the run produced, for the report.
    for r in results:
        if r.name.startswith("quality.") and isinstance(r.value, dict):
            report_obj.quality.extend(r.value.get("results", []))
        if r.name in ("stage.transactions", "stage.weather",
                      "stage.fx", "stage.bank_rate") and r.value:
            report_obj.stats[r.name.split(".", 1)[1]] = r.value

    if report_obj.ok and not dry_run:
        try:
            report_obj.quality = [q.as_dict() for q in run_all(cfg)]
        except Exception:
            pass

    return report_obj


__all__ = ["build_dag", "run", "RunReport", "summarise"]
