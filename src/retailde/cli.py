"""Command-line interface.

Entry points::

    python -m retailde <command>
    retailde <command>            # after `pip install -e .`

Every command accepts ``--root`` so tests and CI can point the whole pipeline
at a temporary directory without touching the real data.

Exit codes are meaningful and used by the shell scripts: 0 = success,
1 = a quality gate failed, 2 = the run itself errored.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

import click

from .config import AppConfig
from .orchestrator import summarise

EXIT_OK = 0
EXIT_QUALITY = 1
EXIT_ERROR = 2


def _cfg(root: str | None) -> AppConfig:
    cfg = AppConfig.load(root=root) if root else AppConfig.load()
    cfg.ensure_dirs()
    return cfg


def _echo_quality(results: list[Any]) -> None:
    from .quality import report as render
    click.echo(render(results))


def _fail_quality(results: list[Any]) -> None:
    from .quality import QualityError, assert_passing
    try:
        assert_passing(results)
    except QualityError as exc:
        click.echo("", err=True)
        for f in exc.failures:
            click.echo(f"  FAILED {f.name}: observed {f.observed}, "
                       f"expected {f.expected}", err=True)
            if f.detail:
                click.echo(f"         {f.detail}", err=True)
        click.echo(f"\n{len(exc.failures)} quality check(s) failed", err=True)
        sys.exit(EXIT_QUALITY)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--root", default=None, type=click.Path(exists=False, file_okay=False),
              help="Project root (default: the installed repository).")
@click.pass_context
def cli(ctx: click.Context, root: str | None) -> None:
    """UK retail analytics -- IBM Data Engineering Capstone pipeline.

    extract -> stage -> quality -> load -> marts -> report, runnable entirely
    on a local machine with no cloud account.
    """
    ctx.ensure_object(dict)
    ctx.obj["root"] = root


# ---------------------------------------------------------------------------
# individual stages
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--force", is_flag=True, help="Ignore the raw-zone cache and re-fetch.")
@click.option("--offline", is_flag=True, help="Never touch the network.")
@click.pass_context
def extract(ctx: click.Context, force: bool, offline: bool) -> None:
    """Download/parse every enabled source into data/raw."""
    from .extract import extract as run_extract
    from .manifest import Manifest

    cfg = _cfg(ctx.obj["root"])
    manifest = Manifest(cfg.raw_dir)
    click.echo("EXTRACT")
    failed = []
    for src in cfg.enabled_sources:
        try:
            rec = run_extract(src, cfg, manifest, force=force, offline=offline)
            click.echo(f"  {src.id:<22} {rec.status:<8} {rec.bytes:>13,} B  "
                       f"{rec.sha256[:12]}  {rec.detail}")
        except Exception as exc:
            failed.append(src.id)
            click.echo(f"  {src.id:<22} FAILED   {type(exc).__name__}: {exc}")

    manifest.prune({s.id for s in cfg.sources})
    if failed:
        click.echo(f"\n{len(failed)} source(s) failed: {', '.join(failed)}", err=True)
        sys.exit(EXIT_ERROR)


@cli.command()
@click.option("--force", is_flag=True, help="Ignore the Parquet source cache.")
@click.pass_context
def stage(ctx: click.Context, force: bool) -> None:
    """Clean and type raw artefacts into data/staging Parquet."""
    from .transform import stage_all

    cfg = _cfg(ctx.obj["root"])
    click.echo("STAGE")
    for stats in stage_all(cfg, force=force):
        click.echo(f"  {stats['table']:<26} {stats.get('rows_out', 0):>10,} rows  "
                   f"({stats.get('elapsed', 0)}s)")


@cli.command()
@click.option("--mode", type=click.Choice(["full", "incremental"]),
              default="full", show_default=True,
              help="full = rebuild; incremental = append above the watermark.")
@click.pass_context
def load(ctx: click.Context, mode: str) -> None:
    """Load staging into the DuckDB star schema."""
    from .warehouse import load as load_warehouse

    cfg = _cfg(ctx.obj["root"])
    click.echo(f"LOAD  (mode={mode})")
    stats = load_warehouse(cfg, mode=mode)
    click.echo(f"  rows loaded : {stats['rows_loaded']:,}")
    click.echo(f"  elapsed     : {stats['elapsed']}s")
    for table, n in stats["key_counts"].items():
        click.echo(f"  {table:<20} {n:>10,} keys")


@cli.command()
@click.option("--phase", type=click.Choice(["all", "staging", "warehouse"]),
              default="all", show_default=True)
@click.pass_context
def quality(ctx: click.Context, phase: str) -> None:
    """Run the data-quality gates; non-zero exit if any fail."""
    from .quality import run_all, staging_checks, warehouse_checks

    cfg = _cfg(ctx.obj["root"])
    if phase == "staging":
        results = staging_checks(cfg)
    elif phase == "warehouse":
        results = warehouse_checks(cfg)
    else:
        results = run_all(cfg)
    _echo_quality(results)
    _fail_quality(results)


@cli.command()
@click.pass_context
def marts(ctx: click.Context) -> None:
    """(Re)build the five business marts and export them to Parquet."""
    from .marts import build

    cfg = _cfg(ctx.obj["root"])
    stats = build(cfg)
    click.echo("MARTS")
    for name, info in stats["marts"].items():
        click.echo(f"  {name:<28} {info['rows']:>9,} rows  {info['elapsed']}s")
    click.echo(f"  {stats['count']} marts in {stats['elapsed']}s")


@cli.command()
@click.pass_context
def report(ctx: click.Context) -> None:
    """Render charts and dashboard.html into data/reports."""
    from .report import build

    cfg = _cfg(ctx.obj["root"])
    stats = build(cfg)
    click.echo("REPORT")
    for chart in stats["charts"]:
        click.echo(f"  {chart['name']:<28} {chart['bytes']:>9,} B")
    click.echo(f"  {stats['html']}")
    k = stats["kpis"]
    click.echo(f"  revenue {k['revenue']:,.2f}  orders {k['orders']:,}  "
               f"customers {k['customers']:,}")


# ---------------------------------------------------------------------------
# whole pipeline
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--start", default=None, metavar="TASK",
              help="Resume from this task (skip its upstream).")
@click.option("--stop", default=None, metavar="TASK",
              help="Stop after this task.")
@click.option("--dry-run", is_flag=True, help="Print the plan; run nothing.")
@click.pass_context
def run(ctx: click.Context, start: str | None, stop: str | None,
        dry_run: bool) -> None:
    """Run the full DAG: extract -> stage -> quality -> load -> marts -> report."""
    from . import pipeline
    from .quality import run_all as run_quality

    cfg = _cfg(ctx.obj["root"])

    def on_event(name: str, state: str) -> None:
        if state == "started":
            click.echo(f"\n--> {name}")
        elif state in ("success", "failed", "skipped"):
            click.echo(f"    {state}")

    try:
        report_obj = pipeline.run(cfg, start=start, stop=stop,
                                  dry_run=dry_run, on_event=on_event)
    except Exception as exc:
        click.echo(f"\nPIPELINE ERROR: {type(exc).__name__}: {exc}", err=True)
        sys.exit(EXIT_ERROR)

    click.echo("\n" + "=" * 74)
    click.echo(summarise(report_obj.results))

    if dry_run:
        click.echo("\n(dry run -- nothing executed)")
        return

    if report_obj.quality:
        from .quality import CheckResult
        click.echo("")
        _echo_quality([CheckResult(**q) for q in report_obj.quality])

    if not report_obj.ok:
        click.echo("\nPIPELINE FAILED", err=True)
        sys.exit(EXIT_QUALITY)

    click.echo(f"\nOK -- {report_obj.loaded_rows:,} rows loaded in "
               f"{report_obj.elapsed}s")


@cli.command()
@click.pass_context
def plan(ctx: click.Context) -> None:
    """Show the task DAG in execution order, without running anything."""
    from . import pipeline

    cfg = _cfg(ctx.obj["root"])
    dag = pipeline.build_dag(cfg)
    order = dag.plan()
    click.echo(f"{len(order)} tasks:\n")
    for i, name in enumerate(order, 1):
        task = dag.tasks[name]
        deps = ", ".join(task.depends_on) or "-"
        click.echo(f"  {i:>2}. {name:<28} {task.description}")
        click.echo(f"      deps: {deps}")


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show warehouse counts, provenance, and the last quality result."""
    from .manifest import Manifest
    from .warehouse import counts

    cfg = _cfg(ctx.obj["root"])
    click.echo("RAW")
    for row in Manifest(cfg.raw_dir).summary_rows():
        click.echo(f"  {row['source']:<22} {row['status']:<8} "
                   f"{row['bytes']:>13,} B  {row['fetched_at']}")

    click.echo("\nSTAGING")
    staging = cfg.staging_dir
    for p in sorted(staging.glob("stg_*.parquet")):
        click.echo(f"  {p.name:<34} {p.stat().st_size:>13,} B")

    click.echo("\nWAREHOUSE")
    warehouse_counts = counts(cfg)
    if not warehouse_counts:
        click.echo("  (not built yet -- run `retailde run`)")
    for table, n in warehouse_counts.items():
        click.echo(f"  {table:<26} {n:>12,}")

    click.echo("\nMARTS")
    for p in sorted(cfg.marts_dir.glob("*.parquet")):
        click.echo(f"  {p.name:<34} {p.stat().st_size:>13,} B")

    click.echo("\nREPORTS")
    for p in sorted(cfg.reports_dir.glob("*")):
        click.echo(f"  {p.name:<34} {p.stat().st_size:>13,} B")


@cli.command()
@click.option("--yes", is_flag=True, help="Do not prompt.")
@click.pass_context
def clean(ctx: click.Context, yes: bool) -> None:
    """Delete every generated artefact (raw, staging, warehouse, reports)."""
    cfg = _cfg(ctx.obj["root"])
    targets = [cfg.raw_dir, cfg.staging_dir, cfg.marts_dir,
               cfg.reports_dir, cfg.logs_dir, cfg.warehouse_path]
    if not yes and not click.confirm(
        f"Delete {[str(t.name) for t in targets]} under {cfg.root}?"
    ):
        click.echo("aborted")
        return
    for target in targets:
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink()
    cfg.ensure_dirs()
    click.echo("cleaned")


def main() -> None:
    """Console-script entry point."""
    try:
        cli(obj={})
    except SystemExit:
        raise
    except Exception as exc:  # last-resort guard so scripts get a sane code
        click.echo(f"error: {type(exc).__name__}: {exc}", err=True)
        sys.exit(EXIT_ERROR)


if __name__ == "__main__":
    main()
