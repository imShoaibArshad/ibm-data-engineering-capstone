"""End-to-end: the whole DAG against the synthetic project.

These are the tests that would catch a wiring mistake -- a stage registered
with the wrong dependency, a task name that does not match its key in the
plan, a gate that never runs. The unit tests cannot see any of that.

They also prove the offline claim: every source is already cached in the raw
zone, so the full pipeline completes without the network ever being touched.
"""

from __future__ import annotations

import pytest

from retailde import pipeline, quality
from retailde.orchestrator import STATUS_SUCCESS


@pytest.fixture()
def report(project):
    """The complete pipeline run against the synthetic fixture."""
    return pipeline.run(project)


class TestDagShape:
    def test_every_stage_is_present(self, project):
        names = set(pipeline.build_dag(project).plan())
        for expected in (
            "extract.uci_online_retail", "extract.open_meteo_london",
            "extract.ecb_fx_gbp", "extract.boe_bank_rate",
            "stage.transactions", "stage.weather", "stage.fx", "stage.bank_rate",
            "quality.staging", "load.warehouse", "quality.warehouse",
            "marts.build", "report.dashboard",
        ):
            assert expected in names, f"missing task {expected}"

    def test_order_is_a_valid_topological_sort(self, project):
        dag = pipeline.build_dag(project)
        order = dag.order()
        position = {name: i for i, name in enumerate(order)}
        for name, task in dag.tasks.items():
            for dep in task.depends_on:
                assert position[dep] < position[name], \
                    f"{dep} must run before {name}"

    def test_extracts_have_no_dependencies(self, project):
        """The four sources are independent -- that is what makes them a
        fan-out rather than a chain."""
        dag = pipeline.build_dag(project)
        for name in dag.tasks:
            if name.startswith("extract."):
                assert dag.tasks[name].depends_on == ()

    def test_gates_sit_between_the_boundaries_they_protect(self, project):
        dag = pipeline.build_dag(project)
        assert set(dag.tasks["quality.staging"].depends_on) == {
            "stage.transactions", "stage.weather", "stage.fx", "stage.bank_rate"}
        assert dag.tasks["load.warehouse"].depends_on == ("quality.staging",)
        assert dag.tasks["quality.warehouse"].depends_on == ("load.warehouse",)
        assert dag.tasks["marts.build"].depends_on == ("quality.warehouse",)


class TestExecution:
    def test_full_pipeline_succeeds_end_to_end(self, report):
        failed = [r.name for r in report.results if r.status != STATUS_SUCCESS]
        assert not failed, f"tasks that did not succeed: {failed}"
        assert report.ok

    def test_it_runs_without_the_network(self, report):
        """Every extract resolved from the raw-zone cache rather than
        re-downloading -- the sources point at 127.0.0.1:9, which refuses."""
        extracts = [r for r in report.results if r.name.startswith("extract.")]
        assert len(extracts) == 4
        for r in extracts:
            assert r.status == STATUS_SUCCESS
            assert r.value["status"] == "cached", \
                f"{r.name} went to the network instead of using its cache"

    def test_all_quality_gates_ran_and_passed(self, report):
        assert report.ok
        phases = {q["phase"] for q in report.quality}
        assert phases == {"staging", "warehouse"}
        assert not [q for q in report.quality if not q["passed"]]

    def test_rows_actually_loaded(self, report):
        assert report.loaded_rows == 13

    def test_artefacts_written(self, project, report):
        assert (project.warehouse_path).exists()
        assert (project.staging_dir / "stg_transactions.parquet").exists()
        assert list(project.marts_dir.glob("mart_*.parquet"))
        assert (project.reports_dir / "dashboard.html").exists()
        assert (project.reports_dir / "kpis.json").exists()
        assert list(project.reports_dir.glob("*.png"))

    def test_report_carries_stage_statistics(self, report):
        """Per-stage stats flow through to the report -- that is where the
        README's numbers come from, so it must not silently be empty."""
        assert set(report.stats) == {"transactions", "weather", "fx", "bank_rate"}
        assert report.stats["transactions"]["rows_out"] == 13


class TestFailurePropagation:
    def test_a_broken_gate_aborts_the_run(self, project):
        """A quality gate that runs but does not stop the pipeline is
        decoration -- assert the downstream load never happens."""
        project.quality["min_transaction_rows"] = 10_000_000
        rep = pipeline.run(project)

        by_name = {r.name: r for r in rep.results}
        assert by_name["quality.staging"].status == "failed"
        assert by_name["load.warehouse"].status == "skipped"
        assert not rep.ok

        # nothing downstream of the gate was built
        assert not list(project.marts_dir.glob("mart_*.parquet"))

    def test_failure_of_one_source_skips_only_its_branch(self, project, monkeypatch):
        """The other three sources must still report -- otherwise you learn
        about failures one run at a time."""
        from retailde import extract as extract_mod

        real = extract_mod.extract

        def sabotaged(src, *a, **k):
            if src.id == "boe_bank_rate":
                raise extract_mod.ExtractionError("sabotaged")
            return real(src, *a, **k)

        monkeypatch.setattr("retailde.extract.extract", sabotaged)
        rep = pipeline.run(project, start="extract.boe_bank_rate")

        by_name = {r.name: r for r in rep.results}
        assert by_name["extract.boe_bank_rate"].status == "failed"
        assert by_name["stage.bank_rate"].status == "skipped"


class TestSelectors:
    def test_dry_run_lists_everything_and_executes_nothing(self, project):
        rep = pipeline.run(project, dry_run=True)
        assert all(r.status == "planned" for r in rep.results)
        assert len(rep.results) == 13
        assert not project.warehouse_path.exists()

    def test_stop_before_the_warehouse_leaves_it_unbuilt(self, project):
        rep = pipeline.run(project, stop="quality.staging")
        names = [r.name for r in rep.results]
        assert names[-1] == "quality.staging"
        assert "load.warehouse" not in names
        assert not project.warehouse_path.exists()

    def test_start_from_load_skips_the_upstream_work(self, project):
        # build the upstream half first, then resume
        pipeline.run(project, stop="quality.staging")
        rep = pipeline.run(project, start="load.warehouse")
        names = [r.name for r in rep.results]
        assert names[0] == "load.warehouse"
        assert "extract.uci_online_retail" not in names
        assert rep.ok
        assert project.warehouse_path.exists()

    def test_unknown_selector_is_rejected_before_running(self, project):
        from retailde.orchestrator import DAGError
        with pytest.raises(DAGError, match="unknown start task"):
            pipeline.run(project, start="does_not_exist")
