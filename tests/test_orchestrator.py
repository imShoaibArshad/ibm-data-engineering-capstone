"""DAG runner tests: ordering, cycles, retries, and failure propagation."""

from __future__ import annotations

import pytest

from retailde.orchestrator import (DAG, DAGError, STATUS_FAILED,
                                   STATUS_SKIPPED, STATUS_SUCCESS, summarise)


def _echo(recorder, name):
    def fn():
        recorder.append(name)
        return name
    return fn


def _boom(recorder, name, exc=RuntimeError("boom")):
    def fn():
        recorder.append(name)
        raise exc
    return fn


class TestStructure:
    def test_topological_order_respects_dependencies(self):
        dag = DAG()
        dag.add("c", lambda: None, depends_on=("b",))
        dag.add("b", lambda: None, depends_on=("a",))
        dag.add("a", lambda: None)
        assert dag.order() == ["a", "b", "c"]

    def test_independent_tasks_can_interleave(self):
        dag = DAG()
        dag.add("root", lambda: None)
        dag.add("left", lambda: None, depends_on=("root",))
        dag.add("right", lambda: None, depends_on=("root",))
        order = dag.order()
        assert order[0] == "root"
        assert set(order[1:]) == {"left", "right"}

    def test_unknown_dependency_is_an_error_not_a_hang(self):
        dag = DAG()
        dag.add("a", lambda: None, depends_on=("ghost",))
        with pytest.raises(DAGError, match="unknown task 'ghost'"):
            dag.order()

    def test_cycle_is_detected(self):
        dag = DAG()
        dag.add("a", lambda: None, depends_on=("c",))
        dag.add("b", lambda: None, depends_on=("a",))
        dag.add("c", lambda: None, depends_on=("b",))
        with pytest.raises(DAGError, match="cycle"):
            dag.order()

    def test_self_dependency_is_a_cycle(self):
        dag = DAG()
        dag.add("a", lambda: None, depends_on=("a",))
        with pytest.raises(DAGError, match="cycle"):
            dag.order()

    def test_duplicate_task_name_rejected(self):
        dag = DAG()
        dag.add("a", lambda: None)
        with pytest.raises(DAGError, match="duplicate task"):
            dag.add("a", lambda: None)

    def test_plan_start_skips_upstream(self):
        dag = DAG()
        dag.add("a", lambda: None)
        dag.add("b", lambda: None, depends_on=("a",))
        dag.add("c", lambda: None, depends_on=("b",))
        assert dag.plan(start="b") == ["b", "c"]

    def test_plan_stop_cuts_the_run_short(self):
        dag = DAG()
        dag.add("a", lambda: None)
        dag.add("b", lambda: None, depends_on=("a",))
        dag.add("c", lambda: None, depends_on=("b",))
        assert dag.plan(stop="b") == ["a", "b"]

    def test_plan_rejects_unknown_bounds(self):
        dag = DAG()
        dag.add("a", lambda: None)
        with pytest.raises(DAGError, match="unknown start task"):
            dag.plan(start="nope")


class TestExecution:
    def test_runs_in_dependency_order(self):
        seen = []
        dag = DAG()
        dag.add("a", _echo(seen, "a"))
        dag.add("b", _echo(seen, "b"), depends_on=("a",))
        dag.add("c", _echo(seen, "c"), depends_on=("a",))
        results = dag.run()
        assert seen == ["a", "b", "c"], "b and c must both follow a"
        assert all(r.status == STATUS_SUCCESS for r in results)

    def test_failure_skips_downstream_and_never_runs_it(self):
        seen = []
        dag = DAG()
        dag.add("a", _boom(seen, "a"), retries=1)
        dag.add("b", _echo(seen, "b"), depends_on=("a",))
        dag.add("root", _echo(seen, "root"))
        dag.add("c", _echo(seen, "c"), depends_on=("root",))

        results = {r.name: r for r in dag.run()}

        assert results["a"].status == STATUS_FAILED
        assert results["b"].status == STATUS_SKIPPED
        assert "b" not in seen, "a task with a failed dependency must not execute"
        # an unrelated branch still completes
        assert results["c"].status == STATUS_SUCCESS
        assert "c" in seen

    def test_retries_until_success(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ConnectionError("transient")
            return "ok"

        dag = DAG()
        dag.add("flaky", flaky, retries=3, backoff=0.0)
        results = dag.run(sleep=lambda _s: None)
        assert results[0].status == STATUS_SUCCESS
        assert results[0].attempts == 3

    def test_gives_up_after_exhausting_retries(self):
        calls = {"n": 0}

        def always_fails():
            calls["n"] += 1
            raise ValueError("nope")

        dag = DAG()
        dag.add("x", always_fails, retries=3, backoff=0.0)
        results = dag.run(sleep=lambda _s: None)
        assert results[0].status == STATUS_FAILED
        assert results[0].attempts == 3
        assert calls["n"] == 3
        assert "ValueError: nope" in results[0].error

    def test_backoff_is_exponential(self):
        delays = []
        calls = {"n": 0}

        def fails_twice():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ConnectionError()
            return "ok"

        dag = DAG()
        dag.add("x", fails_twice, retries=3, backoff=1.0)
        dag.run(sleep=delays.append)
        assert delays == [1.0, 2.0], f"expected exponential backoff, got {delays}"

    def test_downstream_of_a_failure_is_not_retried(self):
        calls = {"a": 0, "b": 0}

        def fail_a():
            calls["a"] += 1
            raise RuntimeError()

        def b():
            calls["b"] += 1

        dag = DAG()
        dag.add("a", fail_a, retries=2, backoff=0.0)
        dag.add("b", b, depends_on=("a",), retries=3, backoff=0.0)
        dag.run(sleep=lambda _s: None)
        assert calls == {"a": 2, "b": 0}

    def test_dry_run_executes_nothing(self):
        seen = []
        dag = DAG()
        dag.add("a", _echo(seen, "a"))
        results = dag.run(dry_run=True)
        assert seen == []
        assert results[0].status == "planned"

    def test_start_selector_resumes_mid_pipeline(self):
        seen = []
        dag = DAG()
        dag.add("a", _echo(seen, "a"))
        dag.add("b", _echo(seen, "b"), depends_on=("a",))
        dag.run(start="b")
        assert seen == ["b"]

    def test_result_values_are_carried_through(self):
        dag = DAG()
        dag.add("produce", lambda: {"rows": 42})
        results = dag.run()
        assert results[0].value == {"rows": 42}


class TestReporting:
    def test_summarise_counts_statuses(self):
        seen = []
        dag = DAG()
        dag.add("ok", _echo(seen, "ok"))
        dag.add("bad", _boom(seen, "bad"), retries=1)
        dag.add("down", _echo(seen, "down"), depends_on=("bad",))
        text = summarise(dag.run())
        # 'ok' runs, 'bad' exhausts its retry, 'down' is skipped because its
        # only dependency failed -- three tasks, three outcomes.
        assert "1 succeeded, 1 failed, 1 skipped" in text
        assert "RuntimeError: boom" in text, "the error must surface in the summary"

    def test_event_callback_observes_lifecycle(self):
        events = []
        dag = DAG()
        dag.add("a", lambda: None)
        dag.run(on_event=lambda name, state: events.append((name, state)))
        assert ("a", "started") in events
        assert ("a", "success") in events
