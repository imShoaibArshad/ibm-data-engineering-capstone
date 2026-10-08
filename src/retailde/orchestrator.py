"""A small DAG runner: topological ordering, retries with backoff, run log.

Deliberately not Airflow. Airflow needs a scheduler daemon, a metadata
database and a web server -- none of which run on a laptop with no cloud
account, which is a hard requirement of this project. What actually matters
about a pipeline orchestrator is small and testable, so it is implemented
directly:

* **Dependencies are a graph**, validated before anything runs: unknown task
  names and cycles are reported as errors rather than discovered as hangs.
* **A failure skips only what depends on it.** Tasks downstream of the failed
  task are reported as skipped rather than executed, while independent
  branches still run to completion. That distinction matters here: if the
  BoE scrape fails you still want to know whether the other three sources
  extracted, instead of finding out one failure at a time.
* **Retries use exponential backoff** (1s, 2s, 4s ...) because the transient
  failures that actually occur are rate limits and dropped connections.
* **Idempotency is the task's job.** The runner will re-run a task; each task
  must therefore be safe to repeat (cached artefacts, upserts, ``OR
  REPLACE``), which is why every stage here is.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable

import networkx as nx

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"


class DAGError(RuntimeError):
    """Raised when the DAG itself is invalid (cycle, unknown dependency)."""


class TaskError(RuntimeError):
    """Raised when a task fails after exhausting its retries."""

    def __init__(self, name: str, attempts: int, cause: BaseException):
        self.task = name
        self.attempts = attempts
        self.cause = cause
        super().__init__(f"task {name!r} failed after {attempts} attempt(s): {cause}")


@dataclass
class Task:
    name: str
    fn: Callable[[], Any]
    depends_on: tuple[str, ...] = ()
    retries: int = 2
    backoff: float = 1.0
    description: str = ""


@dataclass
class TaskResult:
    name: str
    status: str
    attempts: int
    elapsed: float
    detail: str = ""
    value: Any = None
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "status": self.status,
                "attempts": self.attempts, "elapsed": round(self.elapsed, 3),
                "detail": self.detail, "error": self.error}


@dataclass
class DAG:
    """Directed acyclic pipeline of named tasks."""

    tasks: dict[str, Task] = field(default_factory=dict)

    def add(self, name: str, fn: Callable[[], Any], *,
            depends_on: tuple[str, ...] = (), retries: int = 2,
            backoff: float = 1.0, description: str = "") -> "DAG":
        if name in self.tasks:
            raise DAGError(f"duplicate task name {name!r}")
        self.tasks[name] = Task(name=name, fn=fn, depends_on=tuple(depends_on),
                                retries=retries, backoff=backoff,
                                description=description)
        return self

    # -- graph --------------------------------------------------------------
    def graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for name, task in self.tasks.items():
            g.add_node(name)
        for name, task in self.tasks.items():
            for dep in task.depends_on:
                if dep not in self.tasks:
                    raise DAGError(
                        f"task {name!r} depends on unknown task {dep!r} "
                        f"(known: {sorted(self.tasks)})"
                    )
                g.add_edge(dep, name)   # edge direction = "must run before"
        return g

    def order(self) -> list[str]:
        """Topological order; raises DAGError on a cycle."""
        g = self.graph()
        if not nx.is_directed_acyclic_graph(g):
            cycle = nx.find_cycle(g)
            raise DAGError(f"dependency cycle: {' -> '.join(str(n) for n, _ in cycle)}")
        return list(nx.topological_sort(g))

    def plan(self, *, start: str | None = None, stop: str | None = None) -> list[str]:
        """Ordered task names, optionally bounded by ``start``/``stop``.

        ``start`` resumes *at* a task: its upstream is excluded, so nothing
        already done runs again. ``stop`` cuts the run short: only that task
        and everything it needs are planned. The two are mirror images --
        descendants versus ancestors -- which is easy to get backwards.
        """
        order = self.order()
        if start is not None and start not in self.tasks:
            raise DAGError(f"unknown start task {start!r}")
        if stop is not None and stop not in self.tasks:
            raise DAGError(f"unknown stop task {stop!r}")

        if start is not None:
            # resume *from* here: this task and what follows it
            wanted = self._descendants(start) | {start}
            order = [t for t in order if t in wanted]
        if stop is not None:
            # run *up to* here: this task and everything it needs
            needed = self._ancestors(stop) | {stop}
            order = [t for t in order if t in needed]
        return order

    def _ancestors(self, name: str) -> set[str]:
        return set(nx.ancestors(self.graph(), name))

    def _descendants(self, name: str) -> set[str]:
        return set(nx.descendants(self.graph(), name))

    # -- execution ----------------------------------------------------------
    def run(self, *, start: str | None = None, stop: str | None = None,
            dry_run: bool = False, on_event: Callable[[str, str], None] | None = None,
            sleep: Callable[[float], None] = time.sleep) -> list[TaskResult]:
        order = self.plan(start=start, stop=stop)

        if dry_run:
            return [TaskResult(n, "planned", 0, 0.0, self.tasks[n].description)
                    for n in order]

        results: dict[str, TaskResult] = {}
        # Tasks outside this slice (cut out by start/stop) are assumed to
        # have already been done upstream -- otherwise `start=` could never
        # resume, since a resumed task's predecessors are deliberately absent.
        planned = set(order)

        for name in order:
            task = self.tasks[name]

            # Skip only when a dependency *in this run* did not succeed.
            unmet = [d for d in task.depends_on
                     if d in planned
                     and (d not in results or results[d].status != STATUS_SUCCESS)]
            if unmet:
                results[name] = TaskResult(
                    name, STATUS_SKIPPED, 0, 0.0,
                    f"skipped: dependency not successful ({', '.join(unmet)})",
                )
                _notify(on_event, name, STATUS_SKIPPED)
                continue

            _notify(on_event, name, "started")
            t0 = time.perf_counter()
            attempts = 0
            last_error: BaseException | None = None

            for attempt in range(1, max(task.retries, 1) + 1):
                attempts = attempt
                try:
                    value = task.fn()
                    elapsed = time.perf_counter() - t0
                    res = TaskResult(name, STATUS_SUCCESS, attempts, elapsed,
                                     task.description, value=value)
                    results[name] = res
                    _notify(on_event, name, STATUS_SUCCESS)
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt < task.retries:
                        delay = task.backoff * (2 ** (attempt - 1))
                        _notify(on_event, name,
                                f"attempt {attempt} failed, retrying in {delay:g}s")
                        sleep(delay)
            else:
                elapsed = time.perf_counter() - t0
                results[name] = TaskResult(
                    name, STATUS_FAILED, attempts, elapsed,
                    task.description,
                    error=f"{type(last_error).__name__}: {last_error}",
                )
                _notify(on_event, name, STATUS_FAILED)
                # No global abort: the dependency check below already stops
                # everything downstream of this task, while an unrelated
                # branch still gets its chance to run.

        return [results[n] for n in order]


def _notify(cb: Callable[[str, str], None] | None, name: str, state: str) -> None:
    if cb is not None:
        cb(name, state)


def summarise(results: list[TaskResult]) -> str:
    width = max((len(r.name) for r in results), default=10)
    lines = []
    for r in results:
        mark = {"success": "ok  ", "failed": "FAIL",
                "skipped": "skip", "planned": "plan"}[r.status]
        detail = f"  {r.detail}" if r.detail else ""
        err = f"  {r.error}" if r.error else ""
        lines.append(f"  [{mark}] {r.name:<{width}} {r.elapsed:6.2f}s "
                     f"({r.attempts} attempt(s)){detail}{err}")
    ok = sum(1 for r in results if r.status == STATUS_SUCCESS)
    bad = sum(1 for r in results if r.status == STATUS_FAILED)
    skip = sum(1 for r in results if r.status == STATUS_SKIPPED)
    lines.append(f"\n  {ok} succeeded, {bad} failed, {skip} skipped")
    return "\n".join(lines)
