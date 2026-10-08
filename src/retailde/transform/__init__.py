"""Staging: raw artefacts -> typed, deduplicated Parquet.

Responsibility split
--------------------
``retailde.extract``   lands vendor bytes in ``data/raw`` and records provenance.
``retailde.transform`` turns those bytes into analysis-ready tables in
``data/staging``. Nothing here touches the network, so every transform is
deterministic and offline-testable.

Each module exposes ``stage(cfg, force=False) -> dict`` returning a stats
block (row counts, rejection reasons, coverage). Those numbers are what the
quality gates assert against and what the README reports -- they come from a
real run, not from prose.
"""

from __future__ import annotations

import time
from typing import Any, Callable

from ..config import AppConfig
from . import bank_rate, fx, transactions, weather

# Order matters: transactions is the fact spine everything else hangs off.
STAGE_ORDER: list[tuple[str, Callable[..., dict]]] = [
    ("uci_online_retail", transactions.stage),
    ("open_meteo_london", weather.stage),
    ("ecb_fx_gbp", fx.stage),
    ("boe_bank_rate", bank_rate.stage),
]


def stage_all(cfg: AppConfig, *, force: bool = False) -> list[dict[str, Any]]:
    """Run every transform in dependency order, collecting stats.

    Failures propagate -- a staging step that cannot run must stop the
    pipeline rather than silently produce an empty table.
    """
    cfg.ensure_dirs()
    results: list[dict[str, Any]] = []
    for source_id, fn in STAGE_ORDER:
        try:
            cfg.source(source_id)  # raises KeyError if removed from catalogue
        except KeyError:
            continue
        t0 = time.perf_counter()
        stats = fn(cfg, force=force)
        stats.setdefault("source_id", source_id)
        stats.setdefault("elapsed", round(time.perf_counter() - t0, 3))
        results.append(stats)
    return results


__all__ = ["STAGE_ORDER", "stage_all", "transactions", "weather", "fx", "bank_rate"]
