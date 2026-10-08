"""Stage ECB reference exchange rates from the Frankfurter API.

Payload shape (verified against a live call)::

    {"base": "GBP", "start_date": "2009-12-01", "end_date": "2011-12-09",
     "rates": {"2009-12-01": {"USD": 1.6669, "EUR": 1.1305}, ...}}

Two things make this source less trivial than it looks:

1. ``rates`` is a **dict keyed by date**, unlike Open-Meteo's positional
   arrays -- so a different projection is needed for the same end result.
2. The ECB only publishes on **target-area working days**. Over 2009-12-01
   .. 2011-12-09 that is 523 dates against 529 weekdays -- 98.9% coverage on
   weekdays but only 70.8% on calendar days. Any coverage gate written
   against calendar days would fail forever, so the gate measures weekdays.
"""

from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any

import pandas as pd

from ..config import AppConfig

RAW_NAME = "ecb_fx_gbp.json"
STAGING_NAME = "stg_fx.parquet"


def weekday_count(start: pd.Timestamp, end: pd.Timestamp) -> int:
    """Number of Mon-Fri days in [start, end]."""
    if pd.isna(start) or pd.isna(end) or end < start:
        return 0
    days = (end.normalize() - start.normalize()).days + 1
    full_weeks, rem = divmod(days, 7)
    total = full_weeks * 5
    cursor = start.normalize()
    for i in range(rem):
        if (cursor + timedelta(days=i)).weekday() < 5:
            total += 1
    return total


def parse(payload: dict[str, Any]) -> pd.DataFrame:
    if "rates" not in payload or not isinstance(payload["rates"], dict):
        raise ValueError(f"frankfurter payload has no 'rates' mapping (keys={list(payload)})")

    rows = []
    for date_str, quotes in payload["rates"].items():
        if not isinstance(quotes, dict):
            continue
        rows.append({
            "date": pd.to_datetime(date_str, errors="coerce"),
            "gbp_to_usd": pd.to_numeric(quotes.get("USD"), errors="coerce"),
            "gbp_to_eur": pd.to_numeric(quotes.get("EUR"), errors="coerce"),
        })

    df = pd.DataFrame(rows, columns=["date", "gbp_to_usd", "gbp_to_eur"])
    df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="first")
    return df.sort_values("date").reset_index(drop=True)


def stage(cfg: AppConfig, force: bool = False) -> dict[str, Any]:
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    raw = cfg.raw_dir / RAW_NAME
    if not raw.exists():
        raise FileNotFoundError(f"{raw} not found -- run `retailde extract` first.")

    payload = json.loads(raw.read_text(encoding="utf-8"))
    df = parse(payload)

    out = cfg.staging_dir / STAGING_NAME
    df.to_parquet(out, index=False)

    n_weekdays = weekday_count(df["date"].min(), df["date"].max()) if len(df) else 0
    stats: dict[str, Any] = {
        "table": STAGING_NAME,
        "rows_out": int(len(df)),
        "date_min": df["date"].min().date().isoformat() if len(df) else None,
        "date_max": df["date"].max().date().isoformat() if len(df) else None,
        "declared_start": payload.get("start_date"),
        "declared_end": payload.get("end_date"),
        "base_currency": payload.get("base"),
        "weekdays_in_window": n_weekdays,
        "weekday_coverage": round(len(df) / n_weekdays, 4) if n_weekdays else 0.0,
        "mean_gbp_to_usd": round(float(df["gbp_to_usd"].mean()), 4),
        "mean_gbp_to_eur": round(float(df["gbp_to_eur"].mean()), 4),
        "elapsed": round(time.perf_counter() - t0, 3),
        "bytes": out.stat().st_size,
    }
    return stats


def load(cfg: AppConfig) -> pd.DataFrame:
    return pd.read_parquet(cfg.staging_dir / STAGING_NAME)
