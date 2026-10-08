"""Stage the Open-Meteo historical weather feed.

Payload shape (verified against a live call)::

    {"daily": {"time": ["2009-12-01", ...],
               "temperature_2m_max": [5.8, ...],
               "temperature_2m_min": [-0.9, ...],
               "precipitation_sum": [0.3, ...]}}

The three arrays are positionally aligned, not keyed -- the classic
``zip(name, tmax, tmin, precip)`` layout that every weather API seems to
converge on. They are transposed into one row per day here so the join to
``dim_date`` is a plain equality on a date rather than an index lookup.
"""

from __future__ import annotations

import json
import time
from typing import Any

import pandas as pd

from ..config import AppConfig

RAW_NAME = "open_meteo_london.json"
STAGING_NAME = "stg_weather.parquet"


def parse(payload: dict[str, Any]) -> pd.DataFrame:
    """``daily`` block -> tidy frame, one row per date."""
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError(f"open-meteo payload has no 'daily' block (keys={list(payload)})")

    required = ("time", "temperature_2m_max", "temperature_2m_min", "precipitation_sum")
    missing = [k for k in required if k not in daily]
    if missing:
        raise ValueError(f"open-meteo 'daily' missing {missing}")

    lengths = {k: len(daily[k]) for k in required}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"open-meteo daily arrays are not aligned: {lengths}")

    df = pd.DataFrame({
        "date": pd.to_datetime(daily["time"], errors="coerce"),
        "tmax_c": pd.to_numeric(daily["temperature_2m_max"], errors="coerce"),
        "tmin_c": pd.to_numeric(daily["temperature_2m_min"], errors="coerce"),
        "precip_mm": pd.to_numeric(daily["precipitation_sum"], errors="coerce"),
    })

    # A date arriving twice would fan out the fact table on join -- reject it.
    df = df.drop_duplicates(subset=["date"], keep="first")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["is_wet"] = df["precip_mm"].fillna(0).gt(0.5)  # >0.5mm = a wet day
    return df


def stage(cfg: AppConfig, force: bool = False) -> dict[str, Any]:
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    raw = cfg.raw_dir / RAW_NAME
    if not raw.exists():
        raise FileNotFoundError(f"{raw} not found -- run `retailde extract` first.")

    df = parse(json.loads(raw.read_text(encoding="utf-8")))

    out = cfg.staging_dir / STAGING_NAME
    df.to_parquet(out, index=False)

    stats: dict[str, Any] = {
        "table": STAGING_NAME,
        "rows_out": int(len(df)),
        "date_min": df["date"].min().date().isoformat() if len(df) else None,
        "date_max": df["date"].max().date().isoformat() if len(df) else None,
        "wet_days": int(df["is_wet"].sum()),
        "mean_tmax_c": round(float(df["tmax_c"].mean()), 2),
        "total_precip_mm": round(float(df["precip_mm"].sum()), 1),
        "elapsed": round(time.perf_counter() - t0, 3),
        "bytes": out.stat().st_size,
    }
    return stats


def load(cfg: AppConfig) -> pd.DataFrame:
    return pd.read_parquet(cfg.staging_dir / STAGING_NAME)
