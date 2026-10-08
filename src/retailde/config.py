"""Configuration loading: YAML source catalogue -> typed objects + resolved paths.

Everything downstream takes an :class:`AppConfig`; nothing re-reads the YAML.
That keeps one source of truth and makes the pipeline testable by swapping in
a config pointed at a temp directory.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Project root = three levels up from src/retailde/config.py
DEFAULT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = DEFAULT_ROOT / "config" / "sources.yaml"

ADAPTERS = ("bulk_file", "rest_api", "html_page")


@dataclass(frozen=True)
class Source:
    """One row of the source catalogue."""

    id: str
    name: str
    adapter: str
    url: str
    enabled: bool = True
    cache_as: str | None = None
    timeout_seconds: int = 45
    retries: int = 3
    description: str = ""
    # adapter-specific extras, kept loose on purpose
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def cache_name(self) -> str:
        """Filename this source lands under in the raw zone."""
        return self.cache_as or f"{self.id}.dat"


@dataclass
class AppConfig:
    root: Path
    sources: list[Source]
    quality: dict[str, Any]
    raw_dir: Path
    staging_dir: Path
    warehouse_path: Path
    marts_dir: Path
    reports_dir: Path
    logs_dir: Path

    # -- construction --------------------------------------------------------
    @classmethod
    def load(
        cls,
        root: str | Path | None = None,
        config_path: str | Path | None = None,
    ) -> "AppConfig":
        root = Path(root) if root else DEFAULT_ROOT
        cfg_path = Path(config_path) if config_path else root / "config" / "sources.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"config not found: {cfg_path}")

        with open(cfg_path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        if not isinstance(raw, dict) or "sources" not in raw:
            raise ValueError(f"{cfg_path}: expected a mapping with a 'sources' key")

        sources = [_parse_source(entry) for entry in raw["sources"]]

        ids = [s.id for s in sources]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate source ids in {cfg_path}: {sorted(dupes)}")

        warehouse_rel = raw.get("warehouse", "data/warehouse/retail.duckdb")

        return cls(
            root=root,
            sources=sources,
            quality=dict(raw.get("quality") or {}),
            raw_dir=root / "data" / "raw",
            staging_dir=root / "data" / "staging",
            warehouse_path=root / warehouse_rel,
            marts_dir=root / "data" / "marts",
            reports_dir=root / "data" / "reports",
            logs_dir=root / "data" / "logs",
        )

    # -- helpers -------------------------------------------------------------
    @property
    def enabled_sources(self) -> list[Source]:
        return [s for s in self.sources if s.enabled]

    def source(self, source_id: str) -> Source:
        for s in self.sources:
            if s.id == source_id:
                return s
        raise KeyError(f"unknown source id: {source_id!r} (have {[x.id for x in self.sources]})")

    def ensure_dirs(self) -> None:
        for d in (self.raw_dir, self.staging_dir, self.marts_dir,
                  self.reports_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)


def _parse_source(entry: dict[str, Any]) -> Source:
    if "id" not in entry or "adapter" not in entry:
        raise ValueError(f"source entry missing 'id' or 'adapter': {entry!r}")
    adapter = entry["adapter"]
    if adapter not in ADAPTERS:
        raise ValueError(
            f"source {entry['id']!r}: unknown adapter {adapter!r}; "
            f"expected one of {ADAPTERS}"
        )
    if "url" not in entry:
        raise ValueError(f"source {entry['id']!r}: missing 'url'")

    known = {
        "id", "name", "adapter", "url", "enabled", "cache_as",
        "timeout_seconds", "retries", "description",
    }
    options = {k: v for k, v in entry.items() if k not in known}

    return Source(
        id=str(entry["id"]),
        name=str(entry.get("name", entry["id"])),
        adapter=adapter,
        url=str(entry["url"]),
        enabled=bool(entry.get("enabled", True)),
        cache_as=entry.get("cache_as"),
        timeout_seconds=int(entry.get("timeout_seconds", 45)),
        retries=int(entry.get("retries", 3)),
        description=str(entry.get("description", "")).strip(),
        options=options,
    )


def project_root() -> Path:
    """Repository root, honouring an override for tests."""
    return Path(os.environ.get("RETAILDE_ROOT", DEFAULT_ROOT))
