"""Extraction provenance.

Every extract writes a row into ``data/raw/_manifest.json``: what was fetched,
from where, when, how long it took, how many bytes, and the SHA-256 of the
bytes. That is the difference between "a file appeared" and a pipeline you can
audit -- if a number in the dashboard ever looks wrong, the manifest tells you
exactly which artefact produced it and whether it changed.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

MANIFEST_NAME = "_manifest.json"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    """Streaming hash -- the raw zone holds a 45 MB archive."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class ExtractRecord:
    source_id: str
    source_name: str
    adapter: str
    url: str
    started_at: str
    elapsed_seconds: float
    bytes: int
    sha256: str
    status: str  # ok | cached | error
    cached_path: str
    detail: str = ""
    attempts: int = 1
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        """JSON-safe summary -- this is what the task result carries."""
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "adapter": self.adapter,
            "status": self.status,
            "url": self.url,
            "started_at": self.started_at,
            "elapsed_seconds": self.elapsed_seconds,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "cached_path": self.cached_path,
            "detail": self.detail,
            "attempts": self.attempts,
        }


class Manifest:
    """Append-only JSON log of extractions, keyed by source id."""

    def __init__(self, raw_dir: Path):
        self.raw_dir = Path(raw_dir)
        self.path = self.raw_dir / MANIFEST_NAME
        self._entries: dict[str, dict] = {}
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    self._entries = data.get("records", data)
            except (json.JSONDecodeError, OSError):
                # A corrupt manifest must never take the pipeline down.
                self._entries = {}

    def record(self, rec: ExtractRecord) -> None:
        self._entries[rec.source_id] = asdict(rec)
        self.flush()

    def get(self, source_id: str) -> dict | None:
        return self._entries.get(source_id)

    def prune(self, valid_ids: set[str]) -> list[str]:
        """Drop records for sources no longer in the catalogue.

        Sources get retired (gov.uk bank holidays was, after it turned out
        not to overlap the transaction window). Leaving their provenance
        behind implies data that no longer exists.
        """
        stale = [k for k in list(self._entries) if k not in valid_ids]
        for key in stale:
            del self._entries[key]
        if stale:
            self.flush()
        return stale

    def flush(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                {"updated_at": utcnow_iso(), "records": self._entries},
                fh,
                indent=2,
                sort_keys=True,
            )
        tmp.replace(self.path)  # atomic-ish: readers never see a half file

    def summary_rows(self) -> list[dict]:
        rows = []
        for sid, e in sorted(self._entries.items()):
            rows.append({
                "source": sid,
                "status": e.get("status", "?"),
                "bytes": e.get("bytes", 0),
                "sha256": (e.get("sha256") or "")[:12],
                "elapsed_s": e.get("elapsed_seconds", 0),
                "fetched_at": e.get("started_at", "?"),
            })
        return rows


def timed(fn, *args, **kwargs):
    """Run ``fn`` and return ``(result, elapsed_seconds)``."""
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - t0
