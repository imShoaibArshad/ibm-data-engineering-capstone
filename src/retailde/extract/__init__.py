"""Extraction adapters: bulk_file, rest_api, html_page.

Design notes
------------
* **Parsing is separated from fetching.** Adapters only land bytes in the raw
  zone and record provenance; turning those bytes into dataframes lives in
  ``retailde.transform``. That split is what lets the whole test-suite run
  offline with fixture bytes instead of live HTTP.
* **Everything is idempotent.** Re-running extract with a warm cache writes
  nothing new, so the pipeline can be retried safely.
* **``offline=True`` never touches the network.** CI, tests and repeat demos
  use the cached artefacts; a missing cache is a hard error, not a download.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import AppConfig, Source
from ..manifest import ExtractRecord, Manifest, sha256_bytes, utcnow_iso

USER_AGENT = "ibm-de-capstone/1.0 (+https://github.com/imShoaibArshad)"


class ExtractionError(RuntimeError):
    """Raised when a source cannot be landed, after all retries."""


@dataclass
class FetchResult:
    body: bytes
    status: int
    content_type: str
    url: str
    attempts: int
    elapsed: float


def fetch_bytes(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: int = 45,
    retries: int = 3,
) -> FetchResult:
    """GET with exponential backoff. Raises ExtractionError on exhaustion.

    Backoff is ``1s, 2s, 4s`` -- short enough to keep a demo pleasant, long
    enough to ride out a transient 5xx or a blipped connection.
    """
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}{'&' if '?' in url else '?'}{query}"

    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    last_err: BaseException | None = None
    t_start = time.perf_counter()

    for attempt in range(1, max(retries, 1) + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                return FetchResult(
                    body=body,
                    status=getattr(resp, "status", 200),
                    content_type=resp.headers.get("Content-Type", ""),
                    url=resp.geturl(),
                    attempts=attempt,
                    elapsed=time.perf_counter() - t_start,
                )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            # 4xx (except 429) will not fix themselves -- fail immediately.
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500 and exc.code != 429:
                raise ExtractionError(f"HTTP {exc.code} for {url}: {exc.reason}") from exc
            last_err = exc
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))

    raise ExtractionError(
        f"fetch failed after {retries} attempt(s): {url} -- {last_err}"
    ) from last_err


# ---------------------------------------------------------------------------
# URL / parameter templating
# ---------------------------------------------------------------------------

def _render(value: Any, ctx: dict[str, str]) -> Any:
    """Recursively substitute ``{start_date}`` style placeholders."""
    if isinstance(value, str):
        out = value
        for k, v in ctx.items():
            out = out.replace("{" + k + "}", str(v))
        return out
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(v, ctx) for v in value]
    return value


def _source_context(source: Source) -> dict[str, str]:
    opts = source.options
    return {
        "start_date": str(opts.get("start_date", "")),
        "end_date": str(opts.get("end_date", "")),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def extract(
    source: Source,
    cfg: AppConfig,
    manifest: Manifest,
    *,
    force: bool = False,
    offline: bool = False,
) -> ExtractRecord:
    """Land one source in the raw zone and record provenance."""
    cfg.ensure_dirs()
    dest = cfg.raw_dir / source.cache_name
    ctx = _source_context(source)

    # -- warm cache --------------------------------------------------------
    if not force and dest.exists() and dest.stat().st_size > 0:
        rec = ExtractRecord(
            source_id=source.id,
            source_name=source.name,
            adapter=source.adapter,
            url=source.url,
            started_at=utcnow_iso(),
            elapsed_seconds=0.0,
            bytes=dest.stat().st_size,
            sha256=_hash_path(dest),
            status="cached",
            cached_path=str(dest.relative_to(cfg.root)),
            detail="served from raw-zone cache",
        )
        manifest.record(rec)
        return rec

    if offline:
        raise ExtractionError(
            f"offline mode and no cached artefact for {source.id!r} "
            f"(expected {dest}). Run once online to populate the raw zone."
        )

    # -- fetch -------------------------------------------------------------
    params = _render(source.options.get("params"), ctx)
    url = _render(source.url, ctx)

    result = fetch_bytes(
        url,
        params=params,
        timeout=source.timeout_seconds,
        retries=source.retries,
    )

    detail = f"http {result.status} in {result.elapsed:.1f}s ({result.attempts} attempt(s))"

    if source.adapter == "bulk_file":
        detail += _land_bulk_file(source, cfg, result.body, dest)
    else:
        _write_atomic(dest, result.body)

    rec = ExtractRecord(
        source_id=source.id,
        source_name=source.name,
        adapter=source.adapter,
        url=result.url,
        started_at=utcnow_iso(),
        elapsed_seconds=round(result.elapsed, 3),
        bytes=len(result.body),
        sha256=sha256_bytes(result.body),
        status="ok",
        cached_path=str(dest.relative_to(cfg.root)),
        detail=detail,
        attempts=result.attempts,
    )
    manifest.record(rec)
    return rec


def _land_bulk_file(source: Source, cfg: AppConfig, body: bytes, dest: Path) -> str:
    """Write the archive, then unpack ``extract_member`` beside it.

    Unpacking here (rather than in transform) keeps the raw zone honest: it
    holds exactly the bytes the vendor published plus their direct expansion.
    """
    _write_atomic(dest, body)
    member = source.options.get("extract_member")
    if not member:
        return " [archive]"

    try:
        with zipfile.ZipFile(dest) as zf:
            names = zf.namelist()
            if member not in names:
                return f" [archive; member {member!r} absent, have {names}]"
            target = dest.parent / member
            if not (target.exists() and target.stat().st_size > 0):
                zf.extractall(dest.parent)
            size = target.stat().st_size if target.exists() else 0
        return f" [archive; extracted {member} = {size:,} bytes]"
    except zipfile.BadZipFile as exc:
        return f" [archive; INVALID ZIP: {exc}]"


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def _hash_path(path: Path) -> str:
    from ..manifest import sha256_file
    return sha256_file(path)


# ---------------------------------------------------------------------------
# Introspection helpers used by the CLI
# ---------------------------------------------------------------------------

def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
