"""Extraction tests: caching, offline mode, retries, and provenance.

Nothing here reaches the network. Cache behaviour is exercised against
artefacts already on disk, and the retry path points at 127.0.0.1 on a closed
port, which fails locally and instantly rather than depending on the internet.
"""

from __future__ import annotations

import json

import pytest

from retailde.extract import ExtractionError, extract, fetch_bytes
from retailde.manifest import Manifest


UNREACHABLE = "http://127.0.0.1:9/nothing-here"


class TestCaching:
    def test_warm_cache_serves_without_network(self, project, monkeypatch):
        """A cached artefact must never trigger a fetch -- proven by making
        any fetch attempt raise."""
        src = project.source("ecb_fx_gbp")
        (project.raw_dir / src.cache_name).write_text('{"rates": {}}', encoding="utf-8")

        def explode(*_a, **_k):
            raise AssertionError("network was touched despite a warm cache")
        monkeypatch.setattr("retailde.extract.fetch_bytes", explode)

        rec = extract(src, project, Manifest(project.raw_dir))
        assert rec.status == "cached"
        assert rec.bytes > 0
        assert len(rec.sha256) == 64

    def test_cold_cache_fetches_and_records(self, project, monkeypatch):
        """With no cache, extract must actually go and fetch -- proven by
        substituting a fake transport so the assertion is deterministic
        instead of depending on whether a local port happens to refuse."""
        src = project.source("ecb_fx_gbp")
        path = project.raw_dir / src.cache_name
        if path.exists():
            path.unlink()

        body = b'{"base": "GBP", "rates": {"2010-12-02": {"USD": 1.55}}}'

        class Fake:
            def __init__(self):
                self.body = body
                self.status = 200
                self.content_type = "application/json"
                self.url = src.url
                self.attempts = 1
                self.elapsed = 0.01

        monkeypatch.setattr("retailde.extract.fetch_bytes", lambda *a, **k: Fake())

        rec = extract(src, project, Manifest(project.raw_dir))
        assert rec.status == "ok", "a cold cache must report a real fetch"
        assert rec.bytes == len(body)
        assert path.read_bytes() == body, "the artefact must land in the raw zone"
        assert rec.cached_path.endswith(src.cache_name)

    def test_offline_without_cache_is_a_clear_error(self, project):
        src = project.source("ecb_fx_gbp")
        path = project.raw_dir / src.cache_name
        if path.exists():
            path.unlink()
        with pytest.raises(ExtractionError, match="offline mode"):
            extract(src, project, Manifest(project.raw_dir), offline=True)

    def test_offline_with_cache_succeeds(self, project):
        src = project.source("ecb_fx_gbp")
        (project.raw_dir / src.cache_name).write_text('{"rates": {}}', encoding="utf-8")
        rec = extract(src, project, Manifest(project.raw_dir), offline=True)
        assert rec.status == "cached"

    def test_force_refetches(self, project, monkeypatch):
        src = project.source("ecb_fx_gbp")
        (project.raw_dir / src.cache_name).write_text("{}", encoding="utf-8")

        class Fake:
            body = b'{"rates": {"2010-12-02": {"USD": 1.5}}}'
            status = 200
            content_type = "application/json"
            url = src.url
            attempts = 1
            elapsed = 0.01

        monkeypatch.setattr("retailde.extract.fetch_bytes", lambda *a, **k: Fake())
        rec = extract(src, project, Manifest(project.raw_dir), force=True)
        assert rec.status == "ok"
        assert json.loads((project.raw_dir / src.cache_name).read_text())["rates"]


class TestRetries:
    def test_connection_failure_retries_then_raises(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr("retailde.extract.time.sleep", sleeps.append)

        with pytest.raises(ExtractionError, match="after 3 attempt"):
            fetch_bytes(UNREACHABLE, timeout=2, retries=3)
        assert sleeps == [1, 2], f"expected exponential backoff, got {sleeps}"

    def test_single_attempt_makes_no_backoff(self, monkeypatch):
        sleeps = []
        monkeypatch.setattr("retailde.extract.time.sleep", sleeps.append)
        with pytest.raises(ExtractionError):
            fetch_bytes(UNREACHABLE, timeout=2, retries=1)
        assert sleeps == []

    def test_retries_argument_is_respected(self, monkeypatch):
        attempts = []
        monkeypatch.setattr("retailde.extract.time.sleep",
                            lambda s: attempts.append(s))

        with pytest.raises(ExtractionError, match="after 2 attempt"):
            fetch_bytes(UNREACHABLE, timeout=2, retries=2)
        assert len(attempts) == 1, "2 attempts means exactly 1 backoff"

    def test_unreachable_url_message_names_the_url(self):
        with pytest.raises(ExtractionError) as exc:
            fetch_bytes(UNREACHABLE, timeout=2, retries=1)
        assert "127.0.0.1" in str(exc.value)


class TestProvenance:
    def test_manifest_records_each_source(self, project):
        from retailde.manifest import Manifest as M
        manifest = M(project.raw_dir)
        for sid in ("ecb_fx_gbp", "open_meteo_london"):
            extract(project.source(sid), project, manifest)
        rows = manifest.summary_rows()
        assert {r["source"] for r in rows} >= {"ecb_fx_gbp", "open_meteo_london"}
        for row in rows:
            assert len(row["sha256"]) == 12, "hash is truncated for display"

    def test_manifest_persists_across_instances(self, project):
        from retailde.manifest import Manifest as M
        extract(project.source("ecb_fx_gbp"), project, M(project.raw_dir))
        reloaded = M(project.raw_dir)
        assert reloaded.get("ecb_fx_gbp") is not None
        assert reloaded.get("ecb_fx_gbp")["sha256"]

    def test_prune_drops_retired_sources(self, project):
        from retailde.manifest import Manifest as M
        manifest = M(project.raw_dir)
        # the fixture recorded all four sources; retire three of them
        removed = manifest.prune({"ecb_fx_gbp"})
        assert "open_meteo_london" in removed
        assert manifest.get("open_meteo_london") is None, \
            "provenance for a retired source must not linger"
        assert manifest.get("ecb_fx_gbp") is not None

    def test_prune_keeps_live_sources(self, project):
        from retailde.manifest import Manifest as M
        manifest = M(project.raw_dir)
        live = {s.id for s in project.enabled_sources}
        assert manifest.prune(live) == [], "nothing was retired"
        assert manifest.get("ecb_fx_gbp") is not None
        assert len(manifest.summary_rows()) == len(live)

    def test_corrupt_manifest_does_not_crash_the_pipeline(self, project):
        from retailde.manifest import Manifest as M
        path = project.raw_dir / "_manifest.json"
        path.write_text("{ not json", encoding="utf-8")
        manifest = M(project.raw_dir)
        assert manifest.summary_rows() == [], "must degrade, not raise"


class TestConfig:
    def test_duplicate_source_ids_rejected(self, tmp_path):
        from retailde.config import AppConfig
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "sources.yaml").write_text(
            "sources:\n"
            "  - {id: a, adapter: rest_api, url: 'http://x'}\n"
            "  - {id: a, adapter: rest_api, url: 'http://y'}\n",
            encoding="utf-8")
        with pytest.raises(ValueError, match="duplicate source ids"):
            AppConfig.load(root=tmp_path)

    def test_unknown_adapter_rejected(self, tmp_path):
        from retailde.config import AppConfig
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "sources.yaml").write_text(
            "sources:\n  - {id: a, adapter: ftp, url: 'http://x'}\n",
            encoding="utf-8")
        with pytest.raises(ValueError, match="unknown adapter"):
            AppConfig.load(root=tmp_path)

    def test_missing_url_rejected(self, tmp_path):
        from retailde.config import AppConfig
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "sources.yaml").write_text(
            "sources:\n  - {id: a, adapter: rest_api}\n", encoding="utf-8")
        with pytest.raises(ValueError, match="missing 'url'"):
            AppConfig.load(root=tmp_path)

    def test_enabled_sources_filters_disabled(self, project):
        assert {s.id for s in project.enabled_sources} == {
            "uci_online_retail", "open_meteo_london",
            "ecb_fx_gbp", "boe_bank_rate"}

    def test_unknown_source_id_raises_keyerror(self, project):
        with pytest.raises(KeyError, match="unknown source id"):
            project.source("does_not_exist")

    def test_ensure_dirs_is_idempotent(self, project):
        project.ensure_dirs()
        project.ensure_dirs()
        assert project.raw_dir.is_dir() and project.staging_dir.is_dir()
