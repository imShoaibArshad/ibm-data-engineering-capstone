"""Shared fixtures.

Every test runs **offline** against synthetic sources built here. That split
-- parsing/cleaning separated from fetching -- is what makes it possible: the
transforms only ever see bytes that already exist on disk, so no test touches
the network and the suite runs in a couple of seconds.

The synthetic transaction workbook deliberately reproduces every defect found
in the real UCI file, in miniature:

* mixed ``StockCode`` types (``85048`` int alongside ``'79323P'`` str)
* mixed ``Invoice`` types (``489434`` and ``'C489449'``)
* rows with a missing ``Customer ID``
* negative quantities (returns) and non-positive prices
* leading/trailing whitespace in descriptions
* exact-duplicate rows
* two worksheets that overlap by several days, like the real source
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# project root for a synthetic run
# ---------------------------------------------------------------------------

MINIMAL_YAML = """
project: test-retail
warehouse: data/warehouse/retail.duckdb

sources:
  - id: uci_online_retail
    name: Synthetic Retail
    adapter: bulk_file
    enabled: true
    url: http://127.0.0.1:9/unreachable.zip
    format: zip
    # The fixture writes the workbook directly, so the manifest key must point
    # at the same file the transform actually reads.
    cache_as: online_retail_II.xlsx
    timeout_seconds: 2
    retries: 1

  - id: open_meteo_london
    name: Synthetic Weather
    adapter: rest_api
    enabled: true
    url: http://127.0.0.1:9/unreachable
    cache_as: open_meteo_london.json
    timeout_seconds: 2
    retries: 1

  - id: ecb_fx_gbp
    name: Synthetic FX
    adapter: rest_api
    enabled: true
    url: http://127.0.0.1:9/unreachable
    cache_as: ecb_fx_gbp.json
    timeout_seconds: 2
    retries: 1

  - id: boe_bank_rate
    name: Synthetic Bank Rate
    adapter: html_page
    enabled: true
    url: http://127.0.0.1:9/unreachable
    cache_as: boe_bank_rate.html
    table_header_contains: Date Changed
    timeout_seconds: 2
    retries: 1

quality:
  null_customer_id_rate_max: 0.25
  duplicate_natural_key_rate_max: 0.001
  # Sized to the synthetic fixture (13 clean rows), not the real 1M-row file.
  min_transaction_rows: 10
  weather_coverage_min: 0.95
  fx_coverage_min: 0.90
  require_referential_integrity: true
"""

COUNTRY_CSV = (
    "country,region\n"
    "United Kingdom,Europe\n"
    "Germany,Europe\n"
    "France,Europe\n"
    "Australia,Oceania\n"
    "USA,North America\n"
    "Unspecified,Unknown\n"
)

# (invoice, stockcode, description, qty, when, price, customer, country)
SHEET_1 = [
    (489434, 85048, "  LANTERN COLOUR  ", 12, "2010-12-02 07:45", 6.95, 13085, "United Kingdom"),
    (489434, "79323P", "PINK CHERRY LIGHTS", 12, "2010-12-02 07:45", 6.75, 13085, "United Kingdom"),
    (489434, "79323W", None, 12, "2010-12-02 07:45", 6.75, 13085, "United Kingdom"),
    ("C489449", 21491, "SET OF THREE GIFT WRAPS", -4, "2010-12-02 08:00", 1.95, 13085, "United Kingdom"),
    (489450, 85048, "LANTERN COLOUR", 6, "2010-12-03 09:12", 0.0, None, "Germany"),
    (489450, 22041, "RECORD FRAME", 3, "2010-12-03 09:12", 2.55, None, "France"),
    (489451, "85123A", "WHITE HEART HOLDER", 10, "2010-12-04 10:00", 2.55, 14911, "Australia"),
    # exact duplicate of the line above (same invoice, same everything)
    (489451, "85123A", "WHITE HEART HOLDER", 10, "2010-12-04 10:00", 2.55, 14911, "Australia"),
    (489452, 22423, "REGENCY CAKESTAND", 1, "2010-12-05 11:30", 12.75, 14911, "USA"),
    (489453, "DOT", "DOTCOM POSTAGE", 1, "2010-12-06 12:00", 0.0, 14911, "USA"),
]

# Sheet 2 starts inside sheet 1's range -> the real file's overlap defect.
SHEET_2 = [
    # identical to a sheet-1 row: cross-sheet duplicate
    (489452, 22423, "REGENCY CAKESTAND", 1, "2010-12-05 11:30", 12.75, 14911, "USA"),
    (489454, "85099B", "JUMBO BAG RED", 20, "2010-12-07 13:00", 1.95, 13085, "United Kingdom"),
    (489455, 22633, "HAND WARMER UNION", 24, "2010-12-08 14:20", 1.65, 12347, "Germany"),
    (489456, 21754, "HOME BUILDING BLOCK", 6, "2010-12-09 15:00", 6.45, 12347, "France"),
    (489457, "84029G", "KNITTED UNION FLAG", 12, "2010-12-10 16:10", 4.25, 14911, "Australia"),
]


def _write_workbook(raw_dir: Path) -> None:
    import openpyxl

    wb = openpyxl.Workbook()
    header = ["Invoice", "StockCode", "Description", "Quantity",
              "InvoiceDate", "Price", "Customer ID", "Country"]

    first = wb.active
    first.title = "Year 2009-2010"
    first.append(header)
    for row in SHEET_1:
        first.append([
            row[0], row[1], row[2], row[3],
            datetime.fromisoformat(row[4]), row[5], row[6], row[7],
        ])

    second = wb.create_sheet("Year 2010-2011")
    second.append(header)
    for row in SHEET_2:
        second.append([
            row[0], row[1], row[2], row[3],
            datetime.fromisoformat(row[4]), row[5], row[6], row[7],
        ])

    raw_dir.mkdir(parents=True, exist_ok=True)
    wb.save(raw_dir / "online_retail_II.xlsx")


def _write_weather(raw_dir: Path) -> None:
    start = datetime(2010, 12, 2)
    days = [(start + timedelta(days=i)).date().isoformat() for i in range(9)]
    payload = {
        "latitude": 51.5, "longitude": -0.13, "timezone": "Europe/London",
        "daily_units": {"time": "iso8601", "temperature_2m_max": "°C"},
        "daily": {
            "time": days,
            "temperature_2m_max": [5.8, 7.1, 9.4, 4.2, 8.8, 6.5, 11.0, 3.3, 7.7],
            "temperature_2m_min": [-0.9, 2.2, 3.1, 0.4, 4.4, 2.0, 6.1, -1.2, 2.9],
            "precipitation_sum": [0.3, 0.0, 5.5, 0.0, 0.0, 12.4, 0.0, 0.0, 0.6],
        },
    }
    (raw_dir / "open_meteo_london.json").write_text(
        json.dumps(payload), encoding="utf-8")


def _write_fx(raw_dir: Path) -> None:
    payload = {
        "amount": 1.0, "base": "GBP",
        "start_date": "2010-12-02", "end_date": "2010-12-10",
        "rates": {
            "2010-12-02": {"USD": 1.55, "EUR": 1.17},
            "2010-12-03": {"USD": 1.56, "EUR": 1.18},
            "2010-12-06": {"USD": 1.57, "EUR": 1.16},
            "2010-12-07": {"USD": 1.58, "EUR": 1.15},
            "2010-12-08": {"USD": 1.59, "EUR": 1.14},
            "2010-12-09": {"USD": 1.60, "EUR": 1.13},
            "2010-12-10": {"USD": 1.61, "EUR": 1.12},
        },
    }
    (raw_dir / "ecb_fx_gbp.json").write_text(json.dumps(payload), encoding="utf-8")


BANK_RATE_HTML = """<!DOCTYPE html>
<html><head><title>Bank Rate history</title></head>
<body>
<h2>Official Bank Rate</h2>
<table>
  <tr><th>Date Changed</th><th>Rate</th></tr>
  <tr><td>18 Dec 25</td><td>3.75</td></tr>
  <tr><td>04 Nov 21</td><td>0.10</td></tr>
  <tr><td>05 Mar 09</td><td>0.50</td></tr>
  <tr><td>05 Feb 09</td><td>1.00</td></tr>
  <tr><td>08 Jan 09</td><td>1.50</td></tr>
  <tr><td>04 Dec 08</td><td>2.00</td></tr>
</table>
<table>
  <tr><th>Necessary cookies</th><th>Analytics cookies</th></tr>
  <tr><td>Yes</td><td>Yes</td></tr>
</table>
</body></html>
"""


def _write_config(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    (root / "config" / "sources.yaml").write_text(MINIMAL_YAML, encoding="utf-8")
    (root / "config" / "country_regions.csv").write_text(COUNTRY_CSV, encoding="utf-8")


@pytest.fixture()
def project(tmp_path: Path):
    """A synthetic project: config + raw zone populated, no network needed.

    Provenance is recorded for every source, exactly as a real extract run
    would leave it -- the quality gate that checks "did every enabled source
    land?" reads the manifest, so a fixture that wrote bytes without recording
    them would fail a check that is working correctly.
    """
    from retailde.config import AppConfig
    from retailde.manifest import ExtractRecord, Manifest, sha256_file

    _write_config(tmp_path)
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    _write_workbook(raw)
    _write_weather(raw)
    _write_fx(raw)
    (raw / "boe_bank_rate.html").write_text(BANK_RATE_HTML, encoding="utf-8")

    cfg = AppConfig.load(root=tmp_path)
    cfg.ensure_dirs()

    manifest = Manifest(raw)
    for src in cfg.enabled_sources:
        path = raw / src.cache_name
        if not path.exists():
            continue
        manifest.record(ExtractRecord(
            source_id=src.id,
            source_name=src.name,
            adapter=src.adapter,
            url=src.url,
            started_at="1970-01-01T00:00:00+00:00",
            elapsed_seconds=0.0,
            bytes=path.stat().st_size,
            sha256=sha256_file(path),
            status="cached",
            cached_path=str(path),
            detail="synthetic fixture",
        ))
    return cfg


@pytest.fixture()
def staged(project):
    """The project with staging completed, ready for load/quality tests."""
    from retailde.transform import stage_all

    stage_all(project, force=True)
    return project
