"""Dashboard: KPI summary, charts, and a self-contained HTML report.

Output is written to ``data/reports``:

* ``dashboard.html`` -- one static page, no JS, opens from disk
* ``*.png``         -- the individual charts, also referenced by the README

Charts are PNG rather than an interactive JS bundle on purpose. A plotly
dashboard drags in ~4 MB of library code, needs a browser to be meaningful,
and renders nothing at all when someone opens the repo on GitHub. PNGs are a
few hundred KB total and show up inline in the README without any tooling.
"""

from __future__ import annotations

import html
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ..config import AppConfig

# Non-interactive backend: the report runs in CI/CLI with no display.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

GOLD = "#d9a441"
INK = "#08090a"
MUTED = "#8b949e"
GRID = "#1c2128"


def _style_axes(ax, title: str) -> None:
    ax.set_title(title, color=INK, fontsize=11, fontweight="bold", pad=10)
    ax.tick_params(colors=MUTED, labelsize=8)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.6, alpha=0.6)
    ax.set_axisbelow(True)


def _kpis(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    row = con.execute(
        """
        SELECT
            ROUND(SUM(CASE WHEN is_valid_sale THEN line_revenue ELSE 0 END), 2),
            COUNT(DISTINCT invoice) FILTER (WHERE is_valid_sale),
            COUNT(DISTINCT customer_id),
            SUM(CASE WHEN is_valid_sale THEN quantity ELSE 0 END),
            COUNT(DISTINCT stock_code),
            COUNT(DISTINCT country)
        FROM v_transactions
        """
    ).fetchone()
    extra = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM fact_transaction),
            (SELECT COUNT(*) FROM dim_customer),
            (SELECT COUNT(*) FROM dim_date),
            (SELECT MIN(date) FROM dim_date),
            (SELECT MAX(date) FROM dim_date)
        """
    ).fetchone()
    return {
        "revenue": row[0],
        "orders": row[1],
        "customers": row[2],
        "units": row[3],
        "products": row[4],
        "countries": row[5],
        "fact_rows": extra[0],
        "dim_customers": extra[1],
        "calendar_days": extra[2],
        "date_from": str(extra[3]),
        "date_to": str(extra[4]),
    }


def _chart_monthly(con, path: Path) -> None:
    data = con.execute(
        """
        SELECT year_month, ROUND(SUM(revenue)/1000.0, 1)
        FROM mart_monthly_revenue
        GROUP BY year_month ORDER BY year_month
        """
    ).fetchall()
    fig, ax = plt.subplots(figsize=(9, 3.4), dpi=130)
    xs = [d[0] for d in data]
    ys = [d[1] for d in data]
    ax.plot(xs, ys, color=GOLD, linewidth=2, marker="o", markersize=3.5)
    ax.fill_between(xs, ys, color=GOLD, alpha=0.13)
    _style_axes(ax, "Monthly revenue (£k)")
    ax.set_ylabel("£ thousands", color=MUTED, fontsize=8)
    ax.tick_params(axis="x", rotation=45, labelsize=7)
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)


def _chart_top_products(con, path: Path) -> None:
    data = con.execute(
        """
        SELECT description, ROUND(SUM(revenue)/1000.0, 1) AS rev
        FROM mart_product_performance
        WHERE description IS NOT NULL
        GROUP BY description ORDER BY rev DESC LIMIT 10
        """
    ).fetchall()
    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=130)
    labels = [str(d[0])[:34] for d in reversed(data)]
    vals = [d[1] for d in reversed(data)]
    ax.barh(labels, vals, color=GOLD, alpha=0.9, height=0.68)
    _style_axes(ax, "Top 10 products by revenue (£k)")
    ax.set_xlabel("£ thousands", color=MUTED, fontsize=8)
    ax.tick_params(axis="y", labelsize=7.5)
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)


def _chart_segments(con, path: Path) -> None:
    data = con.execute(
        """
        SELECT segment, COUNT(*) FROM mart_customer_segments
        WHERE NOT is_unknown_customer GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    fig, ax = plt.subplots(figsize=(5.6, 3.8), dpi=130)
    ax.bar([d[0] for d in data], [d[1] for d in data],
           color=GOLD, alpha=0.9, width=0.62)
    _style_axes(ax, "Customers by RFM segment")
    ax.set_ylabel("customers", color=MUTED, fontsize=8)
    ax.tick_params(axis="x", rotation=30, labelsize=7.5)
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)


def _chart_weather(con, path: Path) -> None:
    data = con.execute(
        """
        SELECT tmax_c, revenue FROM mart_daily_kpis
        WHERE tmax_c IS NOT NULL AND revenue > 0
        """
    ).fetchall()
    fig, ax = plt.subplots(figsize=(6.4, 3.6), dpi=130)
    ax.scatter([d[0] for d in data], [d[1] for d in data],
               s=13, alpha=0.5, color=GOLD, edgecolors="none")
    _style_axes(ax, "Daily revenue vs maximum temperature")
    ax.set_xlabel("max temp (°C)", color=MUTED, fontsize=8)
    ax.set_ylabel("revenue (£)", color=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)


def _chart_regions(con, path: Path) -> None:
    data = con.execute(
        """
        SELECT region, ROUND(SUM(revenue)/1000.0, 1)
        FROM mart_region_summary GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """
    ).fetchall()
    fig, ax = plt.subplots(figsize=(6.4, 3.4), dpi=130)
    ax.bar([d[0] for d in data], [d[1] for d in data],
           color=GOLD, alpha=0.9, width=0.6)
    _style_axes(ax, "Revenue by region (£k)")
    ax.set_ylabel("£ thousands", color=MUTED, fontsize=8)
    ax.tick_params(axis="x", rotation=30, labelsize=8)
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)


CHARTS = [
    ("monthly_revenue", "Monthly revenue trend", _chart_monthly),
    ("top_products", "Top products", _chart_top_products),
    ("rfm_segments", "Customer segments", _chart_segments),
    ("weather_scatter", "Weather vs revenue", _chart_weather),
    ("regions", "Revenue by region", _chart_regions),
]


def build(cfg: AppConfig) -> dict[str, Any]:
    """Render every chart and the HTML report. Returns a stats block."""
    cfg.ensure_dirs()
    t0 = time.perf_counter()

    con = duckdb.connect(str(cfg.warehouse_path), read_only=True)
    try:
        kpis = _kpis(con)
        written = []
        for slug, _title, fn in CHARTS:
            out = cfg.reports_dir / f"{slug}.png"
            fn(con, out)
            written.append({"name": out.name, "bytes": out.stat().st_size})

        # a small data table for the report body
        monthly = con.execute(
            "SELECT year_month, SUM(revenue), SUM(orders) "
            "FROM mart_monthly_revenue GROUP BY 1 ORDER BY 1"
        ).fetchall()
        segments = con.execute(
            "SELECT segment, COUNT(*), ROUND(SUM(monetary),2) "
            "FROM mart_customer_segments WHERE NOT is_unknown_customer "
            "GROUP BY 1 ORDER BY 3 DESC"
        ).fetchall()
        regions = con.execute(
            "SELECT region, ROUND(SUM(revenue),2), SUM(orders) "
            "FROM mart_region_summary GROUP BY 1 ORDER BY 2 DESC LIMIT 8"
        ).fetchall()
    finally:
        con.close()

    page = _html(kpis, monthly, segments, regions, written)
    page_path = cfg.reports_dir / "dashboard.html"
    page_path.write_text(page, encoding="utf-8")

    (cfg.reports_dir / "kpis.json").write_text(
        __import__("json").dumps(kpis, indent=2, default=str), encoding="utf-8"
    )

    return {
        "charts": written,
        "html": str(page_path.relative_to(cfg.root)),
        "kpis": kpis,
        "elapsed": round(time.perf_counter() - t0, 3),
    }


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def _money(v: Any) -> str:
    try:
        return f"£{float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _table(headers: list[str], rows: list[list[Any]], fmt=None) -> str:
    fmt = fmt or (lambda c, v: html.escape(str(v)))
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{fmt(c, v)}</td>" for c, v in enumerate(row)) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _html(kpis, monthly, segments, regions, charts) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    cards = [
        ("Revenue (valid sales)", _money(kpis["revenue"])),
        ("Orders", f"{kpis['orders']:,}"),
        ("Identified customers", f"{kpis['customers']:,}"),
        ("Units sold", f"{kpis['units']:,}"),
        ("Fact rows", f"{kpis['fact_rows']:,}"),
        ("Countries", f"{kpis['countries']:,}"),
    ]
    card_html = "".join(
        f'<div class="card"><div class="label">{html.escape(l)}</div>'
        f'<div class="value">{html.escape(v)}</div></div>'
        for l, v in cards
    )

    chart_html = "".join(
        f'<figure><img src="{html.escape(c["name"])}" alt="{html.escape(c["name"])}"></figure>'
        for c in charts
    )

    month_rows = [[m[0], _money(m[1]), f"{int(m[2]):,}"] for m in monthly]
    seg_rows = [[s[0], f"{int(s[1]):,}", _money(s[2])] for s in segments]
    reg_rows = [[r[0], _money(r[1]), f"{int(r[2]):,}"] for r in regions]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>UK Retail Analytics &middot; Data Engineering Dashboard</title>
<style>
  :root {{ --ink:#08090a; --gold:#d9a441; --muted:#8b949e; --line:#e6e8eb; --bg:#fbfbfa; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--ink);
         font:15px/1.6 "Segoe UI", Inter, system-ui, -apple-system, sans-serif; }}
  .wrap {{ max-width:1080px; margin:0 auto; padding:40px 24px 72px; }}
  header h1 {{ font-size:26px; margin:0 0 4px; letter-spacing:-.02em; }}
  header p {{ color:var(--muted); margin:0 0 6px; font-size:13.5px; }}
  .stamp {{ color:var(--muted); font-size:12px; margin-bottom:28px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
            gap:12px; margin-bottom:34px; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:9px;
           padding:15px 16px; }}
  .card .label {{ color:var(--muted); font-size:11px; text-transform:uppercase;
                  letter-spacing:.06em; }}
  .card .value {{ font-size:21px; font-weight:650; margin-top:5px;
                  font-variant-numeric:tabular-nums; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr));
           gap:22px; margin-bottom:30px; }}
  figure {{ margin:0; background:#fff; border:1px solid var(--line);
            border-radius:9px; padding:12px; }}
  figure img {{ width:100%; height:auto; display:block; }}
  h2 {{ font-size:16px; margin:34px 0 12px; letter-spacing:-.01em; }}
  table {{ width:100%; border-collapse:collapse; background:#fff;
           border:1px solid var(--line); border-radius:9px; overflow:hidden;
           font-size:13px; }}
  th {{ text-align:left; background:#f4f5f6; color:var(--muted); font-weight:600;
        font-size:11px; text-transform:uppercase; letter-spacing:.05em;
        padding:9px 12px; border-bottom:1px solid var(--line); }}
  td {{ padding:8px 12px; border-bottom:1px solid var(--line);
        font-variant-numeric:tabular-nums; }}
  tbody tr:last-child td {{ border-bottom:none; }}
  td:first-child {{ font-variant-numeric:normal; }}
  footer {{ margin-top:40px; padding-top:18px; border-top:1px solid var(--line);
            color:var(--muted); font-size:12px; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>UK Retail Analytics</h1>
    <p>Data engineering capstone &mdash; four heterogeneous sources
       (bulk archive, two REST APIs, one HTML scrape) modelled into a
       dimensional warehouse.</p>
    <div class="stamp">{kpis['date_from']} &rarr; {kpis['date_to']} &middot;
      {kpis['calendar_days']:,} calendar days &middot; generated {generated}</div>
  </header>

  <div class="cards">{card_html}</div>

  <div class="grid">{chart_html}</div>

  <h2>Monthly revenue</h2>
  {_table(["Month", "Revenue", "Orders"], month_rows)}

  <h2>Customer segments (RFM)</h2>
  {_table(["Segment", "Customers", "Revenue"], seg_rows)}

  <h2>Revenue by region</h2>
  {_table(["Region", "Revenue", "Orders"], regions and reg_rows)}

  <footer>
    Source: UCI Online Retail II (a UK-based online retailer, 2009&ndash;2011),
    Open-Meteo, ECB via Frankfurter, Bank of England.
    Facts exclude cancellations, returns and non-positive prices from revenue
    while retaining every row in <code>fact_transaction</code>.
  </footer>
</div>
</body>
</html>
"""


__all__ = ["build"]
