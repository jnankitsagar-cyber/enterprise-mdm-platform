"""Static operational dashboard (reports/dashboard.html).

Zero-dependency HTML generated after every run from the run manifests,
reconciliation report and DQ results. In Azure the same metrics feed an
Azure Monitor Workbook / Power BI / Databricks SQL dashboard; this file is the
local stand-in so the demo is self-contained.
"""
from __future__ import annotations

import html
import json

import pandas as pd

CSS = """
:root{--bg:#f7f7f5;--card:#fff;--ink:#1d1d1b;--mute:#6b6b66;--line:#e4e4df;--ok:#2e7d32;--warn:#b26a00;--bad:#c62828;--bar:#3b6fb6;--bar2:#9bb7e0}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--card:#20201e;--ink:#ececea;--mute:#a3a39d;--line:#34342f;--bar:#6f9be0;--bar2:#3d5a85}}
*{box-sizing:border-box}body{margin:0;font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--ink)}
main{max-width:1180px;margin:0 auto;padding:24px 16px}h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:0 0 12px}
.sub{color:var(--mute);margin-bottom:20px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:16px;overflow-x:auto}
.kpi .v{font-size:24px;font-weight:600}.kpi .l{color:var(--mute);font-size:12px;text-transform:uppercase;letter-spacing:.04em}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}th{color:var(--mute);font-weight:500}
.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.pill{padding:2px 8px;border-radius:99px;border:1px solid currentColor;font-size:12px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:16px}@media(max-width:800px){.two{grid-template-columns:1fr}}
svg text{fill:var(--ink);font-size:11px}
"""


def _bar_svg(items: list[tuple[str, float]], width=520, color="var(--bar)", fmt="{:,.0f}") -> str:
    if not items:
        return "<p>No data</p>"
    mx = max(v for _, v in items) or 1
    h = 26 * len(items) + 6
    rows = []
    for i, (label, v) in enumerate(items):
        y = i * 26 + 4
        w = (width - 190) * v / mx
        rows.append(f'<text x="0" y="{y + 14}">{html.escape(str(label).replace("_", " "))}</text>'
                    f'<rect x="150" y="{y}" width="{w:.1f}" height="18" rx="3" fill="{color}"/>'
                    f'<text x="{155 + w:.1f}" y="{y + 14}">{fmt.format(v)}</text>')
    return f'<svg viewBox="0 0 {width} {h}" width="100%" role="img">{"".join(rows)}</svg>'


def build_dashboard(cfg, lake) -> None:
    runs_path = cfg.path("lakehouse") / "_runs.jsonl"
    runs = [json.loads(l) for l in runs_path.read_text().splitlines() if l.strip()] if runs_path.exists() else []
    if not runs:
        return
    last = runs[-1]
    mv = last["metrics"]
    rp = cfg.path("reports") / f"reconciliation_{last['run_id']}.json"
    recon = json.loads(rp.read_text()) if rp.exists() else {"funnel": {}, "exceptions": [], "status": "n/a", "checks": []}
    review = lake.read("gold", "review_queue")
    dq = lake.read("silver", "dq_results", run_id=last["run_id"])

    def kpi(label, value, cls=""):
        return f'<div class="card kpi"><div class="l">{label}</div><div class="v {cls}">{value}</div></div>'

    status_cls = "ok" if last["status"] == "SUCCESS" and recon.get("status") == "BALANCED" else "bad"
    kpis = "".join([
        kpi("Run status", f'{last["status"]} / {recon.get("status")}', status_cls),
        kpi("Golden records", f'{int(mv.get("golden.records", 0)):,}'),
        kpi("DQ pass rate (not quarantined)", f'{mv.get("dq.pass_rate", 0):.1%}', "ok" if mv.get("dq.pass_rate", 0) >= 0.85 else "warn"),
        kpi("Dedup ratio", f'{recon["funnel"].get("dedup_ratio", 0):.1%}'),
        kpi("Review queue", f'{len(review):,}', "warn" if len(review) else "ok"),
        kpi("Alerts", f'{len(last.get("alerts", []))}', "bad" if last.get("alerts") else "ok"),
    ])
    funnel = _bar_svg([(k, v) for k, v in recon["funnel"].items() if k != "dedup_ratio"])
    rule_counts = dq.groupby("rule").size().sort_values(ascending=False).head(10) if not dq.empty else pd.Series(dtype=int)
    dq_svg = _bar_svg(list(rule_counts.items()), color="var(--bar2)")
    timing = _bar_svg(sorted(last.get("timings_seconds", {}).items(), key=lambda x: -x[1]), fmt="{:.2f}s")

    hist_rows = "".join(
        f"<tr><td>{r['run_id']}</td><td>{r['status']}</td><td>{int(r['metrics'].get('golden.records', 0)):,}</td>"
        f"<td>{r['metrics'].get('dq.pass_rate', 0):.1%}</td><td>{int(r['metrics'].get('match.auto_match_pairs', 0))}</td>"
        f"<td>{int(r['metrics'].get('golden.events.merge', 0))}</td><td>{len(r.get('alerts', []))}</td></tr>"
        for r in reversed(runs[-15:]))
    exc_rows = "".join(
        f"<tr><td>{e['check']}</td><td class='{'bad' if e['severity'] == 'error' else 'warn'}'>{e['severity']}</td>"
        f"<td>{html.escape(str(e['source_system']))}</td><td>{html.escape(e['description'])}</td>"
        f"<td>{e['expected']}</td><td>{e['actual']}</td></tr>" for e in recon.get("exceptions", [])) or \
        "<tr><td colspan=6 class='ok'>No exceptions - every record accounted for.</td></tr>"
    alert_rows = "".join(
        f"<tr><td>{a['name']}</td><td class='{'bad' if a['severity'] == 'critical' else 'warn'}'>{a['severity']}</td>"
        f"<td>{a['metric']}</td><td>{a['value']}</td><td>{a['op']} {a['threshold']}</td></tr>" for a in last.get("alerts", [])) or \
        "<tr><td colspan=5 class='ok'>No alerts fired.</td></tr>"
    src_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in
                         ["source_system", "source_count", "bronze", "ingest_dlq", "standardized", "unchanged", "valid", "warning", "quarantined"]) + "</tr>"
        for r in recon.get("per_source", []))
    rev_rows = "".join(
        f"<tr><td>{r['record_key_a']}</td><td>{r['record_key_b']}</td><td>{float(r['probability']):.3f}</td><td>{html.escape(str(r['method']))}</td></tr>"
        for r in review.sort_values("probability", ascending=False).head(15).to_dict("records")) if not review.empty else \
        "<tr><td colspan=4>Queue empty.</td></tr>"

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MDM Operations</title><style>{CSS}</style></head><body><main>
<h1>Master Data Platform - Operations</h1><div class="sub">Latest run <code>{last['run_id']}</code> &middot; finished {last['finished'][:19]}Z</div>
<div class="grid">{kpis}</div>
<div class="two"><div class="card"><h2>Record funnel (current state)</h2>{funnel}</div>
<div class="card"><h2>Top DQ rule failures (this run)</h2>{dq_svg}</div></div>
<div class="card"><h2>Per-source reconciliation (this run)</h2><table><tr><th>source</th><th>control total</th><th>bronze</th><th>ingest DLQ</th><th>standardized</th><th>unchanged</th><th>valid</th><th>warning</th><th>quarantined</th></tr>{src_rows}</table></div>
<div class="two"><div class="card"><h2>Reconciliation exceptions</h2><table><tr><th>check</th><th>sev</th><th>source</th><th>description</th><th>expected</th><th>actual</th></tr>{exc_rows}</table></div>
<div class="card"><h2>Alerts</h2><table><tr><th>alert</th><th>sev</th><th>metric</th><th>value</th><th>rule</th></tr>{alert_rows}</table></div></div>
<div class="two"><div class="card"><h2>Stage duration (s)</h2>{timing}</div>
<div class="card"><h2>Data-steward review queue (top 15)</h2><table><tr><th>record A</th><th>record B</th><th>p(match)</th><th>reason</th></tr>{rev_rows}</table></div></div>
<div class="card"><h2>Run history</h2><table><tr><th>run</th><th>status</th><th>golden</th><th>DQ pass</th><th>auto-match pairs</th><th>merges</th><th>alerts</th></tr>{hist_rows}</table></div>
</main></body></html>"""
    out = cfg.path("reports") / "dashboard.html"
    out.write_text(page, encoding="utf-8")
