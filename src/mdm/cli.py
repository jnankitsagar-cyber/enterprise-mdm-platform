"""Command-line entry point.

    python -m mdm demo                 # reset -> day-1 data -> run -> day-2 data -> run -> evaluate
    python -m mdm generate --day 1|2   # synthetic data
    python -m mdm run                  # one pipeline run over whatever landed
    python -m mdm trace <golden_id>    # full lineage of a golden record
    python -m mdm evaluate             # precision / recall vs ground truth
    python -m mdm review               # data-steward review queue
    python -m mdm validate-config      # lint all YAML (CI gate)
    python -m mdm serve                # Golden Record REST API
    python -m mdm reset                # wipe lakehouse/, reports/, data/
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

from mdm.config import PlatformConfig


def _cfg(args) -> PlatformConfig:
    return PlatformConfig.load(args.root)


def _generate(root: str, day: int, people: int = 400, truncated: bool = False):
    sys.path.insert(0, str(Path(root).resolve()))
    from data_generator.generate import main as gen_main
    argv = ["--day", str(day), "--root", root, "--people", str(people)]
    if truncated:
        argv.append("--simulate-truncated-file")
    gen_main(argv)


def _summary(res: dict) -> None:
    r = res["reconciliation"]
    m = res["metrics"]["metrics"]
    print("\n" + "=" * 72)
    print(f" RUN {res['run_id']}   status={res['status']}   reconciliation={r['status']}")
    print("=" * 72)
    print(" funnel:", json.dumps(r["funnel"]))
    print(f" DQ: pass_rate={m.get('dq.pass_rate')}  valid={m.get('dq.valid', 0):.0f}  warning={m.get('dq.warning', 0):.0f}"
          f"  quarantined={m.get('dq.quarantined', 0):.0f}")
    print(f" matching: candidate_pairs={m.get('match.candidate_pairs', 0):.0f}  reused={m.get('match.pairs_reused', 0):.0f}"
          f"  auto_match={m.get('match.auto_match_pairs', 0):.0f}  review={m.get('match.review_pairs', 0):.0f}"
          f"  reduction_ratio={m.get('match.reduction_ratio')}")
    print(f" golden: records={m.get('golden.records', 0):.0f}  create={m.get('golden.events.create', 0):.0f}"
          f"  update={m.get('golden.events.update', 0):.0f}  merge={m.get('golden.events.merge', 0):.0f}"
          f"  split={m.get('golden.events.split', 0):.0f}  changed_versions={m.get('golden.changed', 0):.0f}")
    if r["exceptions"]:
        print(" reconciliation exceptions:")
        for e in r["exceptions"]:
            print(f"   - [{e['severity']}] {e['check']} {e['source_system']}: {e['description']} "
                  f"(expected {e['expected']}, actual {e['actual']})")
    for a in res["metrics"]["alerts"]:
        print(f" ALERT [{a['severity']}] {a['name']}: {a['metric']}={a['value']} ({a['op']} {a['threshold']})")
    print("=" * 72 + "\n")


def cmd_run(args):
    from mdm.pipeline import Pipeline
    res = Pipeline(_cfg(args), log_format=args.log_format).run()
    _summary(res)
    return res


def cmd_evaluate(args):
    from mdm.evaluation import evaluate
    from mdm.storage.lake import LakeStore
    cfg = _cfg(args)
    xref = LakeStore(cfg.path("lakehouse")).read("gold", "xref")
    truth = pd.read_csv(cfg.path("ground_truth"), dtype=str)
    res = evaluate(xref, truth)
    print(json.dumps({k: v for k, v in res.items() if k not in ("false_merges", "missed_matches")}, indent=2))
    if res["false_merges"]:
        print("sample false merges:", res["false_merges"][:5])
    if res["missed_matches"]:
        print("sample missed matches:", res["missed_matches"][:5])
    return res


def cmd_trace(args):
    from mdm.lineage.tracker import build_trace
    from mdm.storage.lake import LakeStore
    cfg = _cfg(args)
    t = build_trace(LakeStore(cfg.path("lakehouse")), args.golden_id)
    if t is None:
        print(f"golden record {args.golden_id} not found")
        sys.exit(1)
    print(json.dumps(t, indent=2, default=str))


def cmd_review(args):
    from mdm.storage.lake import LakeStore
    cfg = _cfg(args)
    lake = LakeStore(cfg.path("lakehouse"))
    rq = lake.read("gold", "review_queue")
    if rq.empty:
        print("review queue is empty")
        return
    silver = lake.read("silver", "party").set_index("record_key")
    cols = ["full_name", "email", "phone", "date_of_birth", "address_line1", "postal_code"]
    for r in rq.sort_values("probability", ascending=False).head(args.limit).to_dict("records"):
        print(f"\n p={r['probability']:.3f}  {r['method']}")
        for k in (r["record_key_a"], r["record_key_b"]):
            row = silver.loc[k]
            print(f"   {k:<22} " + " | ".join("-" if pd.isna(row.get(c)) else str(row.get(c)) for c in cols))
    print(f"\nTo decide: append rows to {cfg.path('state') / 'steward_decisions.csv'} "
          "(record_key_a,record_key_b,decision,steward,reason) and re-run.")


def cmd_validate(args):
    from mdm.matching.engine import IdentityResolutionEngine
    from mdm.quality.engine import DataQualityEngine
    from mdm.standardization.engine import CANONICAL_ATTRIBUTES, StandardizationEngine
    from mdm.survivorship.engine import SurvivorshipEngine
    cfg = _cfg(args)
    StandardizationEngine(cfg)
    DataQualityEngine(cfg.dq, cfg.reference)
    IdentityResolutionEngine(cfg.matching, cfg.reference)
    SurvivorshipEngine(cfg.survivorship)
    for s in cfg.sources:
        unknown = set(s.mapping) - set(CANONICAL_ATTRIBUTES) - {"source_record_id"}
        if unknown:
            raise SystemExit(f"{s.name}: mapping targets unknown canonical attributes {unknown}")
        for k in s.primary_key:
            if k not in s.schema:
                raise SystemExit(f"{s.name}: primary key {k} not in schema")
    print(f"configuration OK: {len(cfg.sources)} sources, {len(cfg.dq['rules'])} DQ rules, "
          f"{len(cfg.matching['comparisons'])} match comparisons, {len(cfg.survivorship['attributes'])} survivorship attributes")


def cmd_reset(args):
    root = Path(args.root)
    for d in ("lakehouse", "reports", "data"):
        shutil.rmtree(root / d, ignore_errors=True)
    print("reset: lakehouse/, reports/, data/ removed")


def cmd_demo(args):
    cmd_reset(args)
    print("\n### DAY 1: initial load from 4 source systems ###")
    _generate(args.root, 1, args.people)
    cmd_run(args)
    print("\n### DAY 2: incremental load (updates, new records, schema drift, bridging record) ###")
    _generate(args.root, 2, args.people, truncated=args.simulate_truncated_file)
    cmd_run(args)
    print("### Identity-resolution accuracy vs ground truth ###")
    cmd_evaluate(args)
    cfg = _cfg(args)
    print(f"\nDashboard:       {cfg.path('reports') / 'dashboard.html'}")
    print(f"Reconciliation:  {cfg.path('reports')}/reconciliation_<run>.md")
    print("Trace a record:  python -m mdm trace <golden_id>")


def cmd_serve(args):
    from mdm.serving.api import serve, serve_stdlib
    cfg = _cfg(args)
    (serve_stdlib if args.stdlib else serve)(cfg, args.host, args.port)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="mdm", description="Enterprise Master Data Platform")
    ap.add_argument("--root", default=".", help="repository root (config/, data/, lakehouse/)")
    ap.add_argument("--log-format", choices=["json", "text"], default="text")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate"); g.add_argument("--day", type=int, choices=[1, 2], required=True)
    g.add_argument("--people", type=int, default=400); g.add_argument("--simulate-truncated-file", action="store_true")
    sub.add_parser("run")
    sub.add_parser("evaluate")
    t = sub.add_parser("trace"); t.add_argument("golden_id")
    rv = sub.add_parser("review"); rv.add_argument("--limit", type=int, default=10)
    sub.add_parser("validate-config")
    sub.add_parser("reset")
    d = sub.add_parser("demo"); d.add_argument("--people", type=int, default=400)
    d.add_argument("--simulate-truncated-file", action="store_true")
    s = sub.add_parser("serve"); s.add_argument("--host", default="127.0.0.1"); s.add_argument("--port", type=int, default=8000)
    s.add_argument("--stdlib", action="store_true")
    args = ap.parse_args(argv)
    {"generate": lambda a: _generate(a.root, a.day, a.people, a.simulate_truncated_file), "run": cmd_run,
     "evaluate": cmd_evaluate, "trace": cmd_trace, "review": cmd_review, "validate-config": cmd_validate,
     "reset": cmd_reset, "demo": cmd_demo, "serve": cmd_serve}[args.cmd](args)


if __name__ == "__main__":
    main()
