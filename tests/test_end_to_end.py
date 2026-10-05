"""Full two-day pipeline in a temporary workspace: the strongest regression test we have."""
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests.helpers import ROOT
from mdm.cli import _generate
from mdm.config import PlatformConfig
from mdm.evaluation import evaluate
from mdm.lineage.tracker import build_trace
from mdm.pipeline import Pipeline
from mdm.serving.api import route
from mdm.serving.repository import GoldenRecordRepository
from mdm.storage.lake import LakeStore


class EndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        shutil.copytree(ROOT / "config", cls.tmp / "config")
        shutil.copytree(ROOT / "data_generator", cls.tmp / "data_generator")
        _generate(str(cls.tmp), 1, 200)
        cls.cfg = PlatformConfig.load(cls.tmp)
        cls.day1 = Pipeline(cls.cfg, log_format="text").run()
        _generate(str(cls.tmp), 2, 200)
        cls.day2 = Pipeline(cls.cfg, log_format="text").run()
        cls.lake = LakeStore(cls.cfg.path("lakehouse"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_both_runs_reconcile(self):
        self.assertEqual(self.day1["reconciliation"]["status"], "BALANCED")
        self.assertEqual(self.day2["reconciliation"]["status"], "BALANCED")

    def test_dead_letter_captures_broken_records(self):
        self.assertEqual(self.day1["metrics"]["metrics"]["ingest.crm.dead_letter"], 1)          # malformed CSV row
        self.assertEqual(self.day1["metrics"]["metrics"]["ingest.erp_billing.dead_letter"], 1)  # missing PK

    def test_incremental_day2_only_processes_deltas(self):
        m = self.day2["metrics"]["metrics"]
        self.assertLess(m["ingest.crm.bronze"], 100)
        self.assertGreater(m["standardize.unchanged_resends"], 0)
        self.assertGreater(m["match.pairs_reused"], 0)

    def test_schema_drift_rescued(self):
        bronze = self.lake.read("bronze", "crm", run_id=self.day2["run_id"])
        self.assertTrue(bronze["_rescued_data"].notna().any())

    def test_bridging_record_merges_golden_records_with_redirect(self):
        self.assertGreaterEqual(self.day2["metrics"]["metrics"]["golden.events.merge"], 1)
        red = self.lake.read("gold", "golden_redirects")
        repo = GoldenRecordRepository(self.cfg)
        bodies = [(old, route(repo, f"/golden-records/{old}", {})) for old in red["old_golden_id"]]
        kj = [(old, b) for old, (st, b) in bodies if st == 200 and b["email"] == "k.johnson88@gmail.com"]
        self.assertEqual(len(kj), 1, "the Katherine Johnson CRM/ecommerce records must be merged on day 2")
        old, body = kj[0]
        self.assertEqual(body["_redirected_from"], old)
        self.assertEqual(body["source_record_count"], 3)

    def test_lineage_trace_is_complete(self):
        g = self.lake.read("gold", "golden_record")
        gid = g.sort_values("source_record_count").iloc[-1]["golden_id"]
        t = build_trace(self.lake, gid)
        for k in ("source_records", "merge_decisions", "survivorship_decisions", "lifecycle_events"):
            self.assertTrue(t[k], k)
        self.assertTrue(t["source_records"][0]["transformations"])

    def test_accuracy_against_ground_truth(self):
        res = evaluate(self.lake.read("gold", "xref"), pd.read_csv(self.cfg.path("ground_truth"), dtype=str))
        self.assertGreaterEqual(res["precision"], 0.98)
        self.assertGreaterEqual(res["recall"], 0.90)

    def test_api_search(self):
        status, body = route(GoldenRecordRepository(self.cfg), "/golden-records", {"last_name": ["Johnson"]})
        self.assertEqual(status, 200)
        self.assertTrue(all(r["last_name"] == "Johnson" for r in body))


if __name__ == "__main__":
    unittest.main()
