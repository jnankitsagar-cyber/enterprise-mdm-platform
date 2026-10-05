import json
import unittest

import pandas as pd

from tests.helpers import CFG
from mdm.quality.engine import DataQualityEngine
from mdm.quality.safe_eval import UnsafeExpression, compile_expression


def rec(**kw):
    base = {"record_key": "crm:1", "source_system": "crm", "source_record_id": "1", "first_name": "Ann", "last_name": "Lee",
            "email": "ann@x.com", "phone": "+16175550142", "date_of_birth": "1980-01-01", "country_code": "US",
            "state_code": "MA", "postal_code": "02134", "status": "ACTIVE", "source_updated_at": "2026-01-01T00:00:00Z",
            "_raw": "{}", "_std_errors": "[]"}
    base.update(kw)
    return base


class TestDQ(unittest.TestCase):
    def setUp(self):
        self.eng = DataQualityEngine(CFG.dq, CFG.reference)

    def run_one(self, **kw):
        out, fails, _ = self.eng.run(pd.DataFrame([rec(**kw)]), "t")
        return out.iloc[0]["_dq_status"], {f["rule_id"] for f in json.loads(out.iloc[0]["_dq_failures"])}

    def test_clean_record_valid(self):
        self.assertEqual(self.run_one()[0], "VALID")

    def test_missing_last_name_quarantined(self):
        st, ids = self.run_one(last_name=None)
        self.assertEqual(st, "QUARANTINED"); self.assertIn("DQ002", ids)

    def test_not_contactable_quarantined(self):
        self.assertEqual(self.run_one(email=None, phone=None)[0], "QUARANTINED")

    def test_future_timestamp_quarantined(self):
        self.assertIn("DQ021", self.run_one(source_updated_at="2999-01-01T00:00:00Z")[1])

    def test_warnings_do_not_quarantine(self):
        st, ids = self.run_one(email="bad@@x", status="VIP", postal_code="123")
        self.assertEqual(st, "WARNING")
        self.assertTrue({"DQ010", "DQ030", "DQ060"} <= ids)

    def test_minor_dob_flagged(self):
        self.assertIn("DQ020", self.run_one(date_of_birth="2020-01-01")[1])

    def test_duplicate_key_only_earlier_copy_flagged(self):
        df = pd.DataFrame([rec(source_updated_at="2026-01-01T00:00:00Z"), rec(source_updated_at="2026-02-01T00:00:00Z")])
        out, _, _ = self.eng.run(df, "t")
        flags = [("DQ050" in s) for s in out["_dq_failures"]]
        self.assertEqual(flags, [True, False])

    def test_new_rule_added_by_config_only(self):
        cfg = {"rules": [{"id": "X1", "name": "gmail_only", "type": "expression", "expression": "email is None or email.endswith('@gmail.com')",
                          "severity": "warning"}]}
        out, _, _ = DataQualityEngine(cfg, CFG.reference).run(pd.DataFrame([rec()]), "t")
        self.assertEqual(out.iloc[0]["_dq_status"], "WARNING")

    def test_expression_sandbox_rejects_code(self):
        for bad in ["__import__('os').system('x')", "open('/etc/passwd')", "email.__class__"]:
            with self.assertRaises(UnsafeExpression):
                compile_expression(bad)


if __name__ == "__main__":
    unittest.main()
