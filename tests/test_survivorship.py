import json
import unittest

import pandas as pd

from tests.helpers import CFG
from mdm.survivorship.engine import SurvivorshipEngine


def m(key, src, trust, **kw):
    base = {"record_key": key, "source_system": src, "source_trust": trust, "_dq_score": 1.0, "_dq_failures": "[]",
            "first_name": None, "last_name": None, "email": None, "phone": None, "date_of_birth": None, "status": None,
            "address_line1": None, "city": None, "state_code": None, "postal_code": None, "country_code": None,
            "source_updated_at": None}
    base.update(kw)
    return base


class TestSurvivorship(unittest.TestCase):
    def setUp(self):
        self.silver = pd.DataFrame([
            m("crm:1", "crm", 0.85, first_name="Bob", last_name="Smith", email="old@x.com", date_of_birth="1980-01-01",
              address_line1="1 MAIN ST", city="Boston", postal_code="02134", country_code="US", source_updated_at="2026-01-01T00:00:00Z"),
            m("erp_billing:2", "erp_billing", 0.95, first_name="Robert", last_name="Smith", email="bad-email",
              date_of_birth="1980-01-01", address_line1="9 ELM ST", city=None, postal_code=None,
              source_updated_at="2025-01-01T00:00:00Z",
              _dq_failures=json.dumps([{"rule_id": "DQ010", "column": "email"}]), _dq_score=0.9),
            m("partner_api:3", "partner_api", 0.6, first_name="Bob", last_name="Smith", email="new@x.com", date_of_birth="1980-10-01",
              source_updated_at="2026-06-01T00:00:00Z"),
        ])
        self.xref = pd.DataFrame({"record_key": ["crm:1", "erp_billing:2", "partner_api:3"], "golden_id": ["G1"] * 3})

    def test_attribute_level_rules(self):
        g, log, hist = SurvivorshipEngine(CFG.survivorship).run(self.silver, self.xref, pd.DataFrame(), pd.DataFrame(), "r1")
        row = g.iloc[0]
        self.assertEqual(row["first_name"], "Robert")                 # source_priority: billing is KYC'd
        self.assertEqual(row["email"], "new@x.com")                   # most_recent; DQ-failed value never survives
        self.assertEqual(row["date_of_birth"], "1980-01-01")          # most_frequent (2 of 3 systems)
        self.assertEqual(row["address_line1"], "1 MAIN ST")           # most_complete address group
        self.assertEqual(row["postal_code"], "02134")                 # ... and from the SAME record (no Frankenstein)
        decided = dict(zip(log["attribute"], log["deciding_rule"]))
        self.assertEqual(decided["email"], "most_recent")
        self.assertEqual(len(hist), 1)

    def test_versioning_only_on_change(self):
        eng = SurvivorshipEngine(CFG.survivorship)
        g1, _, _ = eng.run(self.silver, self.xref, pd.DataFrame(), pd.DataFrame(), "r1")
        g2, _, h2 = eng.run(self.silver, self.xref, g1, pd.DataFrame(), "r2")
        self.assertEqual(int(g2.iloc[0]["version"]), 1)
        self.assertTrue(h2.empty)


if __name__ == "__main__":
    unittest.main()
