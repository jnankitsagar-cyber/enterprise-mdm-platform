import unittest

from tests.helpers import CFG
from mdm.matching.comparators import NicknameIndex, date_similarity, jaro_winkler, soundex
from mdm.matching.golden_ids import assign_golden_ids
from mdm.matching.scorer import MATCH, NO_MATCH, PairScorer

NICK = NicknameIndex(CFG.reference.tables["nicknames"])
SCORER = PairScorer(CFG.matching, NICK)


def r(key, **kw):
    base = dict(record_key=key, source_system=key.split(":")[0], first_name=None, last_name=None, name_suffix=None,
                email=None, phone=None, date_of_birth=None, address_line1=None, postal_code=None)
    base.update(kw)
    return base


class TestComparators(unittest.TestCase):
    def test_jaro_winkler(self):
        self.assertEqual(jaro_winkler("MARTHA", "MARTHA"), 1.0)
        self.assertAlmostEqual(jaro_winkler("MARTHA", "MARHTA"), 0.9611, places=3)

    def test_soundex(self):
        self.assertEqual(soundex("Robert"), soundex("Rupert"))
        self.assertEqual(soundex("Tymczak"), "T522")

    def test_date_transposition(self):
        self.assertEqual(date_similarity("1985-04-07", "1985-07-04"), 0.9)


class TestScorer(unittest.TestCase):
    def test_nickname_and_formatting_match(self):
        a = r("crm:1", first_name="Bob", last_name="Smith", email="bob@x.com", date_of_birth="1980-01-01", postal_code="02134")
        b = r("erp_billing:2", first_name="Robert", last_name="Smith", date_of_birth="1980-01-01", postal_code="02134",
              address_line1="12 MAIN ST")
        self.assertEqual(SCORER.score(a, b).decision, MATCH)

    def test_father_son_vetoed_despite_shared_address_and_phone(self):
        common = dict(first_name="Daniel", last_name="Garcia", phone="+13125550199", address_line1="455 LAKE DR", postal_code="60601")
        res = SCORER.score(r("crm:1", date_of_birth="1991-04-12", **common), r("erp_billing:2", date_of_birth="1962-08-30", **common))
        self.assertEqual(res.decision, NO_MATCH)
        self.assertIn("V1_dob_conflict", res.vetoes)

    def test_jr_sr_vetoed(self):
        common = dict(first_name="James", last_name="Wilson", address_line1="77 OAK LN", postal_code="73301")
        res = SCORER.score(r("crm:1", name_suffix="JR", **common), r("erp_billing:2", name_suffix="SR", **common))
        self.assertEqual(res.decision, NO_MATCH)

    def test_household_shared_email_vetoed(self):
        common = dict(last_name="Thompson", email="thompson.family@gmail.com", address_line1="19 PINE RD", postal_code="80202")
        res = SCORER.score(r("crm:1", first_name="Mark", **common), r("ecommerce:2", first_name="Linda", **common))
        self.assertEqual(res.decision, NO_MATCH)

    def test_name_only_agreement_never_auto_merges(self):
        a = r("crm:1", first_name="John", last_name="Smith")
        b = r("partner_api:2", first_name="John", last_name="Smith")
        self.assertNotEqual(SCORER.score(a, b).decision, MATCH)

    def test_evidence_is_explainable(self):
        a = r("crm:1", first_name="Ann", last_name="Lee", email="a@x.com")
        res = SCORER.score(a, r("crm:2", first_name="Ann", last_name="Lee", email="a@x.com"))
        self.assertIn("email", res.evidence)
        self.assertGreater(res.evidence["email"]["weight"], 0)


class TestGoldenIds(unittest.TestCase):
    def test_ids_are_stable_and_merges_are_logged(self):
        a1, ev1 = assign_golden_ids([{"a"}, {"b"}], {}, "r1")
        self.assertEqual({e["event"] for e in ev1}, {"CREATE"})
        a2, ev2 = assign_golden_ids([{"a", "b", "c"}], a1, "r2")
        survivor = a2["a"]
        self.assertIn(survivor, {a1["a"], a1["b"]})
        merges = [e for e in ev2 if e["event"] == "MERGE"]
        self.assertEqual(len(merges), 1)
        self.assertEqual(merges[0]["golden_id"], survivor)

    def test_split_keeps_id_for_largest_piece(self):
        prev = {"a": "G1", "b": "G1", "c": "G1"}
        a, ev = assign_golden_ids([{"a", "b"}, {"c"}], prev, "r2")
        self.assertEqual(a["a"], "G1")
        self.assertNotEqual(a["c"], "G1")
        self.assertIn("SPLIT", {e["event"] for e in ev})


if __name__ == "__main__":
    unittest.main()
