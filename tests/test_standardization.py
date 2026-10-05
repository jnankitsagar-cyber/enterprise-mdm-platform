import unittest

from tests.helpers import CFG
from mdm.standardization.engine import StandardizationEngine
from mdm.standardization.functions import StdContext, phone_e164, title_case, parse_date, StandardizationError


def ctx(record=None, **args):
    return StdContext(record=record or {}, ref=CFG.reference, settings=CFG.standardization, args=args)


class TestFunctions(unittest.TestCase):
    def test_title_case_handles_irish_scottish_and_hyphenated(self):
        self.assertEqual(title_case("o'brien", ctx()), "O'Brien")
        self.assertEqual(title_case("MCDONALD", ctx()), "McDonald")
        self.assertEqual(title_case("smith-jones", ctx()), "Smith-Jones")

    def test_phone_e164_variants(self):
        us = ctx({"country_code": "US"}, country_field="country_code")
        for raw in ["(617) 555-0142", "617.555.0142", "1-617-555-0142", "+1 617 555 0142", "6175550142 x12"]:
            self.assertEqual(phone_e164(raw, us), "+16175550142", raw)
        self.assertEqual(phone_e164("00919876543210", us), "+919876543210")
        gb = ctx({"country_code": "GB"}, country_field="country_code")
        self.assertEqual(phone_e164("07700 900123", gb), "+447700900123")
        with self.assertRaises(StandardizationError):
            phone_e164("12345", us)

    def test_parse_date_uses_source_format_first(self):
        c = ctx()
        c.date_formats = ["%d-%m-%Y"]
        self.assertEqual(parse_date("04-07-1985", c), "1985-07-04")   # day-first source
        c.date_formats = ["%m/%d/%Y"]
        self.assertEqual(parse_date("04/07/1985", c), "1985-04-07")   # month-first source
        with self.assertRaises(StandardizationError):
            parse_date("31/31/1980", c)


class TestEngine(unittest.TestCase):
    def test_erp_nested_record_to_canonical(self):
        eng = StandardizationEngine(CFG)
        payload = {"account_no": "B-1", "customer": {"full_name": "WILSON SR, JAMES", "birth_date": "1960-03-05"},
                   "contact": {"email": " J.Wilson@GMIAL.com ", "mobile": "+1 512 555 0111"},
                   "billing_address": {"line1": "77 Oak Lane", "city": "AUSTIN", "region": "Texas", "postcode": "73301", "country": "USA"},
                   "account_status": "C", "last_modified": "2026-01-01T00:00:00Z"}
        rec, tf, errs = eng.standardize_record("erp_billing", payload)
        self.assertEqual((rec["first_name"], rec["last_name"], rec["name_suffix"]), ("James", "Wilson", "SR"))
        self.assertEqual(rec["email"], "j.wilson@gmail.com")
        self.assertEqual(rec["phone"], "+15125550111")
        self.assertEqual(rec["state_code"], "TX")
        self.assertEqual(rec["country_code"], "US")
        self.assertEqual(rec["address_line1"], "77 OAK LN")
        self.assertEqual(rec["status"], "INACTIVE")
        self.assertEqual(errs, [])
        self.assertTrue(any(t["fn"] == "fix_email_domain_typos" for t in tf), "every change is captured for lineage")

    def test_us_zip_leading_zero_restored(self):
        rec, _, _ = StandardizationEngine(CFG).standardize_record("crm", {"cust_id": "C1", "zip": "2134", "country": "USA",
                                                                          "last_name": "X", "updated_at": "2026-01-01"})
        self.assertEqual(rec["postal_code"], "02134")

    def test_unknown_function_in_config_fails_fast(self):
        import copy
        bad = copy.copy(CFG)
        bad.standardization = {"attributes": {"email": ["does_not_exist"]}}
        with self.assertRaises(ValueError):
            StandardizationEngine(bad)


if __name__ == "__main__":
    unittest.main()
