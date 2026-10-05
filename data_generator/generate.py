"""Synthetic multi-source party data with realistic, *labelled* data-quality problems.

Why synthetic + labelled?
  Every generated source record carries the id of the real-world person it
  represents (data/ground_truth.csv, never read by the pipeline). That lets us
  *measure* identity resolution - pairwise precision / recall / F1 - instead of
  eyeballing it (`python -m mdm evaluate`).

Four source systems, four formats, four "dialects":
  crm          CSV        MM/DD/YYYY dates, '(555) 123-4567' phones, 'USA' / state names
  erp_billing  JSON       nested, 'LAST, FIRST' names, ISO dates, '+1 555 123 4567'
  ecommerce    SQLite DB  DD-MM-YYYY dates, typo'd email domains, is_active 1/0
  partner_api  REST(mock) 'First Last' names, YYYYMMDD dates, paginated

Injected issues (each maps to a platform capability it validates):
  duplicates within and across sources ........ identity resolution
  nicknames (Bob/Robert), typos, case noise .... fuzzy + nickname matching
  inconsistent formats (dates, phones, country)  standardization
  missing values ............................... non-null survivorship, completeness DQ
  conflicting attributes (stale phone/email) ... survivorship (most_recent / most_frequent)
  invalid values (bad email, future timestamp) . data-quality rules / quarantine
  father/son & Jr/Sr with same address ......... false-positive vetoes
  day/month transposed DOB ..................... transposition-aware comparator
  malformed CSV row, JSON missing PK ........... dead-letter queue + reconciliation
  day-2 changes (moves, new emails, new column)  incremental load, schema drift, golden versioning
  day-2 bridging record ........................ golden-record MERGE + ID redirect
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

FIRST = ["Robert", "William", "Elizabeth", "Katherine", "James", "Michael", "David", "Thomas", "Daniel", "Christopher",
         "Matthew", "Nicholas", "Anthony", "Joseph", "Jennifer", "Samuel", "Alexander", "Andrew", "Steven", "Margaret",
         "Susan", "Patricia", "Richard", "Edward", "Gregory", "Benjamin", "Olivia", "Emma", "Sophia", "Ava", "Mia",
         "Isabella", "Charlotte", "Amelia", "Harper", "Evelyn", "Liam", "Noah", "Ethan", "Lucas", "Mason", "Logan",
         "Priya", "Ananya", "Rahul", "Arjun", "Vikram", "Sneha", "Rajesh", "Aisha", "Fatima", "Omar", "Chen", "Wei",
         "Hiroshi", "Yuki", "Carlos", "Sofia", "Diego", "Lucia", "Grace", "Hannah", "Chloe", "Zoe", "Nora", "Leah"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
        "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson",
        "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Sharma", "Patel", "Iyer", "Reddy",
        "Gupta", "Khan", "O'Brien", "McDonald", "Smith-Jones", "Okafor", "Kowalski", "Nakamura", "Rossi", "Dubois"]
NICK = {"Robert": ["Bob", "Rob"], "William": ["Bill", "Will"], "Elizabeth": ["Liz", "Beth"], "Katherine": ["Kate", "Katie"],
        "James": ["Jim", "Jimmy"], "Michael": ["Mike"], "David": ["Dave"], "Thomas": ["Tom"], "Daniel": ["Dan"],
        "Christopher": ["Chris"], "Matthew": ["Matt"], "Nicholas": ["Nick"], "Anthony": ["Tony"], "Joseph": ["Joe"],
        "Jennifer": ["Jen"], "Samuel": ["Sam"], "Alexander": ["Alex"], "Andrew": ["Andy"], "Steven": ["Steve"],
        "Margaret": ["Maggie", "Peggy"], "Susan": ["Sue"], "Patricia": ["Pat"], "Richard": ["Rick", "Rich"],
        "Edward": ["Ed", "Eddie"], "Gregory": ["Greg"], "Benjamin": ["Ben"], "Rajesh": ["Raj"]}
US_CITIES = [("Boston", "MA", "Massachusetts", "02134"), ("Austin", "TX", "Texas", "73301"), ("Seattle", "WA", "Washington", "98101"),
             ("Chicago", "IL", "Illinois", "60601"), ("Denver", "CO", "Colorado", "80202"), ("Miami", "FL", "Florida", "33101"),
             ("Phoenix", "AZ", "Arizona", "85001"), ("Portland", "OR", "Oregon", "97201"), ("Atlanta", "GA", "Georgia", "30301"),
             ("New York", "NY", "New York", "10001"), ("San Jose", "CA", "California", "95101"), ("Newark", "NJ", "New Jersey", "07101")]
IN_CITIES = [("Mumbai", "MH", "400001"), ("Bengaluru", "KA", "560001"), ("Pune", "MH", "411001"), ("Chennai", "TN", "600001")]
GB_CITIES = [("London", "LDN", "SW1A 1AA"), ("Manchester", "MAN", "M1 1AE"), ("Leeds", "LDS", "LS1 4AP")]
STREETS = ["Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Lake", "Hill", "Park", "Sunset", "River", "Church", "High"]
STREET_TYPES = [("Street", ["St", "St.", "Street", "STREET"]), ("Avenue", ["Ave", "Avenue", "Av"]), ("Road", ["Rd", "Road"]),
                ("Drive", ["Dr", "Drive"]), ("Lane", ["Ln", "Lane"])]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
COUNTRY_VARIANTS = {"US": ["US", "USA", "United States", "U.S.A.", "united states of america"], "IN": ["IN", "India", "IND"],
                    "GB": ["GB", "UK", "United Kingdom", "England"]}


@dataclass
class Person:
    pid: str
    first: str
    last: str
    dob: date
    email: str
    phone_nat: str           # national significant number
    country: str
    street_no: int
    street: str
    street_type: str
    city: str
    state: str
    state_name: str
    postal: str
    suffix: str | None = None
    status: str = "ACTIVE"
    middle: str | None = None
    tags: list = field(default_factory=list)


class Gen:
    def __init__(self, seed: int, today: date):
        self.r = random.Random(seed)
        self.today = today
        self.ids = {"crm": 100000, "erp_billing": 500000, "ecommerce": 9000, "partner_api": 70000}

    def next_id(self, src: str) -> str:
        self.ids[src] += self.r.randint(1, 7)
        n = self.ids[src]
        return {"crm": f"C{n}", "erp_billing": f"B-{n}", "ecommerce": str(n), "partner_api": f"LM{n}"}[src]

    # ------------------------------------------------------------ people
    def person(self, i: int, **over) -> Person:
        r = self.r
        country = r.choices(["US", "IN", "GB"], [0.8, 0.12, 0.08])[0]
        first, last = r.choice(FIRST), r.choice(LAST)
        dob = date(1950, 1, 1) + timedelta(days=r.randint(0, 365 * 52))
        sep = r.choice([".", "_", ""])
        email = f"{first.lower()}{sep}{last.lower().replace(chr(39), '')}{r.randint(1, 999)}@{r.choice(DOMAINS)}"
        if country == "US":
            city, st, st_name, postal = r.choice(US_CITIES)
            phone = f"{r.choice([206, 312, 415, 512, 617, 303, 305, 404, 212, 408])}{r.randint(200, 999)}{r.randint(1000, 9999)}"
        elif country == "IN":
            city, st, postal = r.choice(IN_CITIES)
            st_name = st
            phone = f"{r.choice([9, 8, 7])}{r.randint(100000000, 999999999)}"
        else:
            city, st, postal = r.choice(GB_CITIES)
            st_name = st
            phone = f"7{r.randint(100000000, 999999999)}"
        p = Person(f"P{i:05d}", first, last, dob, email, phone, country, r.randint(1, 9999), r.choice(STREETS),
                   r.choice(STREET_TYPES)[0], city, st, st_name, postal,
                   status=r.choices(["ACTIVE", "INACTIVE", "PROSPECT"], [0.8, 0.15, 0.05])[0])
        return replace(p, **over)

    # ------------------------------------------------------------ noise helpers
    def typo(self, s: str, p: float) -> str:
        if not s or len(s) < 4 or self.r.random() > p:
            return s
        i = self.r.randint(1, len(s) - 2)
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]

    def case_noise(self, s: str) -> str:
        return self.r.choice([s, s, s.upper(), s.lower(), f"  {s} "])

    def maybe(self, v, p_missing: float):
        return None if self.r.random() < p_missing else v

    def street_variant(self, p: Person) -> str:
        variants = dict(STREET_TYPES)[p.street_type]
        return f"{p.street_no} {p.street} {self.r.choice(variants)}"

    def ts(self, days_ago_lo: int, days_ago_hi: int) -> datetime:
        d = self.r.randint(days_ago_lo, days_ago_hi)
        ts = datetime.combine(self.today - timedelta(days=d), datetime.min.time(), tzinfo=timezone.utc) + timedelta(
            seconds=self.r.randint(0, 86399))
        return min(ts, datetime.now(timezone.utc) - timedelta(minutes=5))      # never in the future

    # ------------------------------------------------------------ phone dialects
    def phone_fmt(self, p: Person, style: str) -> str:
        n = p.phone_nat
        cc = {"US": "1", "IN": "91", "GB": "44"}[p.country]
        if p.country == "US":
            return {"crm": self.r.choice([f"({n[:3]}) {n[3:6]}-{n[6:]}", f"{n[:3]}.{n[3:6]}.{n[6:]}", f"{n[:3]}-{n[3:6]}-{n[6:]}"]),
                    "erp": f"+1 {n[:3]} {n[3:6]} {n[6:]}",
                    "ecom": self.r.choice([n, f"1-{n[:3]}-{n[3:6]}-{n[6:]}", f"{n} x{self.r.randint(10, 99)}"]),
                    "api": f"+1{n}"}[style]
        return {"crm": f"0{n[:5]} {n[5:]}", "erp": f"+{cc} {n[:5]} {n[5:]}", "ecom": f"00{cc}{n}", "api": f"+{cc}{n}"}[style]

    # ------------------------------------------------------------ source renderers
    def crm(self, p: Person, ts: datetime, rid: str | None = None) -> dict:
        r = self.r
        first = p.first if r.random() > 0.15 else r.choice(NICK.get(p.first, [p.first]))
        last = p.last + (f" {p.suffix.title()}" if p.suffix else "")
        return {
            "cust_id": rid or self.next_id("crm"),
            "first_name": self.case_noise(first),
            "last_name": self.case_noise(self.typo(last, 0.06)),
            "email": self.maybe(p.email if r.random() > 0.05 else p.email.upper(), 0.08),
            "phone": self.maybe(self.phone_fmt(p, "crm"), 0.10),
            "dob": self.maybe(p.dob.strftime("%m/%d/%Y"), 0.10),
            "address": self.street_variant(p),
            "city": self.case_noise(p.city),
            "state": r.choice([p.state, p.state_name]) if p.country == "US" else p.state,
            "zip": p.postal.lstrip("0") if (p.country == "US" and r.random() < 0.3) else p.postal,   # Excel ate the zero
            "country": r.choice(COUNTRY_VARIANTS[p.country]),
            "status": r.choice({"ACTIVE": ["Active", "A", "active"], "INACTIVE": ["Inactive", "I"], "PROSPECT": ["Lead", "Prospect"]}[p.status]),
            "updated_at": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def erp(self, p: Person, ts: datetime, rid: str | None = None) -> dict:
        r = self.r
        last = p.last.upper() + (f" {p.suffix}" if p.suffix else "")
        full = f"{last}, {p.first.upper()}" + (f" {p.middle[0]}" if p.middle else "")
        return {
            "account_no": rid or self.next_id("erp_billing"),
            "customer": {"full_name": full, "birth_date": p.dob.isoformat()},
            "contact": {"email": self.maybe(p.email, 0.05), "mobile": self.maybe(self.phone_fmt(p, "erp"), 0.05)},
            "billing_address": {"line1": f"{p.street_no} {p.street} {p.street_type}".upper(), "city": p.city.upper(),
                                "region": p.state, "postcode": p.postal, "country": p.country},
            "account_status": {"ACTIVE": "A", "INACTIVE": "C", "PROSPECT": "P"}[p.status],
            "last_modified": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    def ecom(self, p: Person, ts: datetime, rid: str | None = None) -> dict:
        r = self.r
        email = p.email
        if r.random() < 0.07:
            email = email.replace("gmail.com", "gmial.com").replace("hotmail.com", "hotmial.com")
        return {
            "user_id": rid or self.next_id("ecommerce"),
            "fname": self.typo(r.choice([p.first] * 3 + NICK.get(p.first, [])), 0.05),
            "lname": self.typo(p.last, 0.08),
            "email_addr": self.maybe(email, 0.05),
            "phone_no": self.maybe(self.phone_fmt(p, "ecom"), 0.15),
            "date_of_birth": self.maybe(p.dob.strftime("%d-%m-%Y"), 0.35),
            "street": self.street_variant(p),
            "city": p.city,
            "state_code": p.state,
            "postal": p.postal,
            "country_name": r.choice(COUNTRY_VARIANTS[p.country]),
            "is_active": "1" if p.status == "ACTIVE" else "0",
            "modified_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def api(self, p: Person, ts: datetime, rid: str | None = None) -> dict:
        r = self.r
        return {
            "member_id": rid or self.next_id("partner_api"),
            "name": f"{r.choice([p.first] + NICK.get(p.first, []))} {p.last}",
            "email": self.maybe(p.email.lower(), 0.10),
            "phone": self.maybe(self.phone_fmt(p, "api"), 0.20),
            "dob": self.maybe(p.dob.strftime("%Y%m%d"), 0.25),
            "address": {"street": self.street_variant(p), "city": p.city, "state": p.state, "zip": p.postal},
            "country": p.country,
            "status": p.status.lower(),
            "updated": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


# ---------------------------------------------------------------------- writers
def write_csv(path: Path, rows: list[dict], extra_raw_lines: list[str] = (), declared_delta: int = 0):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for row in rows:
            w.writerow({k: ("" if v is None else v) for k, v in row.items()})
        for line in extra_raw_lines:
            fh.write(line + "\n")
    ctl = {"record_count": len(rows) + len(extra_raw_lines) + declared_delta, "file": path.name}
    path.with_suffix(path.suffix + ".ctl").write_text(json.dumps(ctl))


def write_json(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=1))
    path.with_suffix(path.suffix + ".ctl").write_text(json.dumps({"record_count": len(rows), "file": path.name}))


def upsert_db(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    cols = list(rows[0].keys())
    con.execute(f"CREATE TABLE IF NOT EXISTS users ({', '.join(c + ' TEXT' for c in cols)}, PRIMARY KEY(user_id))")
    con.executemany(f"INSERT OR REPLACE INTO users ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                    [tuple(r[c] for c in cols) for r in rows])
    con.commit()
    con.close()


def write_api_page(dirpath: Path, page_no: int, rows: list[dict]):
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / f"page_{page_no:03d}.json").write_text(json.dumps({"data": rows}, indent=1))


# ---------------------------------------------------------------------- scenarios
def day1(g: Gen, n: int, landing: Path, truth: list):
    r = g.r
    people = [g.person(i) for i in range(1, n + 1)]

    # --- hand-crafted edge cases (stable ids so docs/tests can reference them) ---
    kj = g.person(90001, first="Katherine", last="Johnson", dob=date(1988, 2, 14), email="k.johnson88@gmail.com",
                  phone_nat="6175550142", country="US", city="Boston", state="MA", state_name="Massachusetts", postal="02134",
                  street_no=12, street="Beacon", street_type="Street", tags=["bridge_merge"])
    dg_son = g.person(90002, first="Daniel", last="Garcia", dob=date(1991, 4, 12), email="dan.garcia91@gmail.com",
                      phone_nat="3125550199", country="US", city="Chicago", state="IL", state_name="Illinois", postal="60601",
                      street_no=455, street="Lake", street_type="Drive", tags=["father_son"])
    dg_dad = replace(dg_son, pid="P90003", dob=date(1962, 8, 30), email="dgarcia62@outlook.com")
    jw_jr = g.person(90004, first="James", last="Wilson", suffix="JR", dob=date(1990, 1, 1), email="jim.wilson.jr@yahoo.com",
                     phone_nat="5125550111", country="US", city="Austin", state="TX", state_name="Texas", postal="73301",
                     street_no=77, street="Oak", street_type="Lane", tags=["jr_sr"])
    jw_sr = replace(jw_jr, pid="P90005", suffix="SR", dob=date(1960, 3, 5), email="jwilson.sr@yahoo.com")
    dob_tx = g.person(90006, first="Priya", last="Sharma", dob=date(1985, 4, 7), email="priya.sharma85@gmail.com",
                      phone_nat="2065550123", country="US", city="Seattle", state="WA", state_name="Washington", postal="98101",
                      tags=["dob_transposed"])
    hh_a = g.person(90007, first="Mark", last="Thompson", dob=date(1979, 6, 2), email="thompson.family@gmail.com",
                    phone_nat="3035550177", country="US", city="Denver", state="CO", state_name="Colorado", postal="80202",
                    street_no=19, street="Pine", street_type="Road", tags=["household"])
    hh_b = replace(hh_a, pid="P90008", first="Linda", dob=date(1981, 11, 23), phone_nat="3035550178")
    special = [kj, dg_son, dg_dad, jw_jr, jw_sr, dob_tx, hh_a, hh_b]

    crm, erp, ecom, api = [], [], [], []
    def add(src_list, src, rec_fn, p, rid_field):
        rec = rec_fn(p, g.ts(5, 400))
        src_list.append(rec)
        truth.append({"source_system": src, "source_record_id": rec[rid_field], "person_id": p.pid})
        return rec

    for p in people:
        memb = {"crm": r.random() < 0.6, "erp_billing": r.random() < 0.45, "ecommerce": r.random() < 0.5, "partner_api": r.random() < 0.3}
        if not any(memb.values()):
            memb["crm"] = True
        if memb["crm"]:
            add(crm, "crm", g.crm, p, "cust_id")
            if r.random() < 0.05:                       # same person keyed twice in CRM (data-entry duplicate)
                add(crm, "crm", g.crm, p, "cust_id")
        if memb["erp_billing"]:
            # conflicting attribute: ERP has an old phone for some people
            q = replace(p, phone_nat=p.phone_nat[:-4] + f"{r.randint(1000, 9999)}") if r.random() < 0.1 else p
            add(erp, "erp_billing", g.erp, q, "account_no")
        if memb["ecommerce"]:
            if r.random() < 0.04 and sum(memb.values()) > 1:
                # stale account: old address, old email, no phone/DOB -> only the name agrees (hard case)
                old = replace(p, street_no=r.randint(1, 9999), street=r.choice(STREETS), email=f"{p.first.lower()}{r.randint(1, 99)}@aol.com",
                              postal=r.choice(US_CITIES)[3])
                e = add(ecom, "ecommerce", g.ecom, old, "user_id")
                e.update(phone_no=None, date_of_birth=None)
            else:
                add(ecom, "ecommerce", g.ecom, p, "user_id")
        if memb["partner_api"]:
            add(api, "partner_api", g.api, p, "member_id")

    # bridge case: CRM + ecom versions that are too different to match on day 1
    add(crm, "crm", g.crm, kj, "cust_id")
    kj_ecom = g.ecom(replace(kj, first="Kate", email="kate.j@yahoo.com", phone_nat="6175550999", street_no=88,
                             street="Harbor", street_type="Road", postal="02110"), g.ts(30, 60))
    kj_ecom.update(date_of_birth=None, fname="Kate", lname="Johnson", email_addr="kate.j@yahoo.com", phone_no="617-555-0999")
    ecom.append(kj_ecom)
    truth.append({"source_system": "ecommerce", "source_record_id": kj_ecom["user_id"], "person_id": kj.pid})
    # father / son: same address + phone, different DOB
    add(crm, "crm", g.crm, dg_son, "cust_id")
    add(erp, "erp_billing", g.erp, dg_dad, "account_no")
    # Jr / Sr
    add(crm, "crm", g.crm, jw_jr, "cust_id")
    add(erp, "erp_billing", g.erp, jw_sr, "account_no")
    # DOB day/month transposed in ecom (data-entry error)
    add(crm, "crm", g.crm, dob_tx, "cust_id")
    e = add(ecom, "ecommerce", g.ecom, dob_tx, "user_id")
    e["date_of_birth"] = "04-07-1985"; e["fname"] = "Priya"; e["lname"] = "Sharma"

    # household sharing one email address: Mark (CRM, with DOB) and Linda (ecom, no DOB)
    add(crm, "crm", g.crm, hh_a, "cust_id")
    e = add(ecom, "ecommerce", g.ecom, hh_b, "user_id")
    e.update(fname="Linda", date_of_birth=None, email_addr="thompson.family@gmail.com")
    crm[-1].update(first_name="Mark", email="thompson.family@gmail.com")

    # --- invalid / broken records ----------------------------------------------
    bad = people[:6]
    c = add(crm, "crm", g.crm, bad[0], "cust_id"); c["last_name"] = ""                           # no last name -> quarantine
    c = add(crm, "crm", g.crm, bad[1], "cust_id"); c["email"] = None; c["phone"] = None          # not contactable -> quarantine
    c = add(crm, "crm", g.crm, bad[2], "cust_id"); c["email"] = "not-an-email"                   # invalid email -> warning
    c = add(crm, "crm", g.crm, bad[3], "cust_id")
    c["updated_at"] = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")  # future ts -> quarantine
    c = add(crm, "crm", g.crm, bad[4], "cust_id"); c["dob"] = "31/31/1980"                        # unparseable date
    c = add(crm, "crm", g.crm, bad[5], "cust_id"); c["email"] = "noemail@corp.example"           # placeholder email
    dup = dict(crm[10]); dup["updated_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    crm.append(dup)                                                                               # duplicate key in batch
    e = add(erp, "erp_billing", g.erp, bad[0], "account_no"); e["contact"]["email"] = "test@test.com"
    missing_pk = g.erp(bad[1], g.ts(10, 20)); missing_pk["account_no"] = None                     # -> dead-letter
    erp.append(missing_pk)

    write_csv(landing / "crm" / "crm_customers_day1.csv", crm,
              extra_raw_lines=['C999999,Broken,"Row,with,too,many",fields,x,y,z,1,2,3,4,5,6,7'])
    write_json(landing / "erp_billing" / "erp_accounts_day1.json", erp)
    upsert_db(landing / "ecommerce" / "ecommerce.db", ecom)
    size = 50
    for i in range(0, len(api), size):
        write_api_page(landing / "partner_api", i // size + 1, api[i:i + size])
    return {"people": people, "special": special, "crm": crm, "erp": erp, "ecom": ecom, "api": api}


def day2(g: Gen, st: dict, landing: Path, truth: list, truncated: bool = False):
    """Incremental day: moves, new emails, new people, re-sends, a schema change and a bridging record."""
    r = g.r
    pid_of = {(t["source_system"], t["source_record_id"]): t["person_id"] for t in truth}
    people = {p.pid: p for p in st["people"] + st["special"]}
    recent = lambda: g.ts(0, 1)

    # CRM: new column (schema drift) + updates + new customers
    crm_rows = []
    movers = r.sample([c for c in st["crm"] if c["cust_id"] and pid_of.get(("crm", c["cust_id"])) in people
                       and not people[pid_of[("crm", c["cust_id"])]].tags], 25)
    for old in movers:
        p = people[pid_of[("crm", old["cust_id"])]]
        if r.random() < 0.5:
            p2 = replace(p, street_no=r.randint(1, 9999), street=r.choice(STREETS))          # moved house
        else:
            p2 = replace(p, email=p.email.split("@")[0] + ".new@" + r.choice(DOMAINS))      # new email
        people[p.pid] = p2
        row = g.crm(p2, recent(), rid=old["cust_id"])
        row["loyalty_tier"] = r.choice(["GOLD", "SILVER", None])
        crm_rows.append(row)
    for i in range(20):
        p = g.person(80000 + i)
        people[p.pid] = p
        row = g.crm(p, recent())
        row["loyalty_tier"] = "BRONZE"
        crm_rows.append(row)
        truth.append({"source_system": "crm", "source_record_id": row["cust_id"], "person_id": p.pid})
    crm_rows[-1]["email"] = "broken@@mail"                     # day-2 validation failures
    crm_rows[-2]["phone"] = "12345"
    crm_rows[-3]["status"] = "VIP"                             # not in the controlled vocabulary
    write_csv(landing / "crm" / "crm_customers_day2.csv", crm_rows, declared_delta=1 if truncated else 0)

    # ERP: identical re-sends (should be detected as unchanged) + a couple of updates
    erp_rows = [dict(x) for x in r.sample([e for e in st["erp"] if e.get("account_no")], 15)]
    for e in r.sample([e for e in st["erp"] if e.get("account_no") and pid_of.get(("erp_billing", e["account_no"])) in people], 5):
        p = people[pid_of[("erp_billing", e["account_no"])]]
        erp_rows.append(g.erp(p, recent(), rid=e["account_no"]))
    write_json(landing / "erp_billing" / "erp_accounts_day2.json", erp_rows)

    # ecommerce: changed rows get a new modified_ts (picked up by the watermark) + new users
    ecom_rows = []
    for e in r.sample([e for e in st["ecom"] if pid_of.get(("ecommerce", e["user_id"])) in people
                       and not people[pid_of[("ecommerce", e["user_id"])]].tags], 12):
        p = people[pid_of[("ecommerce", e["user_id"])]]
        ecom_rows.append(g.ecom(p, recent(), rid=e["user_id"]))
    for i in range(10):
        p = g.person(81000 + i)
        row = g.ecom(p, recent())
        ecom_rows.append(row)
        truth.append({"source_system": "ecommerce", "source_record_id": row["user_id"], "person_id": p.pid})
    upsert_db(landing / "ecommerce" / "ecommerce.db", ecom_rows)

    # partner API: the bridging record (CRM email + ecom phone/address) => golden MERGE
    kj = next(p for p in st["special"] if "bridge_merge" in p.tags)
    bridge = g.api(replace(kj, phone_nat="6175550999", street_no=88, street="Harbor", street_type="Road", postal="02110"), recent())
    bridge.update(name="Katherine Johnson", email="k.johnson88@gmail.com", dob="19880214", phone="+16175550999")
    truth.append({"source_system": "partner_api", "source_record_id": bridge["member_id"], "person_id": kj.pid})
    new_api = [bridge]
    for i in range(8):
        p = g.person(82000 + i)
        row = g.api(p, recent())
        new_api.append(row)
        truth.append({"source_system": "partner_api", "source_record_id": row["member_id"], "person_id": p.pid})
    existing = sorted((landing / "partner_api").glob("page_*.json"))
    write_api_page(landing / "partner_api", len(existing) + 1, new_api)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate synthetic multi-source party data")
    ap.add_argument("--day", type=int, choices=[1, 2], required=True)
    ap.add_argument("--people", type=int, default=400)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--root", default=".")
    ap.add_argument("--simulate-truncated-file", action="store_true",
                    help="day 2: CRM control file declares one more record than delivered (reconciliation break demo)")
    a = ap.parse_args(argv)
    root = Path(a.root)
    landing, truth_path, state_path = root / "data/landing", root / "data/ground_truth.csv", root / "data/.generator_state.json"
    today = date.today()
    g = Gen(a.seed, today)
    truth: list[dict] = []
    if a.day == 1:
        st = day1(g, a.people, landing, truth)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({"seed": a.seed, "people": a.people, "today": today.isoformat()}))
    else:
        if not truth_path.exists():
            raise SystemExit("run --day 1 first")
        saved = json.loads(state_path.read_text())
        g1 = Gen(saved["seed"], date.fromisoformat(saved.get("today", today.isoformat())))               # regenerate day-1 state deterministically (no files rewritten)
        tmp_truth: list[dict] = []
        st = day1(g1, saved["people"], root / "data/.tmp_landing", tmp_truth)
        import shutil
        shutil.rmtree(root / "data/.tmp_landing", ignore_errors=True)
        with open(truth_path, encoding="utf-8") as fh:
            truth = list(csv.DictReader(fh))
        g1.r.seed(a.seed + 2)
        day2(g1, st, landing, truth, truncated=a.simulate_truncated_file)
    truth_path.parent.mkdir(parents=True, exist_ok=True)
    with open(truth_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["source_system", "source_record_id", "person_id"])
        w.writeheader()
        w.writerows(truth)
    print(f"day {a.day} data written to {landing} ({len(truth)} labelled source records)")


if __name__ == "__main__":
    main()
