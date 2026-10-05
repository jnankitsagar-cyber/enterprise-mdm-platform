"""Standardization function registry.

Every function has the signature ``fn(value, ctx) -> value`` and is *pure*
(no I/O, no global state) so it can be:
  * unit-tested in isolation,
  * composed from YAML (config/standardization.yaml),
  * wrapped as a Spark pandas_udf without modification (see spark/jobs).

Raise ``StandardizationError`` when a value cannot be standardized; the engine
sets it to null and records the failure in lineage - the DQ layer then decides
whether that is fatal.
"""
from __future__ import annotations

import re
import string
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from mdm.config import ReferenceData

REGISTRY: dict[str, Callable] = {}
DERIVE_REGISTRY: dict[str, Callable] = {}


class StandardizationError(ValueError):
    pass


@dataclass
class StdContext:
    record: dict                      # the canonical record being built (read other fields)
    ref: ReferenceData
    settings: dict                    # standardization.yaml
    date_formats: list[str] = field(default_factory=list)
    args: dict = field(default_factory=dict)


def register(name: str):
    def deco(fn):
        REGISTRY[name] = fn
        return fn
    return deco


def derive(name: str):
    def deco(fn):
        DERIVE_REGISTRY[name] = fn
        return fn
    return deco


def _is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "") or (isinstance(v, float) and v != v)


# --------------------------------------------------------------------- strings
@register("trim")
def trim(v, ctx):
    return None if _is_blank(v) else str(v).strip()


@register("lower")
def lower(v, ctx):
    return None if v is None else str(v).lower()


@register("upper")
def upper(v, ctx):
    return None if v is None else str(v).upper()


@register("collapse_whitespace")
def collapse_whitespace(v, ctx):
    return None if v is None else re.sub(r"\s+", " ", str(v)).strip() or None


_PUNCT = "".join(c for c in string.punctuation if c not in "-'")


@register("strip_punctuation")
def strip_punctuation(v, ctx):
    """Remove punctuation except hyphen/apostrophe (O'Brien, Smith-Jones)."""
    if v is None:
        return None
    return str(v).translate(str.maketrans("", "", _PUNCT)).strip() or None


@register("title_case")
def title_case(v, ctx):
    """Name-aware title case: o'brien -> O'Brien, mcdonald -> McDonald, smith-jones -> Smith-Jones."""
    if v is None:
        return None

    def fix(word: str) -> str:
        w = word.lower().capitalize()
        w = re.sub(r"(?<=['-])(\w)", lambda m: m.group(1).upper(), w)
        if re.match(r"^Mc\w", w):
            w = "Mc" + w[2:].capitalize()
        return w

    return " ".join(fix(p) for p in str(v).split(" "))


# ----------------------------------------------------------------------- email
@register("fix_email_domain_typos")
def fix_email_domain_typos(v, ctx):
    if v is None or "@" not in v:
        return v
    local, domain = v.rsplit("@", 1)
    domain = ctx.settings.get("email_domain_typos", {}).get(domain, domain)
    return f"{local}@{domain}"


@register("null_placeholder_email")
def null_placeholder_email(v, ctx):
    """Placeholder emails (noemail@...) would create false matches -> null them."""
    if v is None:
        return None
    for p in ctx.settings.get("placeholders", {}).get("emails", []):
        if v.startswith(p) or v == p:
            return None
    return v


# ----------------------------------------------------------------------- phone
@register("phone_e164")
def phone_e164(v, ctx):
    """Normalize to E.164 (+<country><national>) using reference calling codes.

    Uses the record's country (standardized earlier) as the default region,
    handles '+', '00' international prefix, trunk '0' and extensions.
    """
    if _is_blank(v):
        return None
    s = str(v).strip()
    s = re.split(r"(?i)\s*(?:ext\.?|x|#)\s*\d+$", s)[0]           # drop extension
    has_plus = s.startswith("+")
    digits = re.sub(r"\D", "", s)
    if not digits:
        raise StandardizationError(f"no digits in phone '{v}'")
    codes = {r["code"]: r for r in ctx.ref.tables.get("calling_codes", [])}
    by_cc = {}
    for r in codes.values():
        by_cc.setdefault(r["calling_code"], r)
    if digits.startswith("00"):
        digits, has_plus = digits[2:], True
    if has_plus:
        for cc_len in (1, 2, 3):
            cc = digits[:cc_len]
            if cc in by_cc:
                lengths = [int(x) for x in by_cc[cc]["national_lengths"].split("|")]
                nat = digits[cc_len:]
                if len(nat) in lengths:
                    return f"+{cc}{nat}"
        raise StandardizationError(f"unrecognised international number '{v}'")
    country = ctx.record.get(ctx.args.get("country_field", "country_code")) or ctx.args.get("default_country", "US")
    meta = codes.get(country) or codes.get(ctx.args.get("default_country", "US"))
    cc, trunk = meta["calling_code"], meta["trunk_prefix"]
    lengths = [int(x) for x in meta["national_lengths"].split("|")]
    if len(digits) in lengths:
        return f"+{cc}{digits}"
    if digits.startswith(cc) and len(digits) - len(cc) in lengths:
        return f"+{digits}"
    if trunk and digits.startswith(trunk) and len(digits) - len(trunk) in lengths:
        return f"+{cc}{digits[len(trunk):]}"
    raise StandardizationError(f"invalid phone length for {country}: '{v}'")


@register("null_placeholder_phone")
def null_placeholder_phone(v, ctx):
    if v is None:
        return None
    for p in ctx.settings.get("placeholders", {}).get("phones", []):
        if v.endswith(p):
            return None
    return v


# ------------------------------------------------------------------ dates
_COMMON_DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d %b %Y", "%b %d %Y", "%d-%b-%Y", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"]


@register("parse_date")
def parse_date(v, ctx):
    """Source-declared formats first (avoids 03/04 ambiguity), then common formats."""
    if _is_blank(v):
        return None
    s = str(v).strip()[:10] if re.match(r"^\d{4}-\d{2}-\d{2}T", str(v)) else str(v).strip()
    for fmt in list(ctx.date_formats) + _COMMON_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    raise StandardizationError(f"unparseable date '{v}'")


@register("parse_timestamp")
def parse_timestamp(v, ctx):
    if _is_blank(v):
        return None
    s = str(v).strip().replace("Z", "+00:00")
    for parser in (datetime.fromisoformat,
                   lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
                   lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M"),
                   lambda x: datetime.strptime(x, "%Y-%m-%d")):
        try:
            dt = parser(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    raise StandardizationError(f"unparseable timestamp '{v}'")


# -------------------------------------------------------------- reference data
@register("map_reference")
def map_reference(v, ctx):
    """Map any alias to its canonical code via a reference table (e.g. 'U.S.A.' -> 'US')."""
    if v is None:
        return None
    cond = ctx.args.get("only_when")
    if cond and any(ctx.record.get(k) != val for k, val in cond.items()):
        return v
    lookup = ctx.ref.lookup(ctx.args["table"])
    key = str(v).strip().upper()
    if key in lookup:
        return lookup[key]
    if "default" in ctx.args:
        if ctx.args["default"] is None:
            raise StandardizationError(f"'{v}' not in reference table {ctx.args['table']}")
        return ctx.args["default"]
    return v


@register("controlled_vocabulary")
def controlled_vocabulary(v, ctx):
    if _is_blank(v):
        return None
    vocab = ctx.settings["vocabularies"][ctx.args["vocabulary"]]
    key = str(v).strip().lower()
    for canonical, variants in vocab.items():
        if key == canonical.lower() or key in [x.lower() for x in variants]:
            return canonical
    return str(v).strip().upper()          # unknown -> kept, DQ domain rule flags it


@register("postal_code")
def postal_code(v, ctx):
    if _is_blank(v):
        return None
    s = str(v).strip().upper()
    country = ctx.record.get(ctx.args.get("country_field", "country_code"))
    if country == "US":
        d = re.sub(r"\D", "", s)
        if len(d) in (4, 8):          # leading zero lost by Excel / integer column
            d = "0" + d
        return d[:5] if len(d) >= 5 else d
    if country in ("GB", "CA"):
        s = re.sub(r"\s+", "", s)
        return f"{s[:-3]} {s[-3:]}" if len(s) > 3 else s
    return re.sub(r"\s+", "", s)


_ADDR_ABBR = {
    "STREET": "ST", "STR": "ST", "AVENUE": "AVE", "AV": "AVE", "ROAD": "RD", "BOULEVARD": "BLVD",
    "DRIVE": "DR", "LANE": "LN", "COURT": "CT", "PLACE": "PL", "SUITE": "STE", "APARTMENT": "APT",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W", "HIGHWAY": "HWY", "PARKWAY": "PKWY",
}


@register("address_abbreviations")
def address_abbreviations(v, ctx):
    """USPS-style suffix abbreviations so '12 Main Street' == '12 MAIN ST'."""
    if v is None:
        return None
    tokens = re.sub(r"[.,#]", " ", v).split()
    return " ".join(_ADDR_ABBR.get(t, t) for t in tokens) or None


# ---------------------------------------------------------------- derivations
_SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def _pop_suffix(tokens: list[str]) -> tuple[list[str], str | None]:
    if tokens and tokens[-1].upper().strip(".") in _SUFFIXES:
        return tokens[:-1], tokens[-1].upper().strip(".")
    return tokens, None


@derive("split_full_name")
def split_full_name(rec: dict, ctx) -> list[tuple[str, object, object]]:
    """'SMITH, JOHN A' or 'John A. Smith Jr' -> first/last/suffix (only fills blanks)."""
    changes = []
    full = rec.get("full_name")
    if _is_blank(full) or (rec.get("first_name") and rec.get("last_name")):
        return changes
    full = str(full).strip()
    if "," in full:
        last, rest = [p.strip() for p in full.split(",", 1)]
        rest_t, suffix = _pop_suffix(rest.split())
        last_t, suffix2 = _pop_suffix(last.split())
        first = rest_t[0] if rest_t else None
        last = " ".join(last_t)
        suffix = suffix or suffix2
    else:
        toks, suffix = _pop_suffix(full.split())
        first = toks[0] if toks else None
        last = toks[-1] if len(toks) > 1 else None
    for k, val in (("first_name", first), ("last_name", last), ("name_suffix", suffix)):
        if val and _is_blank(rec.get(k)):
            changes.append((k, rec.get(k), val))
            rec[k] = val
    return changes


@derive("extract_name_suffix")
def extract_name_suffix(rec: dict, ctx) -> list:
    changes = []
    ln = rec.get("last_name")
    if ln and not rec.get("name_suffix"):
        toks, suffix = _pop_suffix(str(ln).replace(",", " ").split())
        if suffix:
            changes.append(("name_suffix", None, suffix))
            changes.append(("last_name", ln, " ".join(toks)))
            rec["name_suffix"], rec["last_name"] = suffix, " ".join(toks)
    return changes


@derive("fill_full_name")
def fill_full_name(rec: dict, ctx) -> list:
    if rec.get("first_name") or rec.get("last_name"):
        new = " ".join(p for p in (rec.get("first_name"), rec.get("last_name")) if p)
        if new != rec.get("full_name"):
            old = rec.get("full_name")
            rec["full_name"] = new
            return [("full_name", old, new)]
    return []
