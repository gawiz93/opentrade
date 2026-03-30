"""
Schema validation for trade records.
"""

import re

VALID_ISO3 = re.compile(r"^[A-Z]{3}$")
VALID_HS   = re.compile(r"^(\d{2,10}|TOTAL|ALL)$")


def validate_trade_record(r: dict) -> list[str]:
    errors = []
    if not r.get("reporter") or not VALID_ISO3.match(r.get("reporter", "")):
        errors.append(f"Invalid reporter: {r.get('reporter')}")
    if not r.get("partner"):
        errors.append("Missing partner")
    if not r.get("hs_code") or not VALID_HS.match(str(r.get("hs_code", ""))):
        errors.append(f"Invalid hs_code: {r.get('hs_code')}")
    if not r.get("year") or not (1962 <= int(r.get("year", 0)) <= 2030):
        errors.append(f"Invalid year: {r.get('year')}")
    if r.get("flow") not in ("export", "import"):
        errors.append(f"Invalid flow: {r.get('flow')}")
    return errors


def validate_tariff_record(r: dict) -> list[str]:
    errors = []
    if not r.get("importer") or not VALID_ISO3.match(r.get("importer", "")):
        errors.append(f"Invalid importer: {r.get('importer')}")
    if not r.get("hs_code"):
        errors.append("Missing hs_code")
    if r.get("rate_pct") is not None and not (0 <= float(r["rate_pct"]) <= 3000):
        errors.append(f"Suspicious tariff rate: {r.get('rate_pct')}")
    return errors
