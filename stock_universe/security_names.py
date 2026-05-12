"""Security-name helpers for issue-level identity checks."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

DISTINCT_ISSUE_WORDS = {
    "debenture",
    "depositary",
    "note",
    "notes",
    "preference",
    "preferred",
    "right",
    "series",
    "unit",
    "warrant",
}

_SERIES_RE = re.compile(r"\b(?:series|ser)\.?\s+([A-Za-z0-9]+)\b", re.IGNORECASE)
_COUPON_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*%", re.IGNORECASE)


def distinct_issue_terms(value: str) -> set[str]:
    tokens = set(name_tokens(value))
    terms = tokens & DISTINCT_ISSUE_WORDS
    if "senior" in tokens and ("note" in tokens or "notes" in tokens):
        terms.add("senior_notes")
    series = series_designator(value)
    if series:
        terms.add(f"series:{series}")
    coupon = coupon_percent(value)
    if coupon:
        terms.add(f"coupon:{coupon}")
    return terms


def preferred_issue_terms_compatible(left: str, right: str) -> bool:
    left_tokens = set(name_tokens(left))
    right_tokens = set(name_tokens(right))
    if not (
        {"preferred", "preference"} & left_tokens
        and {"preferred", "preference"} & right_tokens
    ):
        return False
    left_series = series_designator(left)
    right_series = series_designator(right)
    if (left_series or right_series) and left_series != right_series:
        return False
    left_coupon = coupon_percent(left)
    right_coupon = coupon_percent(right)
    if (left_coupon or right_coupon) and left_coupon != right_coupon:
        return False
    issue_terms = {
        "cumulative",
        "depositary",
        "perpetual",
        "preference",
        "preferred",
        "redeemable",
        "series",
        "stock",
    }
    return len((left_tokens & right_tokens) & issue_terms) >= 3


def distinct_issue_terms_match(left: str, right: str) -> bool:
    required = distinct_issue_terms(left)
    if not required:
        return False
    candidate = distinct_issue_terms(right)
    specific_required = {term for term in required if ":" in term}
    if specific_required:
        return specific_required <= candidate
    broad_required = required & DISTINCT_ISSUE_WORDS
    return bool(broad_required and broad_required <= candidate)


def series_designator(value: str) -> str:
    matches = [match.lower() for match in _SERIES_RE.findall(str(value or ""))]
    return matches[-1] if matches else ""


def coupon_percent(value: str) -> str:
    match = _COUPON_RE.search(str(value or ""))
    if not match:
        return ""
    return _normalize_decimal(match.group(1))


def name_tokens(value: str) -> tuple[str, ...]:
    normalized = "".join(
        char.lower() if char.isalnum() else " " for char in str(value or "")
    )
    return tuple(normalized.split())


def normalized_security_name(value: str) -> str:
    return " ".join(str(value or "").upper().replace(",", " ").split())


def _normalize_decimal(value: str) -> str:
    try:
        normalized = Decimal(value).normalize()
    except InvalidOperation:
        return value
    return format(normalized, "f")
