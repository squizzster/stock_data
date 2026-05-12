"""Reference-boundary helper rules for Massive providers."""

from __future__ import annotations

import urllib.parse
from typing import Any

from stock_universe.domain import BackfillRequest, ReferenceBoundaryFact, TargetIdentity
from stock_universe.domain.records import unfreeze_json
from stock_universe.evidence.normalizers import reference_boundary_fact_from_snapshot
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.payloads import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _reference_snapshot_from_payload,
)
from stock_universe.providers.models import ReferenceSnapshot
from stock_universe.security_names import distinct_issue_terms, normalized_security_name

START_GAP_BAR_SCAN_LIMIT = 260
EXCHANGE_TRADED_PRODUCT_TYPES = {"ETF", "ETN", "ETV", "ETS", "FUND"}


def _reference_is_target_match(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    return (
        reference_boundary_fact_from_snapshot(
            target.ohlcv_series_id,
            target,
            snapshot,
            point="boundary",
        ).matched
        is True
    )


def _reference_is_conclusive_non_target(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    if snapshot.api_status != "OK":
        return False
    if _historical_figi_rekey_reason(target, snapshot, "boundary"):
        return False
    if _successor_cik_rollover_reason(target, snapshot, "boundary"):
        return False
    if (
        target.composite_figi
        and snapshot.composite_figi
        and snapshot.composite_figi != target.composite_figi
    ):
        return True
    if (
        target.share_class_figi
        and snapshot.share_class_figi
        and snapshot.share_class_figi != target.share_class_figi
    ):
        return True
    if target.cik and snapshot.cik and snapshot.cik != target.cik:
        return True
    if (
        target.cik
        and snapshot.cik
        and snapshot.cik == target.cik
        and _same_cik_distinct_issue_mismatch(target, snapshot)
    ):
        return True
    if (
        target.security_type
        and snapshot.security_type
        and not _security_types_compatible_for_historical_identity(
            target.security_type, snapshot.security_type
        )
    ):
        return True
    return False


def _same_cik_distinct_issue_mismatch(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    target_terms = distinct_issue_terms(
        target.company_name or target.current_company_name
    )
    if not target_terms:
        return False
    snapshot_terms = distinct_issue_terms(_reference_name(snapshot))
    if target_terms <= snapshot_terms:
        return False
    return not _same_ticker_type_truncated_issue_name(target, snapshot)


def _same_ticker_type_truncated_issue_name(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    target_ticker = str(target.latest_ticker or "").upper()
    response_ticker = str(snapshot.response_ticker or snapshot.ticker or "").upper()
    if target_ticker and response_ticker and target_ticker != response_ticker:
        return False
    if (
        target.security_type
        and snapshot.security_type
        and target.security_type.upper() != snapshot.security_type.upper()
    ):
        return False
    target_exchange = str(target.latest_primary_exchange or "").upper()
    snapshot_exchange = str(snapshot.primary_exchange or "").upper()
    if target_exchange and snapshot_exchange and target_exchange != snapshot_exchange:
        return False
    target_name = normalized_security_name(
        target.company_name or target.current_company_name
    )
    snapshot_name = normalized_security_name(_reference_name(snapshot))
    return bool(
        target_name
        and snapshot_name
        and (
            target_name == snapshot_name
            or (len(snapshot_name) >= 40 and target_name.startswith(snapshot_name))
            or (len(target_name) >= 40 and snapshot_name.startswith(target_name))
        )
    )


def _segment_validation_row(
    reference_fact: dict[str, Any], point: str
) -> dict[str, Any]:
    payload = dict(reference_fact.get("payload") or {})
    payload["api_status"] = reference_fact.get(
        "api_status", payload.get("api_status", "")
    )
    payload["date"] = reference_fact.get("as_of_date", payload.get("date", ""))
    payload["matched"] = reference_fact.get("matched", payload.get("matched", False))
    payload["match_reason"] = reference_fact.get(
        "match_reason", payload.get("match_reason", "")
    )
    payload["point"] = point
    payload["requested_ticker"] = reference_fact.get(
        "ticker", payload.get("requested_ticker", "")
    )
    return payload


def _reference_name(snapshot: ReferenceSnapshot) -> str:
    raw = unfreeze_json(snapshot.raw)
    if isinstance(raw, dict):
        return str(raw.get("name") or "")
    return ""


def _reference_missing_durable_ids_without_contradiction(
    target: TargetIdentity,
    snapshot: ReferenceSnapshot,
    expected_ticker: str,
) -> bool:
    if snapshot.api_status != "OK" or snapshot.response_ticker != expected_ticker:
        return False
    if snapshot.composite_figi or snapshot.share_class_figi or snapshot.cik:
        return False
    if (
        target.security_type
        and snapshot.security_type
        and snapshot.security_type.upper() != target.security_type.upper()
    ):
        return False
    return True


def _first_bar_boundary_fact_after_start_gap(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    from_date: str,
    to_date: str,
) -> Any | None:
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    dates = _bar_dates_from_payload(bars_payload)
    if not dates or dates[0] <= from_date:
        return None
    anchored_suffix = _first_matching_suffix_boundary_fact(
        client,
        request,
        target,
        ticker,
        dates,
        point="start",
        source="massive.reference_start_gap_first_valid_bar_boundary",
        allow_historical_rekey=True,
    )
    if anchored_suffix is not None:
        return anchored_suffix

    # Without a matching right anchor, the ordered suffix invariant is absent.
    # Preserve legacy behavior for unusual non-monotonic provider histories.
    for date in dates[:START_GAP_BAR_SCAN_LIMIT]:
        fact = _reference_boundary_fact_for_date(
            client,
            request,
            target,
            ticker,
            date,
            point="start",
            source="massive.reference_start_gap_first_valid_bar_boundary",
            allow_historical_rekey=True,
        )
        if fact.matched is True:
            return fact
    return None


def _first_matching_suffix_boundary_fact(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    dates: tuple[str, ...],
    *,
    point: str,
    source: str,
    allow_historical_rekey: bool,
    seed_facts: dict[str, ReferenceBoundaryFact] | None = None,
) -> ReferenceBoundaryFact | None:
    if not dates:
        return None

    cache: dict[str, ReferenceBoundaryFact] = dict(seed_facts or {})
    probed_dates: list[str] = []

    def fact_for(date: str) -> ReferenceBoundaryFact:
        fact = cache.get(date)
        if fact is not None:
            return fact
        probed_dates.append(date)
        fact = _reference_boundary_fact_for_date(
            client,
            request,
            target,
            ticker,
            date,
            point=point,
            source=source,
            allow_historical_rekey=allow_historical_rekey,
        )
        cache[date] = fact
        return fact

    last_index = len(dates) - 1
    if fact_for(dates[last_index]).matched is not True:
        return None

    high = last_index
    false_low = -1
    step = 1
    while True:
        probe = last_index - step
        if probe < 0:
            break
        if fact_for(dates[probe]).matched is True:
            high = probe
            step *= 2
            continue
        false_low = probe
        break

    low = false_low + 1
    while low < high:
        midpoint = (low + high) // 2
        if fact_for(dates[midpoint]).matched is True:
            high = midpoint
        else:
            low = midpoint + 1
    candidate = fact_for(dates[low])
    if candidate.matched is not True:
        return None
    return _with_boundary_search_proof(
        candidate,
        {
            "algorithm": "anchored_final_suffix_lower_bound",
            "allow_historical_rekey": allow_historical_rekey,
            "bar_date_count": len(dates),
            "candidate_date": candidate.as_of_date.isoformat(),
            "left_non_match_date": dates[false_low] if false_low >= 0 else "",
            "probe_count": len(probed_dates),
            "probed_dates": tuple(probed_dates),
            "right_anchor_date": dates[last_index],
            "seeded_dates": tuple(sorted(cache.keys() - set(probed_dates))),
            "rule": (
                "The rightmost bar date must validate the target identity; the search locates the first "
                "validated date in that final target-valid suffix."
            ),
        },
    )


def _retag_reference_boundary_fact(
    fact: ReferenceBoundaryFact,
    *,
    point: str,
    source: str,
) -> ReferenceBoundaryFact:
    payload = dict(unfreeze_json(fact.payload))
    payload["point"] = point
    return ReferenceBoundaryFact(
        ticker=fact.ticker,
        as_of_date=fact.as_of_date,
        api_status=fact.api_status,
        matched=fact.matched,
        match_reason=fact.match_reason,
        payload=payload,
        source=source,
    )


def _reference_boundary_fact_for_date(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    date: str,
    *,
    point: str,
    source: str,
    allow_historical_rekey: bool,
) -> ReferenceBoundaryFact:
    reference_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": date},
    )
    snapshot = _reference_snapshot_from_payload(ticker, date, reference_payload)
    if allow_historical_rekey:
        return _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            snapshot,
            point=point,
            source=source,
        )
    return reference_boundary_fact_from_snapshot(
        request.series_id,
        target,
        snapshot,
        point=point,
        source=source,
    )


def _with_boundary_search_proof(
    fact: ReferenceBoundaryFact, proof: dict[str, Any]
) -> ReferenceBoundaryFact:
    payload = dict(unfreeze_json(fact.payload))
    payload["boundary_search"] = proof
    return ReferenceBoundaryFact(
        ticker=fact.ticker,
        as_of_date=fact.as_of_date,
        api_status=fact.api_status,
        matched=fact.matched,
        match_reason=fact.match_reason,
        payload=payload,
        source=fact.source,
    )


def _reference_boundary_fact_with_historical_rekey(
    series_id: int | str,
    target: TargetIdentity,
    snapshot: ReferenceSnapshot,
    *,
    point: str,
    source: str,
) -> ReferenceBoundaryFact:
    fact = reference_boundary_fact_from_snapshot(
        series_id, target, snapshot, point=point, source=source
    )
    if fact.matched is True:
        return fact
    reason = _historical_figi_rekey_reason(target, snapshot, point)
    override_kind = "provider_historical_figi_rekey"
    if not reason:
        reason = _successor_cik_rollover_reason(target, snapshot, point)
        override_kind = "provider_successor_cik_rollover"
    if not reason:
        return fact
    payload = snapshot.to_payload()
    payload["matched"] = True
    payload["match_reason"] = reason
    payload["point"] = point
    payload["validation_override"] = {
        "reason": override_kind,
        "historical_composite": snapshot.composite_figi,
        "historical_share_class_figi": snapshot.share_class_figi,
        "target_composite_figi": target.composite_figi,
        "target_share_class_figi": target.share_class_figi,
        "target_cik": target.cik,
        "ticker": snapshot.response_ticker,
    }
    return ReferenceBoundaryFact(
        ticker=snapshot.ticker,
        as_of_date=snapshot.as_of_date,
        api_status=snapshot.api_status,
        matched=True,
        match_reason=reason,
        payload=payload,
        source=source,
    )


def _successor_cik_rollover_reason(
    target: TargetIdentity, snapshot: ReferenceSnapshot, point: str
) -> str:
    if snapshot.api_status != "OK":
        return ""
    if target.latest_ticker and snapshot.response_ticker != target.latest_ticker:
        return ""
    if not target.cik or not snapshot.cik or target.cik == snapshot.cik:
        return ""
    if (
        target.composite_figi
        and snapshot.composite_figi
        and target.composite_figi != snapshot.composite_figi
    ):
        return ""
    if (
        target.share_class_figi
        and snapshot.share_class_figi
        and target.share_class_figi != snapshot.share_class_figi
    ):
        return ""
    if (
        target.security_type
        and snapshot.security_type
        and not _security_types_compatible_for_historical_identity(
            target.security_type, snapshot.security_type
        )
    ):
        return ""
    if not _same_named_same_exchange_target(target, snapshot):
        return ""
    return (
        f"provider_successor_cik_rollover_same_ticker_name_exchange_type "
        f"point={point} date={snapshot.as_of_date.isoformat()} "
        f"historical_cik={snapshot.cik} target_cik={target.cik}"
    )


def _historical_figi_rekey_reason(
    target: TargetIdentity, snapshot: ReferenceSnapshot, point: str
) -> str:
    if snapshot.api_status != "OK":
        return ""
    if target.latest_ticker and snapshot.response_ticker != target.latest_ticker:
        return ""
    if (
        target.security_type
        and snapshot.security_type
        and not _security_types_compatible_for_historical_identity(
            target.security_type, snapshot.security_type
        )
    ):
        return ""
    if not (snapshot.composite_figi or snapshot.share_class_figi):
        return ""
    if (target.composite_figi and snapshot.composite_figi == target.composite_figi) or (
        target.share_class_figi and snapshot.share_class_figi == target.share_class_figi
    ):
        return ""
    if target.cik and snapshot.cik == target.cik:
        identity_basis = "same_ticker_cik_type"
    elif not snapshot.cik and _same_named_same_exchange_target(target, snapshot):
        identity_basis = "same_ticker_name_exchange_type_missing_cik"
    else:
        return ""
    return (
        f"provider_historical_figi_rekey_{identity_basis} "
        f"point={point} date={snapshot.as_of_date.isoformat()} "
        f"historical_composite={snapshot.composite_figi} target_composite={target.composite_figi}"
    )


def _security_types_compatible_for_historical_identity(
    target_type: str | None, snapshot_type: str | None
) -> bool:
    target = str(target_type or "").upper()
    snapshot = str(snapshot_type or "").upper()
    if not target or not snapshot:
        return True
    if target == snapshot:
        return True
    return (
        target in EXCHANGE_TRADED_PRODUCT_TYPES
        and snapshot in EXCHANGE_TRADED_PRODUCT_TYPES
    )


def _same_named_same_exchange_target(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    target_name = normalized_security_name(
        target.company_name or target.current_company_name
    )
    snapshot_name = normalized_security_name(_reference_name(snapshot))
    same_name = bool(
        target_name
        and snapshot_name
        and (
            target_name == snapshot_name
            or (len(snapshot_name) >= 40 and target_name.startswith(snapshot_name))
            or (len(target_name) >= 40 and snapshot_name.startswith(target_name))
        )
    )
    if not same_name:
        return False
    target_exchange = str(target.latest_primary_exchange or "").upper()
    snapshot_exchange = str(snapshot.primary_exchange or "").upper()
    if target_exchange and snapshot_exchange and target_exchange != snapshot_exchange:
        return False
    return True
