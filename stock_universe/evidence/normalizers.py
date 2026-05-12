"""Normalize provider read models into typed evidence facts."""

from __future__ import annotations

from stock_universe.domain import (
    BarProbeFact,
    HandoffSegmentFact,
    IdentityScanFact,
    OmittedSegmentFact,
    ReferenceBoundaryFact,
    TargetIdentity,
    TickerReplacementFact,
)
from stock_universe.providers.models import (
    BarProbeResult,
    IdentityScanResult,
    ReferenceSnapshot,
)
from stock_universe.security_names import distinct_issue_terms, normalized_security_name


def reference_boundary_fact_from_snapshot(
    series_id: int | str,
    target: TargetIdentity,
    snapshot: ReferenceSnapshot,
    *,
    point: str,
    source: str = "provider.reference_snapshot",
) -> ReferenceBoundaryFact:
    matched, match_reason = _match_reference_snapshot(target, snapshot)
    payload = snapshot.to_payload()
    payload["matched"] = matched
    payload["match_reason"] = match_reason
    payload["point"] = point
    return ReferenceBoundaryFact(
        ticker=snapshot.ticker,
        as_of_date=snapshot.as_of_date,
        api_status=snapshot.api_status,
        matched=matched,
        match_reason=match_reason,
        payload=payload,
        source=source,
    )


def bar_probe_fact_from_result(
    series_id: int | str,
    result: BarProbeResult,
    *,
    source: str = "provider.bar_probe",
) -> BarProbeFact:
    return BarProbeFact(
        ticker=result.ticker,
        from_date=result.from_date,
        to_date=result.to_date,
        bar_count=result.bar_count,
        api_status=result.api_status,
        source=source,
    )


def identity_scan_fact_from_result(
    series_id: int | str,
    result: IdentityScanResult,
    *,
    source: str = "provider.identity_scan",
) -> IdentityScanFact:
    return IdentityScanFact(
        as_of_date=result.as_of_date,
        query=result.query,
        matches=result.matches,
        source=source,
    )


def omitted_segment_fact_from_absent_reference_and_bars(
    series_id: int | str,
    *,
    ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
    start_identity_scan: IdentityScanResult,
    end_identity_scan: IdentityScanResult,
    source: str = "provider.absent_reference_bars_identity_scan",
) -> OmittedSegmentFact | None:
    """Return an omitted-segment fact only when absence is explicitly proven."""
    if (
        start_reference.ticker != ticker
        or end_reference.ticker != ticker
        or bar_probe.ticker != ticker
    ):
        return None
    if (
        start_reference.as_of_date.isoformat() != from_date
        or end_reference.as_of_date.isoformat() != to_date
    ):
        return None
    if (
        bar_probe.from_date.isoformat() != from_date
        or bar_probe.to_date.isoformat() != to_date
    ):
        return None
    if start_identity_scan.query != ticker or end_identity_scan.query != ticker:
        return None
    if (
        start_identity_scan.as_of_date.isoformat() != from_date
        or end_identity_scan.as_of_date.isoformat() != to_date
    ):
        return None
    if start_identity_scan.matches or end_identity_scan.matches:
        return None
    if (
        start_reference.api_status != "NOT_FOUND"
        or end_reference.api_status != "NOT_FOUND"
    ):
        return None
    if bar_probe.bar_count != 0:
        return None
    reason = (
        "provider reference was NOT_FOUND at both boundaries, point-in-time identity scans found no target, "
        "and aggregate bars were absent for the full segment."
    )
    proof = {
        "bar_probe": {
            "api_status": bar_probe.api_status,
            "bar_count": bar_probe.bar_count,
            "from_date": bar_probe.from_date.isoformat(),
            "ticker": bar_probe.ticker,
            "to_date": bar_probe.to_date.isoformat(),
        },
        "end_identity_scan": {
            "as_of_date": end_identity_scan.as_of_date.isoformat(),
            "match_count": len(end_identity_scan.matches),
            "query": end_identity_scan.query,
        },
        "end_reference": {
            "api_status": end_reference.api_status,
            "as_of_date": end_reference.as_of_date.isoformat(),
            "ticker": end_reference.ticker,
        },
        "start_identity_scan": {
            "as_of_date": start_identity_scan.as_of_date.isoformat(),
            "match_count": len(start_identity_scan.matches),
            "query": start_identity_scan.query,
        },
        "start_reference": {
            "api_status": start_reference.api_status,
            "as_of_date": start_reference.as_of_date.isoformat(),
            "ticker": start_reference.ticker,
        },
    }
    return OmittedSegmentFact(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        reason=reason,
        source=source,
        proof=proof,
    )


def ticker_replacement_fact_from_target_valid_alias_window(
    series_id: int | str,
    target: TargetIdentity,
    *,
    old_ticker: str,
    new_ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    replacement_reason: str = "known_alias_boundary_validation",
    event_date: str | None = None,
    source_prefix: str = "ticker_events",
) -> TickerReplacementFact | None:
    """Return a replacement fact when the new alias validates at both boundaries."""
    if start_reference.ticker != new_ticker or end_reference.ticker != new_ticker:
        return None
    if (
        start_reference.as_of_date.isoformat() != from_date
        or end_reference.as_of_date.isoformat() != to_date
    ):
        return None
    start_matched, start_reason = _match_reference_snapshot(target, start_reference)
    end_matched, end_reason = _match_reference_snapshot(target, end_reference)
    if not start_matched or not end_matched:
        return None
    validation = (
        _validation_row(
            start_reference,
            matched=start_matched,
            match_reason=start_reason,
            point="start",
        ),
        _validation_row(
            end_reference, matched=end_matched, match_reason=end_reason, point="end"
        ),
    )
    metadata = {
        "ticker_replacement": {
            "new_ticker": new_ticker,
            "old_ticker": old_ticker,
            "replacement_reason": replacement_reason,
            "new_start_reason": start_reason,
            "new_end_reason": end_reason,
        }
    }
    return TickerReplacementFact(
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        from_date=from_date,
        to_date=to_date,
        replacement_reason=replacement_reason,
        source=f"{source_prefix}+{replacement_reason}",
        event_date=event_date,
        validation=validation,
        metadata=metadata,
    )


def handoff_segment_fact_from_target_valid_event_window(
    series_id: int | str,
    target: TargetIdentity,
    *,
    event_ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    candidate_ticker: str,
    event_date: str | None = None,
    source: str = "ticker_events+event_ticker_target_valid_bar_window_after_known_alias",
) -> HandoffSegmentFact | None:
    """Return a handoff fact when the event ticker validates at both boundaries."""
    if start_reference.ticker != event_ticker or end_reference.ticker != event_ticker:
        return None
    if (
        start_reference.as_of_date.isoformat() != from_date
        or end_reference.as_of_date.isoformat() != to_date
    ):
        return None
    start_matched, start_reason = _match_reference_snapshot(target, start_reference)
    end_matched, end_reason = _match_reference_snapshot(target, end_reference)
    if not start_matched or not end_matched:
        return None
    validation = (
        _validation_row(
            start_reference,
            matched=start_matched,
            match_reason=start_reason,
            point="start",
        ),
        _validation_row(
            end_reference, matched=end_matched, match_reason=end_reason, point="end"
        ),
    )
    return HandoffSegmentFact(
        ticker=event_ticker,
        from_date=from_date,
        to_date=to_date,
        source=source,
        event_ticker_handoff={
            "candidate_ticker": candidate_ticker,
            "event_ticker": event_ticker,
            "handoff_reason": "event_ticker_valid_after_known_alias",
        },
        validation=validation,
        event_date=event_date,
    )


def _match_reference_snapshot(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> tuple[bool, str]:
    if snapshot.api_status != "OK":
        return False, f"reference status={snapshot.api_status or 'unknown'}"
    if target.composite_figi and snapshot.composite_figi == target.composite_figi:
        return True, "composite_figi_match"
    if target.share_class_figi and snapshot.share_class_figi == target.share_class_figi:
        return True, "share_class_figi_match"
    contradictions = []
    if (
        target.composite_figi
        and snapshot.composite_figi
        and snapshot.composite_figi != target.composite_figi
    ):
        contradictions.append(
            f"composite_figi detail={snapshot.composite_figi} target={target.composite_figi}"
        )
    if (
        target.share_class_figi
        and snapshot.share_class_figi
        and snapshot.share_class_figi != target.share_class_figi
    ):
        contradictions.append(
            f"share_class_figi detail={snapshot.share_class_figi} target={target.share_class_figi}"
        )
    if (
        target.security_type
        and snapshot.security_type
        and snapshot.security_type.upper() != target.security_type.upper()
    ):
        contradictions.append(
            f"security_type detail={snapshot.security_type} target={target.security_type}"
        )
    if contradictions:
        return False, "; ".join(contradictions)
    if target.cik and snapshot.cik and snapshot.cik == target.cik:
        if _same_cik_distinct_issue_mismatch(target, snapshot):
            return False, "cik_match_rejected_distinct_issue_name_mismatch"
        return True, "cik_match"
    if (
        target.identity_status == "provisional"
        and target.latest_ticker
        and snapshot.response_ticker == target.latest_ticker
        and not snapshot.cik
        and not snapshot.composite_figi
        and not snapshot.share_class_figi
    ):
        return True, "ticker_only_provisional_match_missing_cik"
    return False, "insufficient_identity_match"


def _same_cik_distinct_issue_mismatch(
    target: TargetIdentity, snapshot: ReferenceSnapshot
) -> bool:
    target_terms = distinct_issue_terms(
        target.company_name or target.current_company_name
    )
    if not target_terms:
        return False
    snapshot_terms = distinct_issue_terms(_snapshot_name(snapshot))
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
    snapshot_name = normalized_security_name(_snapshot_name(snapshot))
    return bool(
        target_name
        and snapshot_name
        and (
            target_name == snapshot_name
            or (len(snapshot_name) >= 40 and target_name.startswith(snapshot_name))
            or (len(target_name) >= 40 and snapshot_name.startswith(target_name))
        )
    )


def _snapshot_name(snapshot: ReferenceSnapshot) -> str:
    raw = snapshot.raw
    if isinstance(raw, dict):
        return str(raw.get("name") or "")
    try:
        from stock_universe.domain.records import unfreeze_json

        unfreezed = unfreeze_json(raw)
    except Exception:
        return ""
    if isinstance(unfreezed, dict):
        return str(unfreezed.get("name") or "")
    return ""


def _validation_row(
    snapshot: ReferenceSnapshot,
    *,
    matched: bool,
    match_reason: str,
    point: str,
) -> dict[str, object]:
    payload = snapshot.to_payload()
    payload["matched"] = matched
    payload["match_reason"] = match_reason
    payload["point"] = point
    return payload
