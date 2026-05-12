"""Initial event and alias segment derivation for the pure planner."""

from __future__ import annotations

import datetime as dt
from typing import Any

from stock_universe.domain import (
    BackfillRequest,
    EvidenceNeeded,
    EvidenceRequest,
    EvidenceSnapshot,
    PlannedSegment,
    RuleDecision,
    TargetIdentity,
)
from stock_universe.domain.records import parse_date
from stock_universe.market_calendar import (
    first_us_equity_trading_date_on_or_after,
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
)
from stock_universe.planner.boundaries import (
    _apply_reference_start_gaps,
    _require_segment_boundary_evidence,
)
from stock_universe.planner.coverage import (
    _coverage_gap_reason,
    _drop_omitted_segments,
    _has_omitted_coverage,
    _has_terminal_coverage,
    _uncovered_ranges,
)
from stock_universe.planner.segments_utils import _reindex_segments
from stock_universe.planner.transforms import (
    _apply_handoff_segments,
    _apply_ticker_replacements,
)


def _derive_initial_event_segments(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
) -> tuple[PlannedSegment, ...] | EvidenceNeeded:
    fact = evidence.get_one("ticker_events")
    if fact is None:
        return EvidenceNeeded(
            requests=(
                EvidenceRequest(kind="ticker_events", key=(str(request.series_id),)),
            ),
            decisions=(
                RuleDecision(
                    rule_name="derive_initial_event_segments",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason="Ticker-event evidence is required before initial segments can be derived.",
                    evidence_refs=("ticker_events",),
                    decision_id="segments:ticker_events:missing",
                ),
            ),
        )
    payload = fact.payload_value()
    if payload.get("api_status") != "OK":
        alias_segments = _alias_history_segments(
            request,
            evidence,
            before_date=_next_session_after(request.to_date),
        )
        if alias_segments:
            finalized = _finalize_derived_segments(
                request,
                evidence,
                _drop_omitted_segments(evidence, alias_segments),
                coverage_segments=alias_segments,
            )
            return finalized
        return EvidenceNeeded(
            requests=(
                EvidenceRequest(
                    kind="alias_history",
                    key=(
                        str(request.series_id),
                        request.from_date.isoformat(),
                        _next_session_after(request.to_date).isoformat(),
                    ),
                ),
            ),
            decisions=(
                RuleDecision(
                    rule_name="derive_initial_event_segments",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason=(
                        f"Ticker-event lookup is not usable: status={payload.get('api_status') or 'unknown'}; "
                        "bar-backed alias-history evidence is required before planning without ticker events."
                    ),
                    evidence_refs=("ticker_events", "known_aliases", "alias_history"),
                    decision_id="segments:ticker_events:not_ok",
                ),
            ),
        )

    events = [
        {
            "date": parse_date(item["date"]),
            "ticker": str(item.get("ticker") or ""),
            "type": item.get("type"),
        }
        for item in payload.get("events", [])
        if item.get("date") and item.get("ticker")
    ]
    events.sort(key=lambda item: item["date"])
    event_segments = [event for event in events if event["date"] <= request.to_date]
    if not event_segments:
        alias_segments = _alias_history_segments(
            request,
            evidence,
            before_date=_next_session_after(request.to_date),
        )
        if alias_segments:
            finalized = _finalize_derived_segments(
                request,
                evidence,
                _drop_omitted_segments(evidence, alias_segments),
                coverage_segments=alias_segments,
            )
            return finalized
        return EvidenceNeeded(
            requests=(
                EvidenceRequest(
                    kind="alias_history",
                    key=(
                        str(request.series_id),
                        request.from_date.isoformat(),
                        _next_session_after(request.to_date).isoformat(),
                    ),
                ),
            ),
            decisions=(
                RuleDecision(
                    rule_name="derive_initial_event_segments",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason=(
                        "Ticker events do not identify a ticker active during the requested range; "
                        "bar-backed alias-history evidence is required before planning without an active event."
                    ),
                    evidence_refs=("ticker_events", "known_aliases", "alias_history"),
                    decision_id="segments:ticker_events:no_active_event",
                ),
            ),
        )
    if event_segments[0]["date"] > request.from_date:
        alias_segments = _alias_history_segments(
            request, evidence, before_date=event_segments[0]["date"]
        )
        if alias_segments:
            combined = _reindex_segments(
                alias_segments + _event_segments_for_range(request, event_segments)
            )
            finalized = _finalize_derived_segments(
                request,
                evidence,
                _drop_omitted_segments(evidence, combined),
                coverage_segments=combined,
            )
            return finalized
        pre_event_end = _previous_session_before(event_segments[0]["date"])
        first_event_ticker = str(event_segments[0]["ticker"])
        if _has_omitted_coverage(
            evidence, first_event_ticker, request.from_date, pre_event_end
        ):
            finalized = _finalize_derived_segments(
                request,
                evidence,
                _drop_omitted_segments(
                    evidence, _event_segments_for_range(request, event_segments)
                ),
                coverage_segments=_event_segments_for_range(request, event_segments),
            )
            return finalized
        return EvidenceNeeded(
            requests=(
                EvidenceRequest(
                    kind="alias_history",
                    key=(
                        str(request.series_id),
                        request.from_date.isoformat(),
                        event_segments[0]["date"].isoformat(),
                        first_event_ticker,
                    ),
                ),
                *_pre_event_evidence_requests(
                    request,
                    evidence,
                    first_event_ticker=first_event_ticker,
                    pre_event_end=pre_event_end,
                ),
            ),
            decisions=(
                RuleDecision(
                    rule_name="derive_initial_event_segments",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason=(
                        "Ticker events start after the requested range begins; alias-history and reference evidence "
                        "are required to fill the pre-event interval."
                    ),
                    evidence_refs=(
                        "ticker_events",
                        "known_aliases",
                        "backfill_request",
                    ),
                    decision_id="segments:ticker_events:pre_event_gap",
                ),
            ),
        )

    raw_segments = _event_segments_for_range(request, event_segments)
    segments = _drop_omitted_segments(evidence, raw_segments)
    if not segments:
        if raw_segments:
            return ()
        return EvidenceNeeded(
            requests=(
                EvidenceRequest(
                    kind="reference_boundary", key=(str(request.series_id),)
                ),
            ),
            decisions=(
                RuleDecision(
                    rule_name="derive_initial_event_segments",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason="Ticker events fell outside the requested range after date clipping.",
                    evidence_refs=("ticker_events", "backfill_request"),
                    decision_id="segments:ticker_events:outside_range",
                ),
            ),
        )
    return _finalize_derived_segments(
        request, evidence, segments, coverage_segments=raw_segments
    )


def _finalize_derived_segments(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
    segments: list[PlannedSegment],
    *,
    coverage_segments: list[PlannedSegment] | None = None,
) -> tuple[PlannedSegment, ...] | EvidenceNeeded:
    transformed = _apply_handoff_segments(
        evidence, _apply_ticker_replacements(evidence, segments)
    )
    coverage_basis = coverage_segments if coverage_segments is not None else segments
    gaps = _uncovered_ranges(evidence, coverage_basis, transformed)
    gaps = gaps + _latest_ticker_terminal_coverage_gaps(
        request, evidence, coverage_basis, transformed
    )
    if gaps:
        return EvidenceNeeded(
            requests=tuple(
                EvidenceRequest(
                    kind=kind,
                    key=(
                        str(request.series_id),
                        ticker,
                        from_date.isoformat(),
                        to_date.isoformat(),
                    ),
                )
                for kind, ticker, from_date, to_date in gaps
            ),
            decisions=tuple(
                RuleDecision(
                    rule_name="coverage_accounting",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason=(_coverage_gap_reason(kind, ticker, from_date, to_date)),
                    evidence_refs=(
                        "ticker_replacement",
                        "handoff_segment",
                        "omitted_segment",
                        "terminal_coverage",
                    ),
                    decision_id=f"segments:{kind}:{ticker}:{from_date.isoformat()}:{to_date.isoformat()}",
                )
                for kind, ticker, from_date, to_date in gaps
            ),
        )
    adjusted = _apply_reference_start_gaps(evidence, transformed)
    boundary_result = _require_segment_boundary_evidence(request, evidence, adjusted)
    if isinstance(boundary_result, EvidenceNeeded):
        return boundary_result
    return tuple(adjusted)


def _latest_ticker_terminal_coverage_gaps(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
    original_segments: list[PlannedSegment],
    transformed_segments: list[PlannedSegment],
) -> tuple[tuple[str, str, dt.date, dt.date], ...]:
    target_fact = evidence.get_one("target_identity")
    if target_fact is None:
        return ()
    target = TargetIdentity.from_payload(target_fact.payload_value())
    latest_ticker = str(target.latest_ticker or "")
    if not latest_ticker:
        return ()
    if not _has_non_latest_omitted_tail(
        evidence, original_segments, latest_ticker, request.to_date
    ):
        return ()
    covered_end = max(
        (segment.to_date for segment in transformed_segments), default=None
    )
    from_date = (
        request.from_date
        if covered_end is None
        else max(request.from_date, _next_session_after(covered_end))
    )
    to_date = request.to_date
    if from_date > to_date:
        return ()
    if _has_terminal_coverage(evidence, latest_ticker, from_date, to_date):
        return ()
    return (("terminal_coverage", latest_ticker, from_date, to_date),)


def _has_non_latest_omitted_tail(
    evidence: EvidenceSnapshot,
    original_segments: list[PlannedSegment],
    latest_ticker: str,
    request_to_date: dt.date,
) -> bool:
    terminal_segments = [
        segment
        for segment in original_segments
        if segment.to_date == request_to_date
        and segment.ticker != latest_ticker
        and _has_omitted_coverage(
            evidence, segment.ticker, segment.from_date, segment.to_date
        )
    ]
    return bool(terminal_segments)


def _event_segments_for_range(
    request: BackfillRequest, event_segments: list[dict[str, Any]]
) -> list[PlannedSegment]:
    segments: list[PlannedSegment] = []
    for index, event in enumerate(event_segments):
        next_event_date = (
            event_segments[index + 1]["date"]
            if index + 1 < len(event_segments)
            else None
        )
        from_date = parse_date(
            first_us_equity_trading_date_on_or_after(
                max(event["date"], request.from_date)
            )
        )
        to_date = min(
            _previous_session_before(next_event_date)
            if next_event_date
            else request.to_date,
            request.to_date,
        )
        if (
            to_date < request.from_date
            or from_date > request.to_date
            or from_date > to_date
        ):
            continue
        segments.append(
            PlannedSegment(
                segment_index=len(segments) + 1,
                ticker=event["ticker"],
                from_date=from_date,
                to_date=to_date,
                source="ticker_events",
                event_date=event["date"],
                validation=(),
            )
        )
    return segments


def _pre_event_evidence_requests(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
    *,
    first_event_ticker: str,
    pre_event_end: dt.date,
) -> tuple[EvidenceRequest, ...]:
    absence_request = EvidenceRequest(
        kind="coverage_gap",
        key=(
            str(request.series_id),
            first_event_ticker,
            request.from_date.isoformat(),
            pre_event_end.isoformat(),
        ),
    )
    fact = evidence.get_one("known_aliases")
    if fact is None:
        return (absence_request,)

    tickers: list[str] = []
    for item in fact.payload_value():
        ticker = str(item.get("symbol_text") or item.get("ticker") or "")
        if not ticker or ticker == first_event_ticker or ticker in tickers:
            continue
        tickers.append(ticker)
    if not tickers:
        return (absence_request,)

    alias_requests = tuple(
        EvidenceRequest(
            kind="reference_boundary",
            key=(
                str(request.series_id),
                ticker,
                request.from_date.isoformat(),
                "start",
            ),
        )
        for ticker in tickers
    )
    return (absence_request, *alias_requests)


def _alias_history_segments(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
    *,
    before_date: dt.date,
) -> list[PlannedSegment]:
    fact = evidence.get_one("alias_history")
    if fact is None:
        return []
    pre_event_end = _previous_session_before(before_date)
    segments: list[PlannedSegment] = []
    for span in fact.payload_value().get("spans", []):
        from_date = max(parse_date(span["from_date"]), request.from_date)
        to_date = min(parse_date(span["to_date"]), pre_event_end, request.to_date)
        if from_date > to_date:
            continue
        segments.append(
            PlannedSegment(
                segment_index=len(segments) + 1,
                ticker=str(span["ticker"]),
                from_date=from_date,
                to_date=to_date,
                source=str(span.get("source") or "alias_history"),
                valid=bool(span.get("valid", True)),
                validation=span.get("validation") or (),
                event_date=span.get("event_date"),
            )
        )
    return segments


def _next_session_after(value: dt.date) -> dt.date:
    return parse_date(next_us_equity_trading_date(value))


def _previous_session_before(value: dt.date) -> dt.date:
    return parse_date(previous_us_equity_trading_date(value))
