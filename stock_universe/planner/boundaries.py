"""Boundary-evidence stage helpers for the pure planner."""

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
)
from stock_universe.domain.records import parse_date, unfreeze_json
from stock_universe.market_calendar import (
    first_us_equity_trading_date_on_or_after,
    next_us_equity_trading_date,
)
from stock_universe.planner.segments_utils import _reindex_segments


def _require_segment_boundary_evidence(
    request: BackfillRequest,
    evidence: EvidenceSnapshot,
    segments: list[PlannedSegment],
) -> EvidenceNeeded | None:
    missing: list[EvidenceRequest] = []
    failed: list[EvidenceRequest] = []
    for segment in segments:
        next_start = _next_start_boundary_request_date(evidence, segment)
        if next_start is not None:
            if next_start <= segment.to_date:
                missing.append(
                    EvidenceRequest(
                        kind="reference_boundary",
                        key=(
                            str(request.series_id),
                            segment.ticker,
                            next_start.isoformat(),
                            "start",
                        ),
                    )
                )
            else:
                failed.append(
                    EvidenceRequest(
                        kind="ticker_replacement",
                        key=(
                            str(request.series_id),
                            segment.ticker,
                            segment.from_date.isoformat(),
                            segment.to_date.isoformat(),
                        ),
                    )
                )
        for point, date_value in (("end", segment.to_date),):
            row = _segment_validation_for_boundary(segment, date_value, point)
            if row is None:
                row = _reference_boundary_for_segment(
                    evidence, segment.ticker, date_value, point
                )
            if row is None:
                missing.append(
                    EvidenceRequest(
                        kind="reference_boundary",
                        key=(
                            str(request.series_id),
                            segment.ticker,
                            date_value.isoformat(),
                            point,
                        ),
                    )
                )
            elif row.get("matched") is not True:
                failed.append(
                    EvidenceRequest(
                        kind="ticker_replacement",
                        key=(
                            str(request.series_id),
                            segment.ticker,
                            segment.from_date.isoformat(),
                            segment.to_date.isoformat(),
                        ),
                    )
                )
    if failed:
        return EvidenceNeeded(
            requests=tuple(dict.fromkeys(failed)),
            decisions=(
                RuleDecision(
                    rule_name="final_boundary_validation",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason="One or more event-derived segment boundaries failed target identity validation.",
                    evidence_refs=(
                        "reference_boundary",
                        "ticker_replacement",
                        "handoff_segment",
                        "omitted_segment",
                    ),
                    decision_id="segments:boundary_validation:failed",
                ),
            ),
        )
    if missing:
        return EvidenceNeeded(
            requests=tuple(missing),
            decisions=(
                RuleDecision(
                    rule_name="final_boundary_validation",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason="Event-derived segments require explicit start and end reference-boundary validation.",
                    evidence_refs=("ticker_events", "reference_boundary"),
                    decision_id="segments:boundary_validation:missing",
                ),
            ),
        )
    return None


def _next_start_boundary_request_date(
    evidence: EvidenceSnapshot, segment: PlannedSegment
) -> dt.date | None:
    current = parse_date(first_us_equity_trading_date_on_or_after(segment.from_date))
    while current <= segment.to_date:
        row = _segment_validation_for_boundary(segment, current, "start")
        if row is None:
            row = _reference_boundary_for_segment(
                evidence, segment.ticker, current, "start"
            )
        if row is None:
            return current
        if row.get("matched") is True:
            return None
        current = parse_date(
            first_us_equity_trading_date_on_or_after(current + dt.timedelta(days=1))
        )
    return current


def _segment_validation_for_boundary(
    segment: PlannedSegment, as_of_date: dt.date, point: str
) -> dict[str, Any] | None:
    rows = unfreeze_json(segment.validation)
    rows = rows if isinstance(rows, (list, tuple)) else ()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("point") or "") != point:
            continue
        date_value = row.get("date") or row.get("as_of_date")
        if not date_value:
            continue
        row_date = parse_date(date_value)
        if row_date == as_of_date:
            return row
        if point == "end" and _is_market_closed_boundary_after(row_date, as_of_date):
            return row
    return None


def _reference_boundary_for_segment(
    evidence: EvidenceSnapshot,
    ticker: str,
    as_of_date: dt.date,
    point: str,
) -> dict[str, Any] | None:
    rows = _latest_reference_boundaries(evidence)
    exact = rows.get((ticker, as_of_date, point))
    if exact is not None or point != "end":
        return exact
    candidates = [
        row
        for (row_ticker, row_date, row_point), row in rows.items()
        if row_ticker == ticker
        and row_point == point
        and _is_market_closed_boundary_after(row_date, as_of_date)
    ]
    return (
        max(candidates, key=lambda row: parse_date(row["as_of_date"]))
        if candidates
        else None
    )


def _is_market_closed_boundary_after(row_date: dt.date, segment_date: dt.date) -> bool:
    if row_date <= segment_date:
        return False
    next_session = parse_date(next_us_equity_trading_date(segment_date))
    return row_date < next_session


def _latest_reference_boundaries(
    evidence: EvidenceSnapshot,
) -> dict[tuple[str, dt.date, str], dict[str, Any]]:
    rows: dict[tuple[str, dt.date, str], dict[str, Any]] = {}
    for fact in evidence.get_all("reference_boundary"):
        payload = fact.payload_value()
        rows[
            (
                str(payload.get("ticker") or ""),
                parse_date(payload["as_of_date"]),
                str(payload.get("payload", {}).get("point") or ""),
            )
        ] = payload
    return rows


def _apply_reference_start_gaps(
    evidence: EvidenceSnapshot, segments: list[PlannedSegment]
) -> list[PlannedSegment]:
    reference_rows = [
        row
        for row in _latest_reference_boundaries(evidence).values()
        if row.get("matched") is True
    ]
    adjusted: list[PlannedSegment] = []
    for segment in segments:
        validation_start = _segment_validation_for_boundary(
            segment, segment.from_date, "start"
        )
        if validation_start is not None and validation_start.get("matched") is True:
            adjusted.append(segment)
            continue
        matches = [
            row
            for row in reference_rows
            if row.get("ticker") == segment.ticker
            and segment.from_date <= parse_date(row["as_of_date"]) <= segment.to_date
            and row.get("payload", {}).get("point") == "start"
        ]
        if not matches:
            adjusted.append(segment)
            continue
        first_match_row = min(matches, key=lambda row: parse_date(row["as_of_date"]))
        first_match = parse_date(first_match_row["as_of_date"])
        if first_match <= segment.from_date:
            adjusted.append(segment)
            continue
        adjusted.append(
            PlannedSegment(
                segment_index=segment.segment_index,
                ticker=segment.ticker,
                from_date=first_match,
                to_date=segment.to_date,
                source=f"{segment.source}+reference_start_gap_with_no_bars",
                valid=segment.valid,
                validation=_validation_with_start_boundary(
                    segment.validation,
                    _reference_boundary_validation_row(first_match_row, "start"),
                ),
                event_date=segment.event_date,
                request_symbol=segment.request_symbol,
                extra=segment.extra,
            )
        )
    return _reindex_segments(
        [segment for segment in adjusted if segment.from_date <= segment.to_date]
    )


def _validation_with_start_boundary(
    validation: Any, start_row: dict[str, Any]
) -> tuple[Any, ...]:
    rows = (
        list(validation)
        if isinstance(validation, (list, tuple))
        else ([validation] if validation else [])
    )
    return (
        start_row,
        *(
            row
            for row in rows
            if not (isinstance(row, dict) and row.get("point") == "start")
        ),
    )


def _reference_boundary_validation_row(
    reference_boundary: dict[str, Any], point: str
) -> dict[str, Any]:
    payload = dict(reference_boundary.get("payload") or {})
    payload["api_status"] = reference_boundary.get(
        "api_status", payload.get("api_status", "")
    )
    payload["date"] = reference_boundary.get("as_of_date", payload.get("date", ""))
    payload["matched"] = reference_boundary.get(
        "matched", payload.get("matched", False)
    )
    payload["match_reason"] = reference_boundary.get(
        "match_reason", payload.get("match_reason", "")
    )
    payload["point"] = point
    payload["requested_ticker"] = reference_boundary.get(
        "ticker", payload.get("requested_ticker", "")
    )
    return payload
