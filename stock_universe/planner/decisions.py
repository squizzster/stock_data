"""Decision and validation helpers for the pure planner."""

from __future__ import annotations

from typing import Any

from stock_universe.domain import (
    BackfillRequest,
    EvidenceSnapshot,
    PlannedSegment,
    RuleDecision,
    TargetIdentity,
)


def _derived_fact_decisions(evidence: EvidenceSnapshot) -> tuple[RuleDecision, ...]:
    decisions: list[RuleDecision] = []
    for index, fact in enumerate(evidence.get_all("ticker_replacement"), 1):
        payload = fact.payload_value()
        old_ticker = payload["old_ticker"]
        new_ticker = payload["new_ticker"]
        if old_ticker == new_ticker:
            reason = (
                f"Segment {index} retained ticker {old_ticker}: ticker-events coverage required explicit "
                f"target-valid window evidence qualified by {payload['replacement_reason']}."
            )
        else:
            reason = (
                f"Segment {index} ticker changed from {old_ticker} to {new_ticker}: "
                "ticker-events alias did not validate target identity, while exactly one replacement "
                f"qualified by {payload['replacement_reason']}."
            )
        decisions.append(
            RuleDecision(
                rule_name="ticker_replacement",
                outcome="warn",
                segment_id=None,
                reason=reason,
                evidence_refs=(f"ticker_replacement:{':'.join(fact.key)}",),
                decision_id=f"ticker_replacement:{old_ticker}:{new_ticker}",
            )
        )
    return tuple(decisions)


def _validate_segment_boundaries(
    request: BackfillRequest, segments: tuple[PlannedSegment, ...]
) -> tuple[RuleDecision, ...]:
    decisions: list[RuleDecision] = []
    previous: PlannedSegment | None = None
    for expected_index, segment in enumerate(segments, 1):
        if segment.segment_index != expected_index:
            decisions.append(
                RuleDecision(
                    rule_name="segment_index_order",
                    outcome="block",
                    segment_id=segment.segment_id,
                    reason=f"Segment index {segment.segment_index} is out of order; expected {expected_index}.",
                    evidence_refs=("candidate_segments",),
                    decision_id=f"segment:{segment.segment_index}:index_order",
                )
            )
        if segment.from_date > segment.to_date:
            decisions.append(
                RuleDecision(
                    rule_name="segment_date_order",
                    outcome="block",
                    segment_id=segment.segment_id,
                    reason=f"Segment {segment.segment_index} {segment.ticker} starts after it ends.",
                    evidence_refs=("candidate_segments",),
                    decision_id=f"segment:{segment.segment_index}:date_order",
                )
            )
        if segment.from_date < request.from_date or segment.to_date > request.to_date:
            decisions.append(
                RuleDecision(
                    rule_name="segment_request_bounds",
                    outcome="block",
                    segment_id=segment.segment_id,
                    reason=(
                        f"Segment {segment.segment_index} {segment.ticker} falls outside requested range "
                        f"{request.from_date.isoformat()} to {request.to_date.isoformat()}."
                    ),
                    evidence_refs=("candidate_segments", "backfill_request"),
                    decision_id=f"segment:{segment.segment_index}:request_bounds",
                )
            )
        if previous and segment.from_date <= previous.to_date:
            decisions.append(
                RuleDecision(
                    rule_name="segment_non_overlap",
                    outcome="block",
                    segment_id=segment.segment_id,
                    reason=(
                        f"Segment {segment.segment_index} {segment.ticker} overlaps prior segment "
                        f"{previous.segment_index} {previous.ticker}."
                    ),
                    evidence_refs=("candidate_segments",),
                    decision_id=f"segment:{segment.segment_index}:overlap",
                )
            )
        previous = segment
    return tuple(decisions)


def _validate_segment_identity_flags(
    segments: tuple[PlannedSegment, ...],
) -> tuple[RuleDecision, ...]:
    decisions: list[RuleDecision] = []
    for segment in segments:
        if not segment.valid:
            decisions.append(
                RuleDecision(
                    rule_name="segment_validation_flag",
                    outcome="block",
                    segment_id=segment.segment_id,
                    reason=f"Segment {segment.segment_index} {segment.ticker} is marked invalid.",
                    evidence_refs=("candidate_segments",),
                    decision_id=f"segment:{segment.segment_index}:invalid",
                )
            )
            continue
        validation_rows: list[dict[str, Any]] = segment.to_payload().get(
            "validation", []
        )
        for row in validation_rows:
            if row.get("matched") is False:
                point = row.get("point") or "boundary"
                reason = row.get("match_reason") or "unmatched"
                decisions.append(
                    RuleDecision(
                        rule_name="segment_boundary_validation",
                        outcome="block",
                        segment_id=segment.segment_id,
                        reason=f"Segment {segment.segment_index} {segment.ticker} {point} failed validation: {reason}.",
                        evidence_refs=("candidate_segments",),
                        decision_id=f"segment:{segment.segment_index}:{point}:unmatched",
                    )
                )
    return tuple(decisions)


def _assign_status(target: TargetIdentity, decisions: list[RuleDecision]) -> str:
    if any(decision.outcome == "block" for decision in decisions):
        return "blocked"
    if target.identity_status != "permanent":
        return "caution"
    return "safe"
