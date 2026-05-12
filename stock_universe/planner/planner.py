"""Pure backfill planner kernel.

This module deliberately accepts only evidence snapshots and returns typed
records. It does not open SQLite, call HTTP, read environment variables, read
files, write files, or use the clock.
"""

from __future__ import annotations

from stock_universe.domain import (
    BackfillPlan,
    BackfillRequest,
    EvidenceNeeded,
    EvidenceRequest,
    EvidenceSnapshot,
    KnownAlias,
    PlannedSegment,
    RuleDecision,
    TargetIdentity,
)
from stock_universe.planner.decisions import (
    _assign_status,
    _derived_fact_decisions,
    _legacy_decisions,
    _validate_segment_boundaries,
    _validate_segment_identity_flags,
)
from stock_universe.planner.initial_segments import _derive_initial_event_segments
from stock_universe.planner.required import _missing_required_evidence

PLANNER_VERSION = "backfill-engine-slice-0.1"


def plan_backfill(evidence: EvidenceSnapshot) -> BackfillPlan | EvidenceNeeded:
    missing = _missing_required_evidence(evidence)
    if missing:
        return EvidenceNeeded(
            requests=tuple(EvidenceRequest(kind=item, key=()) for item in missing),
            decisions=tuple(
                RuleDecision(
                    rule_name="required_evidence",
                    outcome="needs_evidence",
                    segment_id=None,
                    reason=f"Missing required evidence: {item}",
                    evidence_refs=(),
                    decision_id=f"needs:{item}",
                )
                for item in missing
            ),
        )

    target_payload = evidence.get_one("target_identity").payload_value()  # type: ignore[union-attr]
    target = TargetIdentity.from_legacy_dict(target_payload)
    request_payload = evidence.get_one("backfill_request").payload_value()  # type: ignore[union-attr]
    request = BackfillRequest.from_legacy_dict(target.ohlcv_series_id, request_payload)
    aliases = tuple(
        KnownAlias.from_legacy_dict(item)
        for item in evidence.get_one("known_aliases").payload_value()  # type: ignore[union-attr]
    )
    candidate_segments_fact = evidence.get_one("candidate_segments")
    if candidate_segments_fact:
        segments = tuple(
            PlannedSegment.from_legacy_dict(item)
            for item in candidate_segments_fact.payload_value()
        )
    else:
        derived = _derive_initial_event_segments(request, evidence)
        if isinstance(derived, EvidenceNeeded):
            return derived
        segments = derived
    metadata = evidence.get_one("plan_metadata").payload_value()  # type: ignore[union-attr]
    event_lookup_fact = evidence.get_one("event_lookup")
    event_lookup = event_lookup_fact.payload_value() if event_lookup_fact else {}

    decisions: list[RuleDecision] = []
    legacy_decisions = _legacy_decisions(evidence)
    decisions.extend(legacy_decisions)
    if not legacy_decisions:
        decisions.extend(_derived_fact_decisions(evidence))
    decisions.extend(_validate_segment_boundaries(request, segments))
    if not segments:
        decisions.append(
            RuleDecision(
                rule_name="segments_present",
                outcome="block",
                segment_id=None,
                reason="No validated ticker segments were produced.",
                evidence_refs=("candidate_segments",),
                decision_id="segments:empty",
            )
        )
    if not any(decision.outcome == "block" for decision in decisions):
        decisions.extend(_validate_segment_identity_flags(segments))

    status = _assign_status(target, decisions)
    return BackfillPlan(
        request=request,
        status=status,
        target=target,
        segments=segments,
        decisions=tuple(decisions),
        evidence_ledger_hash=evidence.ledger_hash,
        planner_version=PLANNER_VERSION,
        created_at_utc=str(metadata.get("generated_at_utc") or "unknown"),
        event_lookup=event_lookup,
        known_aliases=aliases,
        raw_dir=str(metadata.get("raw_dir") or ""),
        api_requests=int(metadata.get("api_requests") or 0),
        identity_discovery=metadata.get("identity_discovery") or {},
        plan_files=metadata.get("plan_files") or {},
    )
