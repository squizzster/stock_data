"""Validation for collected evidence before pure planning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from stock_universe.domain import EvidenceFact


@dataclass(frozen=True)
class EvidenceContractIssue:
    code: str
    reason: str
    fact_kind: str
    fact_key: tuple[str, ...]


def validate_collected_backfill_facts(
    facts: tuple[EvidenceFact, ...],
    *,
    allow_candidate_segments: bool = False,
) -> tuple[EvidenceContractIssue, ...]:
    """Check evidence-source output before planner input.

    The contract is intentionally small. It catches collectors that smuggle
    precomputed candidate segments into typed-evidence tests, or emit trusted
    replacement/handoff facts without boundary validation payloads.
    """
    issues: list[EvidenceContractIssue] = []
    series_ids = {fact.key[0] for fact in facts if fact.key}
    if len(series_ids) > 1:
        issues.append(
            EvidenceContractIssue(
                code="mixed_series_id",
                reason="Collected facts reference more than one series id.",
                fact_kind="*",
                fact_key=tuple(sorted(series_ids)),
            )
        )

    for fact in facts:
        payload = fact.payload_value()
        if fact.kind == "candidate_segments" and not allow_candidate_segments:
            issues.append(
                _issue(
                    "candidate_segments_not_allowed",
                    "Collector emitted precomputed candidate segments.",
                    fact,
                )
            )
        elif fact.kind == "ticker_replacement":
            issues.extend(_validate_replacement_fact(fact, payload))
        elif fact.kind == "handoff_segment":
            issues.extend(_validate_handoff_fact(fact, payload))
        elif fact.kind == "omitted_segment":
            issues.extend(_validate_omitted_segment_fact(fact, payload))
        elif fact.kind == "reference_boundary":
            issues.extend(_validate_reference_boundary_fact(fact, payload))
        elif fact.kind == "terminal_coverage":
            issues.extend(_validate_terminal_coverage_fact(fact, payload))
    return tuple(issues)


def _validate_replacement_fact(
    fact: EvidenceFact, payload: dict[str, Any]
) -> tuple[EvidenceContractIssue, ...]:
    issues: list[EvidenceContractIssue] = []
    for field in (
        "old_ticker",
        "new_ticker",
        "from_date",
        "to_date",
        "replacement_reason",
    ):
        if not payload.get(field):
            issues.append(
                _issue(
                    "replacement_field_missing",
                    f"Ticker replacement is missing {field}.",
                    fact,
                )
            )
    if not payload.get("validation"):
        issues.append(
            _issue(
                "replacement_validation_missing",
                "Ticker replacement has no validation rows.",
                fact,
            )
        )
    return tuple(issues)


def _validate_handoff_fact(
    fact: EvidenceFact, payload: dict[str, Any]
) -> tuple[EvidenceContractIssue, ...]:
    issues: list[EvidenceContractIssue] = []
    for field in ("ticker", "from_date", "to_date", "source"):
        if not payload.get(field):
            issues.append(
                _issue(
                    "handoff_field_missing",
                    f"Handoff segment is missing {field}.",
                    fact,
                )
            )
    if not payload.get("event_ticker_handoff"):
        issues.append(
            _issue(
                "handoff_metadata_missing",
                "Handoff segment has no event_ticker_handoff metadata.",
                fact,
            )
        )
    if not payload.get("validation"):
        issues.append(
            _issue(
                "handoff_validation_missing",
                "Handoff segment has no validation rows.",
                fact,
            )
        )
    return tuple(issues)


def _validate_omitted_segment_fact(
    fact: EvidenceFact, payload: dict[str, Any]
) -> tuple[EvidenceContractIssue, ...]:
    issues: list[EvidenceContractIssue] = []
    for field in ("ticker", "from_date", "to_date", "reason"):
        if not payload.get(field):
            issues.append(
                _issue(
                    "omitted_segment_field_missing",
                    f"Omitted segment is missing {field}.",
                    fact,
                )
            )
    if fact.source == "plan_payload":
        return tuple(issues)
    proof = payload.get("proof") or {}
    for field in (
        "start_reference",
        "end_reference",
        "bar_probe",
        "start_identity_scan",
        "end_identity_scan",
    ):
        if not proof.get(field):
            issues.append(
                _issue(
                    "omitted_segment_proof_missing",
                    f"Omitted segment proof is missing {field}.",
                    fact,
                )
            )
    return tuple(issues)


def _validate_reference_boundary_fact(
    fact: EvidenceFact, payload: dict[str, Any]
) -> tuple[EvidenceContractIssue, ...]:
    if payload.get("matched") is True and not payload.get("payload", {}).get("point"):
        return (
            _issue(
                "reference_boundary_point_missing",
                "Matched reference boundary does not identify start/end point.",
                fact,
            ),
        )
    return ()


def _validate_terminal_coverage_fact(
    fact: EvidenceFact, payload: dict[str, Any]
) -> tuple[EvidenceContractIssue, ...]:
    issues: list[EvidenceContractIssue] = []
    for field in ("ticker", "from_date", "to_date", "reason"):
        if not payload.get(field):
            issues.append(
                _issue(
                    "terminal_coverage_field_missing",
                    f"Terminal coverage is missing {field}.",
                    fact,
                )
            )
    return tuple(issues)


def _issue(code: str, reason: str, fact: EvidenceFact) -> EvidenceContractIssue:
    return EvidenceContractIssue(
        code=code, reason=reason, fact_kind=fact.kind, fact_key=fact.key
    )
