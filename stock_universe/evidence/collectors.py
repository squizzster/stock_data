"""Evidence collector contracts.

Collectors are allowed to know about repositories or providers. The planner is
not. This module starts with a static source used by offline tests so the live
provider contract can grow against typed facts instead of legacy plan dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceLedger,
    EvidenceNeeded,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.evidence.contracts import (
    EvidenceContractIssue,
    validate_collected_backfill_facts,
)
from stock_universe.evidence.legacy import facts_from_legacy_plan
from stock_universe.providers import BackfillProviderSet


class BackfillEvidenceSource(Protocol):
    def initial_facts(self) -> tuple[EvidenceFact, ...]:
        """Return seed facts available before the planner asks for more."""

    def requested_facts(
        self, requests: tuple[EvidenceRequest, ...]
    ) -> tuple[EvidenceFact, ...]:
        """Return typed facts for planner-requested evidence."""


@dataclass(frozen=True)
class StaticBackfillEvidenceSource:
    """In-memory evidence source for offline contract and fixture tests."""

    seed_facts: tuple[EvidenceFact, ...]
    supplemental_facts: tuple[EvidenceFact, ...] = ()

    @classmethod
    def from_legacy_plan(
        cls,
        plan: dict,
        *,
        source: str = "legacy_plan_json",
        include_candidate_segments: bool = False,
        defer_kinds: tuple[str, ...] = (),
    ) -> "StaticBackfillEvidenceSource":
        facts = facts_from_legacy_plan(
            plan, source, include_candidate_segments=include_candidate_segments
        )
        deferred = set(defer_kinds)
        if not include_candidate_segments:
            deferred.add("candidate_segments")
        seed = tuple(fact for fact in facts if fact.kind not in deferred)
        supplemental = tuple(
            fact
            for fact in facts
            if fact.kind in deferred and fact.kind != "candidate_segments"
        )
        return cls(seed, supplemental)

    def initial_facts(self) -> tuple[EvidenceFact, ...]:
        return self.seed_facts

    def requested_facts(
        self, requests: tuple[EvidenceRequest, ...]
    ) -> tuple[EvidenceFact, ...]:
        requested_kinds = {request.kind for request in requests}
        return tuple(
            fact for fact in self.supplemental_facts if fact.kind in requested_kinds
        )

    def all_facts(self) -> tuple[EvidenceFact, ...]:
        return self.seed_facts + self.supplemental_facts


@dataclass(frozen=True)
class ProviderBackfillEvidenceSource:
    """Evidence source that combines repository seed facts with providers."""

    base_facts: tuple[EvidenceFact, ...]
    providers: BackfillProviderSet

    def initial_facts(self) -> tuple[EvidenceFact, ...]:
        request, target = _request_and_target_from_base_facts(self.base_facts)
        return self.base_facts + self.providers.initial_facts(request, target)

    def requested_facts(
        self, requests: tuple[EvidenceRequest, ...]
    ) -> tuple[EvidenceFact, ...]:
        request, target = _request_and_target_from_base_facts(self.base_facts)
        return self.providers.requested_facts(request, target, requests)


class EvidenceCollectionError(ValueError):
    def __init__(self, issues: tuple[EvidenceContractIssue, ...]) -> None:
        self.issues = issues
        summary = "; ".join(f"{issue.code}: {issue.reason}" for issue in issues)
        super().__init__(summary)


def collect_initial_backfill_evidence(
    source: BackfillEvidenceSource,
    *,
    validate: bool = False,
    allow_candidate_segments: bool = False,
) -> EvidenceLedger:
    facts = source.initial_facts()
    _raise_for_contract_issues(
        facts, validate=validate, allow_candidate_segments=allow_candidate_segments
    )
    return EvidenceLedger(facts)


def collect_requested_evidence(
    needed: EvidenceNeeded,
    source: BackfillEvidenceSource,
    *,
    validate: bool = False,
    allow_candidate_segments: bool = False,
) -> tuple[EvidenceFact, ...]:
    facts = source.requested_facts(needed.requests)
    _raise_for_contract_issues(
        facts, validate=validate, allow_candidate_segments=allow_candidate_segments
    )
    return facts


def _raise_for_contract_issues(
    facts: tuple[EvidenceFact, ...],
    *,
    validate: bool,
    allow_candidate_segments: bool,
) -> None:
    if not validate:
        return
    issues = validate_collected_backfill_facts(
        facts, allow_candidate_segments=allow_candidate_segments
    )
    if issues:
        raise EvidenceCollectionError(issues)


def _request_and_target_from_base_facts(
    facts: tuple[EvidenceFact, ...],
) -> tuple[BackfillRequest, TargetIdentity]:
    target_fact = _one_base_fact(facts, "target_identity")
    target = TargetIdentity.from_legacy_dict(target_fact.payload_value())
    request_fact = _one_base_fact(facts, "backfill_request")
    request = BackfillRequest.from_legacy_dict(
        target.ohlcv_series_id, request_fact.payload_value()
    )
    return request, target


def _one_base_fact(facts: tuple[EvidenceFact, ...], kind: str) -> EvidenceFact:
    matches = [fact for fact in facts if fact.kind == kind]
    if not matches:
        raise EvidenceCollectionError(
            (
                EvidenceContractIssue(
                    code="base_fact_missing",
                    reason=f"Provider-backed collection requires base fact {kind}.",
                    fact_kind=kind,
                    fact_key=(),
                ),
            )
        )
    return matches[-1]
