"""Adaptive planning loop.

Collectors live outside the planner. They may read SQLite or call providers;
the planner only sees the ledger snapshot they return.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass

from stock_universe.domain import (
    BackfillPlan,
    EvidenceFact,
    EvidenceLedger,
    EvidenceNeeded,
)
from stock_universe.evidence import (
    BackfillEvidenceSource,
    collect_initial_backfill_evidence,
    collect_requested_evidence,
)
from stock_universe.defaults import DEFAULT_MAX_ROUNDS
from stock_universe.planner import plan_backfill

EvidenceCollector = Callable[[EvidenceNeeded], tuple[EvidenceFact, ...]]


@dataclass(frozen=True)
class PlanningRound:
    round_index: int
    ledger_hash: str
    result: BackfillPlan | EvidenceNeeded
    collected_facts: tuple[EvidenceFact, ...] = ()


@dataclass(frozen=True)
class PlanningTrace:
    result: BackfillPlan | EvidenceNeeded
    rounds: tuple[PlanningRound, ...]

    @property
    def plan(self) -> BackfillPlan:
        if isinstance(self.result, BackfillPlan):
            return self.result
        raise RuntimeError(
            "backfill planning stopped before a final plan was available"
        )


@dataclass(frozen=True)
class DryRunPlanningTrace:
    result: BackfillPlan | EvidenceNeeded
    rounds: tuple[PlanningRound, ...]

    @property
    def plan(self) -> BackfillPlan:
        if isinstance(self.result, BackfillPlan):
            return self.result
        raise RuntimeError(
            "backfill planning stopped before a final plan was available"
        )


def run_backfill_planning_loop(
    initial_ledger: EvidenceLedger,
    collect_requested_evidence: EvidenceCollector,
    *,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
) -> BackfillPlan:
    return run_backfill_planning_trace(
        initial_ledger, collect_requested_evidence, max_rounds=max_rounds
    ).plan


def run_backfill_planning_trace(
    initial_ledger: EvidenceLedger,
    collect_requested_evidence: EvidenceCollector,
    *,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
) -> PlanningTrace:
    result, rounds = _run_adaptive_planning_trace(
        initial_ledger,
        collect_requested_evidence,
        max_rounds=max_rounds,
    )
    return PlanningTrace(result, rounds)


def run_backfill_source_planning_trace(
    source: BackfillEvidenceSource,
    *,
    validate_evidence: bool = True,
    allow_candidate_segments: bool = False,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
) -> PlanningTrace:
    """Run the adaptive loop from an evidence source with contract checks."""
    initial_ledger = collect_initial_backfill_evidence(
        source,
        validate=validate_evidence,
        allow_candidate_segments=allow_candidate_segments,
    )

    def collect(needed: EvidenceNeeded) -> tuple[EvidenceFact, ...]:
        return collect_requested_evidence(
            needed,
            source,
            validate=validate_evidence,
            allow_candidate_segments=allow_candidate_segments,
        )

    return run_backfill_planning_trace(initial_ledger, collect, max_rounds=max_rounds)


def run_backfill_source_planning_loop(
    source: BackfillEvidenceSource,
    *,
    validate_evidence: bool = True,
    allow_candidate_segments: bool = False,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
) -> BackfillPlan:
    return run_backfill_source_planning_trace(
        source,
        validate_evidence=validate_evidence,
        allow_candidate_segments=allow_candidate_segments,
        max_rounds=max_rounds,
    ).plan


def run_backfill_source_dry_run_trace(
    source: BackfillEvidenceSource,
    *,
    validate_evidence: bool = True,
    allow_candidate_segments: bool = False,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
) -> DryRunPlanningTrace:
    """Run planning until a plan exists or no new requested facts are available."""
    ledger = collect_initial_backfill_evidence(
        source,
        validate=validate_evidence,
        allow_candidate_segments=allow_candidate_segments,
    )
    result, rounds = _run_adaptive_planning_trace(
        ledger,
        lambda needed: collect_requested_evidence(
            needed,
            source,
            validate=validate_evidence,
            allow_candidate_segments=allow_candidate_segments,
        ),
        max_rounds=max_rounds,
    )
    return DryRunPlanningTrace(result, rounds)


def _run_adaptive_planning_trace(
    initial_ledger: EvidenceLedger,
    collect_requested_evidence: EvidenceCollector,
    *,
    max_rounds: int,
) -> tuple[BackfillPlan | EvidenceNeeded, tuple[PlanningRound, ...]]:
    """Run planning until it has a final plan or cannot make evidence progress."""
    if max_rounds < 1:
        raise ValueError("max_rounds must be at least 1")

    ledger = initial_ledger
    rounds: list[PlanningRound] = []
    seen_hashes: set[str] = set()
    requested_keys: set[tuple[str, tuple[str, ...]]] = set()
    for round_index in range(1, max_rounds + 1):
        snapshot = ledger.snapshot()
        result = plan_backfill(snapshot)
        if isinstance(result, BackfillPlan):
            rounds.append(PlanningRound(round_index, snapshot.ledger_hash, result))
            return result, tuple(rounds)
        new_requests = tuple(
            request
            for request in result.requests
            if (request.kind, request.key) not in requested_keys
        )
        if not new_requests:
            rounds.append(PlanningRound(round_index, snapshot.ledger_hash, result))
            return result, tuple(rounds)
        requested_keys.update((request.kind, request.key) for request in new_requests)
        collected = tuple(
            collect_requested_evidence(
                EvidenceNeeded(requests=new_requests, decisions=result.decisions)
            )
        )
        existing_facts = {_fact_identity(fact) for fact in ledger.facts}
        new_facts = tuple(
            fact for fact in collected if _fact_identity(fact) not in existing_facts
        )
        rounds.append(
            PlanningRound(round_index, snapshot.ledger_hash, result, new_facts)
        )
        if not new_facts or snapshot.ledger_hash in seen_hashes:
            return result, tuple(rounds)
        seen_hashes.add(snapshot.ledger_hash)
        ledger = ledger.merge(new_facts)
    return result, tuple(rounds)


def _fact_identity(fact: EvidenceFact) -> str:
    return json.dumps(fact.to_legacy_dict(), sort_keys=True, separators=(",", ":"))
