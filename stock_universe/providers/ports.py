"""Read-only provider contracts for backfill evidence collection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)


class BackfillFactProvider(Protocol):
    """Provider that can return typed evidence facts without planner access."""

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        """Return facts available before the planner asks for more."""

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        """Return facts matching planner-requested evidence."""


@dataclass(frozen=True)
class BackfillProviderSet:
    """Ordered collection of read-only providers."""

    providers: tuple[BackfillFactProvider, ...] = ()

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        facts: list[EvidenceFact] = []
        for provider in self.providers:
            facts.extend(provider.initial_facts(request, target))
        return tuple(facts)

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        facts: list[EvidenceFact] = []
        for provider in self.providers:
            facts.extend(provider.requested_facts(request, target, evidence_requests))
        return tuple(facts)
