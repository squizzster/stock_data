"""Massive coverage-accounting evidence provider."""

from __future__ import annotations

from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import (
    _known_alias_replacement_for_gap,
    _omitted_fact_for_absent_ticker_interval,
    _terminal_coverage_fact,
)


@dataclass
class MassiveCoverageAccountingProvider:
    client: MassiveReadOnlyClient

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        return ()

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        facts: list[EvidenceFact] = []
        for evidence_request in evidence_requests:
            if len(evidence_request.key) < 4:
                continue
            _, ticker, from_date, to_date = evidence_request.key[:4]
            if evidence_request.kind == "coverage_gap":
                replacement = _known_alias_replacement_for_gap(
                    self.client,
                    request,
                    target,
                    old_ticker=ticker,
                    from_date=from_date,
                    to_date=to_date,
                )
                if replacement is not None:
                    facts.append(replacement)
                    continue
                omitted = _omitted_fact_for_absent_ticker_interval(
                    self.client,
                    request,
                    target,
                    ticker,
                    from_date,
                    to_date,
                )
                if omitted is not None:
                    facts.append(omitted)
            elif evidence_request.kind == "terminal_coverage":
                terminal = _terminal_coverage_fact(
                    self.client, request, target, ticker, from_date, to_date
                )
                if terminal is not None:
                    facts.append(terminal)
        return tuple(facts)
