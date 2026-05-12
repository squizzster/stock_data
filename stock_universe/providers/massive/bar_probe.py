"""Massive aggregate-bar probe evidence provider."""

from __future__ import annotations

from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.evidence.normalizers import bar_probe_fact_from_result
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import (
    _aggregate_bars_payload,
    _bar_probe_result_from_payload,
)


@dataclass
class MassiveBarProbeProvider:
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
            if evidence_request.kind != "bar_probe" or len(evidence_request.key) < 4:
                continue
            _, ticker, from_date, to_date = evidence_request.key[:4]
            payload = _aggregate_bars_payload(
                self.client, request, ticker, from_date, to_date
            )
            result = _bar_probe_result_from_payload(ticker, from_date, to_date, payload)
            fact = bar_probe_fact_from_result(
                request.series_id,
                result,
                source="massive.bar_probe",
            )
            facts.append(fact.to_evidence_fact(request.series_id))
        return tuple(facts)
