"""Massive identity-scan evidence provider."""

from __future__ import annotations

from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.evidence.normalizers import identity_scan_fact_from_result
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import _identity_scan_result_from_payload


@dataclass
class MassiveIdentityScanProvider:
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
            if (
                evidence_request.kind != "identity_scan"
                or len(evidence_request.key) < 3
            ):
                continue
            _, query, as_of_date = evidence_request.key[:3]
            payload = self.client.get(
                "/v3/reference/tickers",
                {
                    "search": query,
                    "date": as_of_date,
                    "active": "false",
                    "limit": "100",
                },
            )
            result = _identity_scan_result_from_payload(query, as_of_date, payload)
            fact = identity_scan_fact_from_result(
                request.series_id,
                result,
                source="massive.identity_scan",
            )
            facts.append(fact.to_evidence_fact(request.series_id))
        return tuple(facts)
