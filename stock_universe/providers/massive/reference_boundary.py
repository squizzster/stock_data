"""Massive reference-boundary evidence provider."""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import (
    _first_bar_boundary_fact_after_start_gap,
    _reference_boundary_fact_with_historical_rekey,
    _reference_snapshot_from_payload,
)


@dataclass
class MassiveReferenceBoundaryProvider:
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
        alias_history_requested = any(
            req.kind == "alias_history" for req in evidence_requests
        )
        for evidence_request in evidence_requests:
            if (
                evidence_request.kind != "reference_boundary"
                or len(evidence_request.key) < 4
            ):
                continue
            _, ticker, as_of_date, point = evidence_request.key[:4]
            if point not in {"start", "end"}:
                continue
            payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
                {"date": as_of_date},
            )
            snapshot = _reference_snapshot_from_payload(ticker, as_of_date, payload)
            fact = _reference_boundary_fact_with_historical_rekey(
                request.series_id,
                target,
                snapshot,
                point=point,
                source="massive.reference_boundary",
            )
            facts.append(fact.to_evidence_fact(request.series_id))
            if (
                point == "start"
                and fact.matched is not True
                and not alias_history_requested
            ):
                later = _first_bar_boundary_fact_after_start_gap(
                    self.client,
                    request,
                    target,
                    ticker,
                    as_of_date,
                    request.to_date.isoformat(),
                )
                if later is not None:
                    facts.append(later.to_evidence_fact(request.series_id))
        return tuple(facts)
