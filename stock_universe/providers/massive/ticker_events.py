"""Massive ticker-events evidence provider."""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
    TickerEventFact,
)
from stock_universe.providers.massive.client import MassiveReadOnlyClient


@dataclass
class MassiveTickerEventsProvider:
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
        if not any(
            evidence_request.kind == "ticker_events"
            for evidence_request in evidence_requests
        ):
            return ()
        identifier, identifier_type = _ticker_events_identifier(target)
        if not identifier:
            return ()
        endpoint = (
            f"/vX/reference/tickers/{urllib.parse.quote(identifier, safe='')}/events"
        )
        payload = self.client.get(endpoint, {"types": "ticker_change"})
        fact = TickerEventFact.from_provider_payload(
            identifier,
            identifier_type,
            payload,
            source="massive.ticker_events",
        )
        return (fact.to_evidence_fact(request.series_id),)


def _ticker_events_identifier(target: TargetIdentity) -> tuple[str, str]:
    if target.composite_figi:
        return target.composite_figi, "composite_figi"
    if target.latest_ticker:
        return target.latest_ticker, "ticker"
    return "", ""
