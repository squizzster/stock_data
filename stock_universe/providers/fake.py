"""Offline fake providers for evidence-source tests."""

from __future__ import annotations

from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    TargetIdentity,
)
from stock_universe.evidence.normalizers import (
    bar_probe_fact_from_result,
    handoff_segment_fact_from_target_valid_event_window,
    identity_scan_fact_from_result,
    omitted_segment_fact_from_absent_reference_and_bars,
    reference_boundary_fact_from_snapshot,
    ticker_replacement_fact_from_target_valid_alias_window,
)
from stock_universe.providers.models import (
    BarProbeResult,
    HandoffWindow,
    IdentityScanResult,
    OmittedSegmentProbe,
    ReferenceBoundaryProbe,
    ReferenceSnapshot,
    TickerReplacementWindow,
)


@dataclass(frozen=True)
class StaticBackfillFactProvider:
    """Read-only fake provider backed by typed evidence facts."""

    facts: tuple[EvidenceFact, ...]
    seed_kinds: tuple[str, ...] = ()

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        return tuple(
            fact
            for fact in self.facts
            if fact.kind in self.seed_kinds
            and _matches_series(fact, request.series_id, target.ohlcv_series_id)
        )

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        requested_kinds = {
            evidence_request.kind for evidence_request in evidence_requests
        }
        return tuple(
            fact
            for fact in self.facts
            if fact.kind in requested_kinds
            and fact.kind not in self.seed_kinds
            and _matches_series(fact, request.series_id, target.ohlcv_series_id)
        )


def _matches_series(
    fact: EvidenceFact, request_series_id: int, target_series_id: int
) -> bool:
    if not fact.key:
        return True
    return fact.key[0] in {str(request_series_id), str(target_series_id)}


@dataclass(frozen=True)
class StaticProviderReadFactProvider:
    """Fake provider that normalizes deterministic raw read models."""

    reference_boundary_probes: tuple[ReferenceBoundaryProbe, ...] = ()
    reference_snapshots: tuple[ReferenceSnapshot, ...] = ()
    bar_probes: tuple[BarProbeResult, ...] = ()
    identity_scans: tuple[IdentityScanResult, ...] = ()
    omitted_segments: tuple[OmittedSegmentProbe, ...] = ()
    ticker_replacements: tuple[TickerReplacementWindow, ...] = ()
    handoffs: tuple[HandoffWindow, ...] = ()
    seed_kinds: tuple[str, ...] = ()

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        facts = self._facts(request, target)
        return tuple(fact for fact in facts if fact.kind in self.seed_kinds)

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        requested_kinds = {
            evidence_request.kind for evidence_request in evidence_requests
        }
        facts = self._facts(request, target)
        return tuple(
            fact
            for fact in facts
            if fact.kind in requested_kinds and fact.kind not in self.seed_kinds
        )

    def _facts(
        self, request: BackfillRequest, target: TargetIdentity
    ) -> tuple[EvidenceFact, ...]:
        facts: list[EvidenceFact] = []
        facts.extend(
            reference_boundary_fact_from_snapshot(
                request.series_id,
                target,
                probe.snapshot,
                point=probe.point,
            ).to_evidence_fact(request.series_id)
            for probe in self.reference_boundary_probes
        )
        facts.extend(
            bar_probe_fact_from_result(request.series_id, probe).to_evidence_fact(
                request.series_id
            )
            for probe in self.bar_probes
        )
        facts.extend(
            identity_scan_fact_from_result(request.series_id, scan).to_evidence_fact(
                request.series_id
            )
            for scan in self.identity_scans
        )
        for probe in self.omitted_segments:
            start = self._reference_snapshot(probe.ticker, probe.from_date.isoformat())
            end = self._reference_snapshot(probe.ticker, probe.to_date.isoformat())
            bars = self._bar_probe(
                probe.ticker, probe.from_date.isoformat(), probe.to_date.isoformat()
            )
            start_scan = self._identity_scan(probe.ticker, probe.from_date.isoformat())
            end_scan = self._identity_scan(probe.ticker, probe.to_date.isoformat())
            if start and end and bars and start_scan and end_scan:
                fact = omitted_segment_fact_from_absent_reference_and_bars(
                    request.series_id,
                    ticker=probe.ticker,
                    from_date=probe.from_date.isoformat(),
                    to_date=probe.to_date.isoformat(),
                    start_reference=start,
                    end_reference=end,
                    bar_probe=bars,
                    start_identity_scan=start_scan,
                    end_identity_scan=end_scan,
                )
                if fact:
                    facts.append(fact.to_evidence_fact(request.series_id))
        for window in self.ticker_replacements:
            start = self._reference_snapshot(
                window.new_ticker, window.from_date.isoformat()
            )
            end = self._reference_snapshot(
                window.new_ticker, window.to_date.isoformat()
            )
            if start and end:
                fact = ticker_replacement_fact_from_target_valid_alias_window(
                    request.series_id,
                    target,
                    old_ticker=window.old_ticker,
                    new_ticker=window.new_ticker,
                    from_date=window.from_date.isoformat(),
                    to_date=window.to_date.isoformat(),
                    start_reference=start,
                    end_reference=end,
                    replacement_reason=window.replacement_reason,
                    event_date=window.event_date.isoformat()
                    if window.event_date
                    else None,
                )
                if fact:
                    facts.append(fact.to_evidence_fact(request.series_id))
        for window in self.handoffs:
            start = self._reference_snapshot(
                window.event_ticker, window.from_date.isoformat()
            )
            end = self._reference_snapshot(
                window.event_ticker, window.to_date.isoformat()
            )
            if start and end:
                fact = handoff_segment_fact_from_target_valid_event_window(
                    request.series_id,
                    target,
                    event_ticker=window.event_ticker,
                    from_date=window.from_date.isoformat(),
                    to_date=window.to_date.isoformat(),
                    start_reference=start,
                    end_reference=end,
                    candidate_ticker=window.candidate_ticker,
                    event_date=window.event_date.isoformat()
                    if window.event_date
                    else None,
                )
                if fact:
                    facts.append(fact.to_evidence_fact(request.series_id))
        return tuple(facts)

    def _reference_snapshot(
        self, ticker: str, as_of_date: str
    ) -> ReferenceSnapshot | None:
        for probe in self.reference_boundary_probes:
            if probe.ticker == ticker and probe.as_of_date.isoformat() == as_of_date:
                return probe.snapshot
        return None

    def _bar_probe(
        self, ticker: str, from_date: str, to_date: str
    ) -> BarProbeResult | None:
        for probe in self.bar_probes:
            if (
                probe.ticker == ticker
                and probe.from_date.isoformat() == from_date
                and probe.to_date.isoformat() == to_date
            ):
                return probe
        return None

    def _identity_scan(self, query: str, as_of_date: str) -> IdentityScanResult | None:
        for scan in self.identity_scans:
            if scan.query == query and scan.as_of_date.isoformat() == as_of_date:
                return scan
        return None
