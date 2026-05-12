from __future__ import annotations

import datetime as dt
from dataclasses import replace
from urllib.parse import parse_qs, urlparse

from stock_universe.domain import (
    AliasHistoryFact,
    BackfillPlan,
    BackfillRequest,
    BarProbeFact,
    EvidenceFact,
    EvidenceLedger,
    EvidenceNeeded,
    EvidenceRequest,
    HandoffSegmentFact,
    OmittedSegmentFact,
    ReferenceBoundaryFact,
    TargetIdentity,
    TerminalCoverageFact,
    TickerReplacementFact,
    TickerEventFact,
)
from stock_universe.evidence import (
    EvidenceCollectionError,
    ProviderBackfillEvidenceSource,
    collect_initial_backfill_evidence,
    collect_requested_evidence,
    bar_probe_fact_from_result,
    handoff_segment_fact_from_target_valid_event_window,
    identity_scan_fact_from_result,
    omitted_segment_fact_from_absent_reference_and_bars,
    reference_boundary_fact_from_snapshot,
    ticker_replacement_fact_from_target_valid_alias_window,
    validate_collected_backfill_facts,
)
from stock_universe.executors import (
    ExecutionApproval,
    ExecutionContractError,
    execute_live_bar_backfill,
    validate_approved_plan,
)
from stock_universe.planner import plan_backfill
from stock_universe.providers import (
    BackfillProviderSet,
    BarProbeResult,
    HandoffWindow,
    HttpJsonResponse,
    IdentityScanResult,
    MassiveAliasHistoryProvider,
    MassiveBarProbeProvider,
    MassiveCoverageAccountingProvider,
    MassiveIdentityScanProvider,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    MassiveReferenceBoundaryProvider,
    MassiveTickerEventsProvider,
    MassiveTickerReplacementProvider,
    OmittedSegmentProbe,
    ReferenceBoundaryProbe,
    ReferenceSnapshot,
    StaticBackfillFactProvider,
    StaticProviderReadFactProvider,
    TickerReplacementWindow,
    massive_read_only_provider_set,
)
from stock_universe.reports import render_backfill_plan_markdown
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.workflows import (
    run_backfill_planning_trace,
    run_backfill_source_dry_run_trace,
    run_backfill_source_planning_trace,
)
from stock_universe.xctx import (
    result_envelope,
    result_envelope_schema,
    xctx_command_schemas,
    xctx_tool_manifest,
)
from stock_universe.xctx.cli import main as xctx_main


def without_facts(ledger: EvidenceLedger, *kinds: str) -> EvidenceLedger:
    return EvidenceLedger(
        tuple(fact for fact in ledger.facts if fact.kind not in kinds)
    )


def action_names(actions: list[dict]) -> list[str]:
    return [action["name"] for action in actions]


def plan_with_allocated_lookup(
    repository: SQLiteStockUniverseRepository, plan: BackfillPlan
) -> BackfillPlan:
    series_id = repository.ensure_ohlcv_series_id(plan.target.natural_key)
    target = replace(plan.target, ohlcv_series_id=series_id)
    request = BackfillRequest(
        series_id=series_id,
        from_date=plan.request.from_date,
        to_date=plan.request.to_date,
        multiplier=plan.request.multiplier,
        timespan=plan.request.timespan,
        adjusted=plan.request.adjusted,
    )
    return replace(plan, target=target, request=request)


def boundary_probe(
    ticker: str, as_of_date: str, point: str, snapshot: ReferenceSnapshot
) -> ReferenceBoundaryProbe:
    return ReferenceBoundaryProbe(ticker, as_of_date, point, snapshot)


class FakeHttpJsonTransport:
    def __init__(self, response: HttpJsonResponse) -> None:
        self.response = response
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        return self.response


class QueueHttpJsonTransport:
    def __init__(self, responses: list[HttpJsonResponse]) -> None:
        self.responses = list(responses)
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        if not self.responses:
            raise AssertionError("no queued HTTP response")
        return self.responses.pop(0)
