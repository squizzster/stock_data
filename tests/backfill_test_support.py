from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

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
    StaticBackfillEvidenceSource,
    collect_initial_backfill_evidence,
    collect_requested_evidence,
    facts_from_legacy_plan,
    ledger_from_legacy_plan,
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
from stock_universe.reports import legacy_plan_dict, render_backfill_plan_markdown
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.workflows import (
    run_backfill_planning_trace,
    live_dry_run_base_facts_from_legacy_plan,
    massive_live_dry_run_source_from_legacy_plan,
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


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "legacy_plans"
ALL_LEGACY_FIXTURES = tuple(sorted(path.name for path in FIXTURE_DIR.glob("*.json")))


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


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


def ledger_from_static_source_without_candidates(
    legacy: dict,
    *,
    defer_kinds: tuple[str, ...] = (),
) -> EvidenceLedger:
    source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy,
        include_candidate_segments=False,
        defer_kinds=defer_kinds,
    )
    return collect_initial_backfill_evidence(source)


def provider_source_from_legacy_plan(
    legacy: dict,
    *,
    seed_provider_kinds: tuple[str, ...],
) -> ProviderBackfillEvidenceSource:
    facts = facts_from_legacy_plan(legacy, include_candidate_segments=False)
    base_kinds = {
        "backfill_request",
        "event_lookup",
        "known_aliases",
        "legacy_decision",
        "plan_metadata",
        "target_identity",
    }
    base_facts = tuple(fact for fact in facts if fact.kind in base_kinds)
    provider_facts = tuple(
        fact
        for fact in facts
        if fact.kind not in base_kinds and fact.kind != "candidate_segments"
    )
    providers = BackfillProviderSet(
        (StaticBackfillFactProvider(provider_facts, seed_provider_kinds),)
    )
    return ProviderBackfillEvidenceSource(base_facts, providers)


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


def assert_core_parity(actual: dict, legacy: dict) -> None:
    assert actual["status"] == legacy["status"]
    assert actual["target"]["ohlcv_series_id"] == legacy["target"]["ohlcv_series_id"]
    assert actual["target"]["latest_ticker"] == legacy["target"]["latest_ticker"]
    assert actual["target"]["identity_status"] == legacy["target"]["identity_status"]
    assert actual["range"] == legacy["range"]
    assert actual["warnings"] == legacy.get("warnings", [])
    assert actual["errors"] == legacy.get("errors", [])

    actual_segments = [
        {
            "segment_index": segment["segment_index"],
            "ticker": segment["ticker"],
            "from_date": segment["from_date"],
            "to_date": segment["to_date"],
            "source": segment["source"],
            "valid": segment["valid"],
        }
        for segment in actual["segments"]
    ]
    legacy_segments = [
        {
            "segment_index": segment["segment_index"],
            "ticker": segment["ticker"],
            "from_date": segment["from_date"],
            "to_date": segment["to_date"],
            "source": segment["source"],
            "valid": segment["valid"],
        }
        for segment in legacy["segments"]
    ]
    assert actual_segments == legacy_segments
