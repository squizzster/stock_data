from __future__ import annotations

import inspect
from dataclasses import replace
from pathlib import Path

from stock_universe.executors import ExecutionApproval, execute_live_bar_backfill
from stock_universe.domain import (
    BackfillPlan,
    BackfillRequest,
    PlannedSegment,
    RuleDecision,
    TargetIdentity,
)
from stock_universe.planner import plan_backfill
from stock_universe.planner import planner as planner_module
from stock_universe.providers import (
    HttpJsonResponse,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
)
from stock_universe.storage import SQLiteStockUniverseRepository


class RecordingTransport:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        return HttpJsonResponse(
            200,
            {
                "status": "OK",
                "results": [
                    {"t": 1620135000000, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100},
                ],
            },
        )


def test_planner_boundary_has_no_provider_dependency() -> None:
    signature = inspect.signature(plan_backfill)
    assert tuple(signature.parameters) == ("evidence",)

    source = inspect.getsource(planner_module)
    assert "stock_universe.providers" not in source
    assert "MassiveReadOnlyClient" not in source
    assert "MassiveProviderConfig" not in source


def test_executor_fetches_planned_segment_ticker_not_target_latest_ticker(
    tmp_path: Path,
) -> None:
    plan = _sample_plan()
    plan = replace(plan, target=replace(plan.target, latest_ticker="TARGETONLY"))
    transport = RecordingTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    plan = _plan_with_allocated_lookup(repository, plan)
    approval = ExecutionApproval(
        request_hash=plan.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(plan, approval, reason="unit test approval")

    execute_live_bar_backfill(
        plan,
        approval,
        client,
        repository,
    )

    assert "/v2/aggs/ticker/SFBC/range/" in transport.urls[0]
    assert "TARGETONLY" not in transport.urls[0]


def _plan_with_allocated_lookup(
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


def _sample_plan() -> BackfillPlan:
    target = TargetIdentity(
        ohlcv_series_id=0,
        company_name="Sound Financial Bancorp Inc.",
        cik="0001495925",
        composite_figi="BBG000SFBC01",
        share_class_figi="BBG000SFBC02",
        identity_status="permanent",
        latest_ticker="SFBC",
        latest_primary_exchange="XNAS",
        locale="us",
        market="stocks",
        natural_key="massive:composite_figi:BBG000SFBC01",
        security_type="CS",
    )
    request = BackfillRequest(
        series_id=0,
        from_date="2021-05-04",
        to_date="2021-05-04",
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    return BackfillPlan(
        request=request,
        status="safe",
        target=target,
        segments=(
            PlannedSegment(
                segment_index=1,
                ticker="SFBC",
                from_date="2021-05-04",
                to_date="2021-05-04",
                source="unit-test",
            ),
        ),
        decisions=(
            RuleDecision(
                rule_name="unit.safe",
                outcome="pass",
                segment_id="segment:1",
                reason="unit test plan is executable",
            ),
        ),
        evidence_ledger_hash="unit-ledger",
        planner_version="unit-test",
        created_at_utc="2026-05-12T00:00:00Z",
    )
