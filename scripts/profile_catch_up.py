#!/usr/bin/env python3
"""Profile a bounded sequential catch-up sample against the stock-universe DB."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stock_universe.domain import BackfillPlan, EvidenceNeeded
from stock_universe.evidence.normalizers import reference_boundary_fact_from_snapshot
from stock_universe.executors import ExecutionApproval, ProviderEntitlementUnavailable
from stock_universe.executors.backfill_executor import validate_approved_plan
from stock_universe.executors.live_bar_executor import (
    PROVIDER_ENTITLEMENT_SKIP_REASON,
    LiveExecutionReceipt,
    _fetch_segment_bars,
    _request_log_payload,
)
from stock_universe.paths import CANONICAL_DB_PATH
from stock_universe.providers import (
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    MassiveRequestRecord,
)
from stock_universe.providers.massive.payloads import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _reference_snapshot_from_payload,
)
from stock_universe.providers.massive.reference_helpers import (
    START_GAP_BAR_SCAN_LIMIT,
    _reference_boundary_fact_with_historical_rekey,
)
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    SqlEvent,
    set_sql_event_handler,
)
from stock_universe.workflows import (
    build_catch_up_plan,
    massive_live_source_from_series_id,
    run_backfill_source_dry_run_trace,
)


DEFAULT_RUN_ROOT = CANONICAL_DB_PATH.parent / "catch_up_profiles"


@dataclass
class SqlCollector:
    events: list[SqlEvent] = field(default_factory=list)

    def handler(self, event: SqlEvent) -> None:
        if event.phase in {"after", "error"}:
            self.events.append(event)

    def mark(self) -> int:
        return len(self.events)

    def summary_since(self, index: int, *, threshold_seconds: float) -> dict[str, Any]:
        events = self.events[index:]
        total_seconds = sum(float(event.duration_seconds or 0) for event in events)
        by_operation: dict[str, dict[str, Any]] = {}
        for event in events:
            bucket = by_operation.setdefault(
                event.operation or "unknown", {"count": 0, "seconds": 0.0}
            )
            bucket["count"] += 1
            bucket["seconds"] += float(event.duration_seconds or 0)
        slow = [
            _sql_event_payload(event)
            for event in events
            if float(event.duration_seconds or 0) >= threshold_seconds
        ]
        return {
            "count": len(events),
            "seconds": round(total_seconds, 6),
            "by_operation": {
                key: {"count": value["count"], "seconds": round(value["seconds"], 6)}
                for key, value in sorted(by_operation.items())
            },
            "slow": slow[:20],
            "slow_count": len(slow),
        }


@contextmanager
def timed_phase(
    target_profile: dict[str, Any],
    name: str,
    *,
    sql: SqlCollector,
    sql_threshold_seconds: float,
    http_detail_limit: int = 1000,
    client: Any = None,
) -> Iterator[dict[str, Any]]:
    phase: dict[str, Any] = {"name": name, "ok": True}
    sql_mark = sql.mark()
    request_mark = len(client.request_log) if client is not None else 0
    started = time.perf_counter()
    try:
        yield phase
    except Exception as exc:
        phase["ok"] = False
        phase["error_type"] = exc.__class__.__name__
        phase["error"] = str(exc)
        raise
    finally:
        phase["seconds"] = round(time.perf_counter() - started, 6)
        phase["sql"] = sql.summary_since(
            sql_mark, threshold_seconds=sql_threshold_seconds
        )
        if client is not None:
            new_requests = client.request_log[request_mark:]
            phase["http"] = _request_summary(
                new_requests, detail_limit=http_detail_limit
            )
        target_profile.setdefault("phases", []).append(phase)


@contextmanager
def boundary_search_mode(mode: str) -> Iterator[None]:
    if mode == "current":
        yield
        return
    if mode != "legacy-linear-260":
        raise ValueError(f"unsupported boundary search mode: {mode}")

    from stock_universe.providers.massive import alias_history as alias_history_module
    from stock_universe.providers.massive import (
        reference_boundary as reference_boundary_module,
    )

    original_start_gap = (
        reference_boundary_module._first_bar_boundary_fact_after_start_gap
    )
    original_suffix_start = (
        alias_history_module.MassiveAliasHistoryProvider._target_valid_bar_suffix_start
    )
    reference_boundary_module._first_bar_boundary_fact_after_start_gap = (
        _legacy_first_bar_boundary_fact_after_start_gap
    )
    alias_history_module.MassiveAliasHistoryProvider._target_valid_bar_suffix_start = (
        _legacy_target_valid_bar_suffix_start
    )
    try:
        yield
    finally:
        reference_boundary_module._first_bar_boundary_fact_after_start_gap = (
            original_start_gap
        )
        alias_history_module.MassiveAliasHistoryProvider._target_valid_bar_suffix_start = original_suffix_start


def _legacy_first_bar_boundary_fact_after_start_gap(
    client: MassiveReadOnlyClient,
    request: Any,
    target: Any,
    ticker: str,
    from_date: str,
    to_date: str,
) -> Any | None:
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    dates = _bar_dates_from_payload(bars_payload)
    if not dates or dates[0] <= from_date:
        return None
    for date in dates[:START_GAP_BAR_SCAN_LIMIT]:
        reference_payload = client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": date},
        )
        snapshot = _reference_snapshot_from_payload(ticker, date, reference_payload)
        fact = _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            snapshot,
            point="start",
            source="massive.reference_start_gap_first_valid_bar_boundary",
        )
        if fact.matched is True:
            return fact
    return None


def _legacy_target_valid_bar_suffix_start(
    self: Any,
    request: Any,
    target: Any,
    ticker: str,
    bar_dates: tuple[str, ...],
    **_: Any,
) -> tuple[str, Any] | None:
    for date in bar_dates[:START_GAP_BAR_SCAN_LIMIT]:
        payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": date},
        )
        fact = reference_boundary_fact_from_snapshot(
            request.series_id,
            target,
            _reference_snapshot_from_payload(ticker, date, payload),
            point="start",
            source="massive.alias_history.first_target_valid_bar_boundary",
        )
        if fact.matched is True:
            return date, fact
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.commit and not os.environ.get("MASSIVE_API_KEY") and not args.api_key:
        print(
            "profile-catch-up: MASSIVE_API_KEY or --api-key is required for --commit",
            file=sys.stderr,
        )
        return 2

    run_dir = Path(args.run_dir) if args.run_dir else _default_run_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    sql = SqlCollector()
    set_sql_event_handler(sql.handler)
    started = _utc_now()
    try:
        payload = _profile(args, run_dir=run_dir, sql=sql, started_at_utc=started)
    finally:
        set_sql_event_handler(None)

    profile_path = run_dir / "profile.json"
    profile_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary = _summary_payload(payload, profile_path=profile_path)
    print(json.dumps(summary, indent=2))
    return 0 if payload["ok"] else 1


def _profile(
    args: argparse.Namespace,
    *,
    run_dir: Path,
    sql: SqlCollector,
    started_at_utc: str,
) -> dict[str, Any]:
    top_profile: dict[str, Any] = {"phases": []}
    with timed_phase(
        top_profile,
        "build_catch_up_plan",
        sql=sql,
        sql_threshold_seconds=args.sql_threshold_seconds,
    ):
        materialized_target_limit = (
            args.target_limit + args.target_offset if args.target_limit else 0
        )
        plan = build_catch_up_plan(
            args.db,
            workers=1,
            batch_size=1,
            stale_before=args.stale_before,
            categories=tuple(args.category),
            exchanges=tuple(args.exchange),
            security_types=tuple(args.security_type),
            series_ids=tuple(args.ohlcv_series_id),
            tickers=tuple(args.ticker),
            target_limit=materialized_target_limit,
            from_date=args.from_date,
            to_date=args.to_date,
            run_dir=run_dir / "catch_up_artifacts",
        )

    selected_targets = plan.targets[args.target_offset :]
    if args.target_limit:
        selected_targets = selected_targets[: args.target_limit]

    targets = []
    ok = True
    for index, target in enumerate(selected_targets, start=1):
        print(
            "profile-catch-up: target "
            f"{index}/{len(selected_targets)} ohlcv_series_id={target.ohlcv_series_id} "
            f"ticker={target.ticker} category={target.category}",
            file=sys.stderr,
            flush=True,
        )
        target_profile = _profile_target(args, target, sql=sql, run_dir=run_dir)
        targets.append(target_profile)
        ok = ok and target_profile["status"] in {"ok", "skipped", "planned"}
        if target_profile["status"] == "error" and args.fail_fast:
            break

    return {
        "ok": ok,
        "command": "profile-catch-up",
        "commit": bool(args.commit),
        "db": str(Path(args.db)),
        "run_dir": str(run_dir),
        "started_at_utc": started_at_utc,
        "finished_at_utc": _utc_now(),
        "plan": {
            "plan_hash": plan.plan_hash,
            "generated_at_utc": plan.generated_at_utc,
            "target_count": len(plan.targets),
            "target_policy": plan.target_policy,
            "quality_audit_summary": plan.quality_audit_summary,
        },
        "settings": {
            "target_limit": args.target_limit,
            "max_rounds": args.max_rounds,
            "timeout_seconds": args.timeout_seconds,
            "http_detail_limit": args.http_detail_limit,
            "sql_threshold_seconds": args.sql_threshold_seconds,
            "target_offset": args.target_offset,
            "boundary_search_mode": args.boundary_search_mode,
        },
        "top_level_phases": top_profile["phases"],
        "phase_totals": _phase_totals(top_profile["phases"], targets),
        "targets": targets,
        "notes": [
            "SQL timings are process-level managed sqlite execute timings.",
            "HTTP elapsed seconds are measured by MassiveReadOnlyClient around each request.",
            "Use --commit to include approval, bar fetch, bar insert, and receipt insert phases.",
        ],
    }


def _profile_target(
    args: argparse.Namespace,
    target: Any,
    *,
    sql: SqlCollector,
    run_dir: Path,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "ohlcv_series_id": target.ohlcv_series_id,
        "ticker": target.ticker,
        "category": target.category,
        "from_date": target.from_date,
        "max_bar_date": target.max_bar_date,
        "snapshot_as_of_date": target.snapshot_as_of_date,
        "status": "planned",
        "phases": [],
    }
    target_started = time.perf_counter()
    client = None
    try:
        with boundary_search_mode(args.boundary_search_mode):
            with timed_phase(
                profile,
                "series_source_setup",
                sql=sql,
                sql_threshold_seconds=args.sql_threshold_seconds,
            ):
                capture_dir = (
                    run_dir / "raw" / str(target.ohlcv_series_id)
                    if args.capture_raw
                    else None
                )
                api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
                if not api_key:
                    raise ValueError("MASSIVE_API_KEY or --api-key is required")
                client = MassiveReadOnlyClient(
                    MassiveProviderConfig(
                        api_key=api_key,
                        base_url=args.base_url,
                        timeout_seconds=args.timeout_seconds,
                    ),
                    raw_capture_dir=capture_dir,
                )
                source, client, snapshot = massive_live_source_from_series_id(
                    args.db,
                    target.ohlcv_series_id,
                    from_date=target.from_date,
                    to_date=args.to_date,
                    as_of_date=target.snapshot_as_of_date,
                    client=client,
                )
            profile["selected_identity"] = {
                "ticker": snapshot.ticker,
                "company_name": snapshot.company_name,
                "security_type": snapshot.security_type,
                "primary_exchange": snapshot.primary_exchange,
                "cik": snapshot.cik,
            }

            with timed_phase(
                profile,
                "planning",
                sql=sql,
                sql_threshold_seconds=args.sql_threshold_seconds,
                http_detail_limit=args.http_detail_limit,
                client=client,
            ):
                trace = run_backfill_source_dry_run_trace(
                    source, max_rounds=args.max_rounds
                )
        result = trace.result
        profile["planning_round_count"] = len(trace.rounds)
        profile["planning_fact_count"] = sum(
            len(item.collected_facts) for item in trace.rounds
        )
        if not isinstance(result, BackfillPlan):
            profile["status"] = "skipped"
            profile["reason"] = "planner returned EvidenceNeeded"
            if isinstance(result, EvidenceNeeded):
                profile["unresolved_evidence"] = _evidence_needed_payload(result)
            profile["total_seconds"] = round(time.perf_counter() - target_started, 6)
            return profile
        profile["plan_status"] = result.status
        profile["segment_count"] = len(result.segments)
        if result.status == "blocked":
            profile["status"] = "skipped"
            profile["reason"] = "blocked plans are not executable"
            profile["total_seconds"] = round(time.perf_counter() - target_started, 6)
            return profile
        if result.status == "caution" and args.no_caution:
            profile["status"] = "skipped"
            profile["reason"] = "caution plan skipped by --no-caution"
            profile["total_seconds"] = round(time.perf_counter() - target_started, 6)
            return profile
        if not args.commit:
            profile["status"] = "planned"
            profile["reason"] = (
                "dry-run profile; pass --commit to execute writes and aggregate fetches"
            )
            profile["total_seconds"] = round(time.perf_counter() - target_started, 6)
            return profile

        _execute_profiled_plan(
            args,
            profile,
            result,
            trace,
            source,
            client,
            sql=sql,
        )
        return profile
    except Exception as exc:
        profile["status"] = "error"
        profile["error_type"] = exc.__class__.__name__
        profile["error"] = str(exc)
        return profile
    finally:
        profile["total_seconds"] = round(time.perf_counter() - target_started, 6)
        if client is not None:
            profile["request_count"] = len(client.request_log)
            profile["http_elapsed_seconds"] = round(
                sum(float(item.elapsed_seconds) for item in client.request_log),
                6,
            )
            profile["http"] = _request_summary(
                client.request_log,
                detail_limit=args.http_detail_limit,
            )


def _execute_profiled_plan(
    args: argparse.Namespace,
    profile: dict[str, Any],
    result: BackfillPlan,
    trace: Any,
    source: Any,
    client: Any,
    *,
    sql: SqlCollector,
) -> None:
    repository = SQLiteStockUniverseRepository(args.db)
    approval = ExecutionApproval(
        request_hash=result.request.request_hash,
        allow_caution=result.status == "caution",
        approved_by="stock-universe catch-up-profile",
    )
    with timed_phase(
        profile,
        "approval_insert",
        sql=sql,
        sql_threshold_seconds=args.sql_threshold_seconds,
    ):
        approval_record = repository.insert_execution_approval(
            result, approval, reason="catch-up profiling"
        )
    validate_approved_plan(result, approval)

    evidence_facts = source.base_facts + tuple(
        fact for item in trace.rounds for fact in item.collected_facts
    )
    fetched = []
    inserted = 0
    started = _utc_now()
    try:
        with timed_phase(
            profile,
            "persist_plan_context",
            sql=sql,
            sql_threshold_seconds=args.sql_threshold_seconds,
        ):
            repository.persist_plan_context(result, evidence_facts=evidence_facts)
        for index, segment in enumerate(result.segments, start=1):
            with timed_phase(
                profile,
                f"aggregate_fetch_segment_{index}",
                sql=sql,
                sql_threshold_seconds=args.sql_threshold_seconds,
                http_detail_limit=args.http_detail_limit,
                client=client,
            ):
                segment_bars = _fetch_segment_bars(result, segment, client)
            fetched.extend(segment_bars)
        profile["fetched_bar_count"] = len(fetched)
        with timed_phase(
            profile,
            "insert_bars",
            sql=sql,
            sql_threshold_seconds=args.sql_threshold_seconds,
        ):
            inserted = repository.insert_bars(fetched)
        profile["inserted_bar_count"] = inserted
        receipt = LiveExecutionReceipt(
            ok=True,
            request_hash=result.request.request_hash,
            evidence_ledger_hash=result.evidence_ledger_hash,
            ohlcv_series_id=result.target.ohlcv_series_id,
            planned_segment_count=len(result.segments),
            fetched_bar_count=len(fetched),
            inserted_bar_count=inserted,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            request_log=_request_log_payload(client),
        )
        with timed_phase(
            profile,
            "insert_receipt",
            sql=sql,
            sql_threshold_seconds=args.sql_threshold_seconds,
        ):
            repository.insert_execution_receipt(
                receipt.to_dict()
                | {
                    "approved_by": approval.approved_by,
                    "approval_hash": approval_record["approval_hash"],
                }
            )
        profile["status"] = "ok"
    except ProviderEntitlementUnavailable as exc:
        profile["status"] = "skipped"
        profile["reason"] = PROVIDER_ENTITLEMENT_SKIP_REASON
        profile["provider_status"] = exc.provider_status
        receipt = LiveExecutionReceipt(
            ok=False,
            status="skipped",
            request_hash=result.request.request_hash,
            evidence_ledger_hash=result.evidence_ledger_hash,
            ohlcv_series_id=result.target.ohlcv_series_id,
            planned_segment_count=len(result.segments),
            fetched_bar_count=len(fetched),
            inserted_bar_count=inserted,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            request_log=_request_log_payload(client),
            skip_reason=PROVIDER_ENTITLEMENT_SKIP_REASON,
            provider_status=exc.provider_status,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
        with timed_phase(
            profile,
            "insert_receipt",
            sql=sql,
            sql_threshold_seconds=args.sql_threshold_seconds,
        ):
            repository.insert_execution_receipt(
                receipt.to_dict()
                | {
                    "approved_by": approval.approved_by,
                    "approval_hash": approval_record["approval_hash"],
                }
            )
    except Exception as exc:
        profile["status"] = "error"
        profile["error_type"] = exc.__class__.__name__
        profile["error"] = str(exc)
        receipt = LiveExecutionReceipt(
            ok=False,
            request_hash=result.request.request_hash,
            evidence_ledger_hash=result.evidence_ledger_hash,
            ohlcv_series_id=result.target.ohlcv_series_id,
            planned_segment_count=len(result.segments),
            fetched_bar_count=len(fetched),
            inserted_bar_count=inserted,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            request_log=_request_log_payload(client),
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
        try:
            with timed_phase(
                profile,
                "insert_receipt",
                sql=sql,
                sql_threshold_seconds=args.sql_threshold_seconds,
            ):
                repository.insert_execution_receipt(
                    receipt.to_dict()
                    | {
                        "approved_by": approval.approved_by,
                        "approval_hash": approval_record["approval_hash"],
                    }
                )
        finally:
            raise


def _phase_totals(
    top_phases: list[dict[str, Any]], targets: list[dict[str, Any]]
) -> dict[str, Any]:
    totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "count": 0,
            "seconds": 0.0,
            "http_seconds": 0.0,
            "sql_seconds": 0.0,
            "local_seconds": 0.0,
        }
    )
    for phase in top_phases:
        _add_phase_total(totals, phase)
    for target in targets:
        for phase in target.get("phases") or []:
            _add_phase_total(totals, phase)
    return {
        name: {
            "count": value["count"],
            "seconds": round(value["seconds"], 6),
            "http_seconds": round(value["http_seconds"], 6),
            "sql_seconds": round(value["sql_seconds"], 6),
            "local_seconds": round(value["local_seconds"], 6),
        }
        for name, value in sorted(
            totals.items(), key=lambda item: item[1]["seconds"], reverse=True
        )
    }


def _add_phase_total(totals: dict[str, dict[str, Any]], phase: dict[str, Any]) -> None:
    bucket = totals[phase["name"]]
    bucket["count"] += 1
    bucket["seconds"] += float(phase.get("seconds") or 0)
    bucket["http_seconds"] += float(
        ((phase.get("http") or {}).get("elapsed_seconds")) or 0
    )
    bucket["sql_seconds"] += float(((phase.get("sql") or {}).get("seconds")) or 0)
    bucket["local_seconds"] += max(
        0.0,
        float(phase.get("seconds") or 0)
        - float(((phase.get("http") or {}).get("elapsed_seconds")) or 0)
        - float(((phase.get("sql") or {}).get("seconds")) or 0),
    )


def _request_summary(
    records: list[MassiveRequestRecord], *, detail_limit: int
) -> dict[str, Any]:
    exact_duplicates = _exact_duplicate_summary(records)
    repeated_endpoints = _repeated_endpoint_summary(records)
    request_details = [_request_payload(item) for item in records[:detail_limit]]
    return {
        "count": len(records),
        "elapsed_seconds": round(
            sum(float(item.elapsed_seconds) for item in records), 6
        ),
        "statuses": _counts(str(item.api_status or "") for item in records),
        "http_codes": _counts(str(item.http_code or "") for item in records),
        "endpoint_families": _request_group_summary(
            records, key_fn=lambda item: _endpoint_family(item.endpoint)
        ),
        "endpoints": _request_group_summary(records, key_fn=lambda item: item.endpoint),
        "repeated_endpoints": repeated_endpoints,
        "exact_duplicates": exact_duplicates,
        "slowest_requests": _slowest_requests(records, limit=20),
        "requests": request_details,
        "request_detail_count": len(request_details),
        "request_details_truncated": len(records) > detail_limit,
    }


def _request_group_summary(
    records: list[MassiveRequestRecord], *, key_fn: Any
) -> list[dict[str, Any]]:
    grouped: dict[str, list[MassiveRequestRecord]] = defaultdict(list)
    for record in records:
        grouped[str(key_fn(record))].append(record)
    return [
        _request_group_payload(name, items)
        for name, items in sorted(
            grouped.items(),
            key=lambda item: (
                sum(float(record.elapsed_seconds) for record in item[1]),
                len(item[1]),
                item[0],
            ),
            reverse=True,
        )
    ]


def _request_group_payload(
    name: str, records: list[MassiveRequestRecord]
) -> dict[str, Any]:
    total_seconds = sum(float(item.elapsed_seconds) for item in records)
    return {
        "name": name,
        "count": len(records),
        "elapsed_seconds": round(total_seconds, 6),
        "avg_seconds": round(total_seconds / len(records), 6) if records else 0.0,
        "max_seconds": round(
            max((float(item.elapsed_seconds) for item in records), default=0.0), 6
        ),
        "statuses": _counts(str(item.api_status or "") for item in records),
        "http_codes": _counts(str(item.http_code or "") for item in records),
    }


def _exact_duplicate_summary(records: list[MassiveRequestRecord]) -> dict[str, Any]:
    grouped: dict[
        tuple[str, tuple[tuple[str, str], ...]], list[MassiveRequestRecord]
    ] = defaultdict(list)
    for record in records:
        grouped[_request_signature(record)].append(record)
    duplicate_groups = [
        (signature, items) for signature, items in grouped.items() if len(items) > 1
    ]
    duplicate_payloads = []
    extra_elapsed = 0.0
    for signature, items in duplicate_groups:
        extra_elapsed += sum(float(item.elapsed_seconds) for item in items[1:])
        duplicate_payloads.append(_duplicate_group_payload(signature, items))
    duplicate_payloads.sort(
        key=lambda item: (item["extra_elapsed_seconds"], item["extra_count"]),
        reverse=True,
    )
    return {
        "group_count": len(duplicate_groups),
        "request_count": sum(len(items) for _, items in duplicate_groups),
        "extra_count": sum(len(items) - 1 for _, items in duplicate_groups),
        "elapsed_seconds": round(
            sum(
                sum(float(item.elapsed_seconds) for item in items)
                for _, items in duplicate_groups
            ),
            6,
        ),
        "extra_elapsed_seconds": round(extra_elapsed, 6),
        "groups": duplicate_payloads[:50],
        "groups_truncated": len(duplicate_payloads) > 50,
    }


def _duplicate_group_payload(
    signature: tuple[str, tuple[tuple[str, str], ...]],
    records: list[MassiveRequestRecord],
) -> dict[str, Any]:
    endpoint, params = signature
    total_seconds = sum(float(item.elapsed_seconds) for item in records)
    extra_seconds = sum(float(item.elapsed_seconds) for item in records[1:])
    return {
        "endpoint": endpoint,
        "params_without_api_key": dict(params),
        "count": len(records),
        "extra_count": max(0, len(records) - 1),
        "elapsed_seconds": round(total_seconds, 6),
        "extra_elapsed_seconds": round(extra_seconds, 6),
        "elapsed_values": [round(float(item.elapsed_seconds), 6) for item in records],
        "statuses": _counts(str(item.api_status or "") for item in records),
        "http_codes": _counts(str(item.http_code or "") for item in records),
    }


def _repeated_endpoint_summary(records: list[MassiveRequestRecord]) -> dict[str, Any]:
    grouped: dict[str, list[MassiveRequestRecord]] = defaultdict(list)
    for record in records:
        grouped[record.endpoint].append(record)
    repeated = [
        _request_group_payload(endpoint, items)
        for endpoint, items in grouped.items()
        if len(items) > 1
    ]
    repeated.sort(
        key=lambda item: (item["elapsed_seconds"], item["count"]), reverse=True
    )
    return {
        "group_count": len(repeated),
        "request_count": sum(item["count"] for item in repeated),
        "elapsed_seconds": round(
            sum(float(item["elapsed_seconds"]) for item in repeated), 6
        ),
        "groups": repeated[:50],
        "groups_truncated": len(repeated) > 50,
    }


def _slowest_requests(
    records: list[MassiveRequestRecord], *, limit: int
) -> list[dict[str, Any]]:
    return [
        _request_payload(item)
        for item in sorted(
            records, key=lambda record: float(record.elapsed_seconds), reverse=True
        )[:limit]
    ]


def _request_payload(record: MassiveRequestRecord) -> dict[str, Any]:
    return {
        "endpoint_family": _endpoint_family(record.endpoint),
        "endpoint": record.endpoint,
        "params_without_api_key": dict(_request_params(record.params_without_api_key)),
        "http_code": record.http_code,
        "api_status": record.api_status,
        "elapsed_seconds": round(float(record.elapsed_seconds), 6),
    }


def _request_signature(
    record: MassiveRequestRecord,
) -> tuple[str, tuple[tuple[str, str], ...]]:
    return (record.endpoint, _request_params(record.params_without_api_key))


def _request_params(params: Any) -> tuple[tuple[str, str], ...]:
    if isinstance(params, dict):
        items = params.items()
    else:
        items = params or ()
    return tuple(sorted((str(key), str(value)) for key, value in items))


def _endpoint_family(endpoint: str) -> str:
    if endpoint == "/v3/reference/tickers":
        return "reference_search"
    if endpoint.startswith("/v3/reference/tickers/"):
        return "reference_detail"
    if endpoint.startswith("/vX/reference/tickers/") and endpoint.endswith("/events"):
        return "ticker_events"
    if endpoint.startswith("/v2/aggs/ticker/"):
        return "aggregate_bars"
    return "other"


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _summary_payload(payload: dict[str, Any], *, profile_path: Path) -> dict[str, Any]:
    status_counts = _counts(target.get("status") for target in payload["targets"])
    return {
        "ok": payload["ok"],
        "commit": payload["commit"],
        "db": payload["db"],
        "run_dir": payload["run_dir"],
        "profile_path": str(profile_path),
        "target_count": len(payload["targets"]),
        "status_counts": status_counts,
        "time_breakdown": _time_breakdown(payload),
        "phase_totals": payload["phase_totals"],
        "http_breakdown": _http_breakdown(payload),
        "http_totals_by_target": [
            _target_http_summary(target)
            for target in payload["targets"]
            if target.get("http")
        ],
        "targets": [
            {
                "ohlcv_series_id": target["ohlcv_series_id"],
                "ticker": target["ticker"],
                "category": target["category"],
                "status": target["status"],
                "total_seconds": target.get("total_seconds"),
                "http_elapsed_seconds": target.get("http_elapsed_seconds", 0),
                "request_count": target.get("request_count", 0),
                "exact_duplicate_extra_requests": (
                    ((target.get("http") or {}).get("exact_duplicates") or {}).get(
                        "extra_count", 0
                    )
                ),
                "exact_duplicate_extra_seconds": (
                    ((target.get("http") or {}).get("exact_duplicates") or {}).get(
                        "extra_elapsed_seconds", 0
                    )
                ),
                "slowest_request_seconds": (
                    (
                        ((target.get("http") or {}).get("slowest_requests") or [{}])[0]
                    ).get("elapsed_seconds", 0)
                ),
                "fetched_bar_count": target.get("fetched_bar_count", 0),
                "inserted_bar_count": target.get("inserted_bar_count", 0),
                "reason": target.get("reason", ""),
                "unresolved_evidence": target.get("unresolved_evidence", {}),
                "error": target.get("error", ""),
            }
            for target in payload["targets"]
        ],
    }


def _time_breakdown(payload: dict[str, Any]) -> dict[str, Any]:
    phase_totals = payload.get("phase_totals") or {}
    total_seconds = sum(
        float(item.get("seconds") or 0) for item in phase_totals.values()
    )
    http_seconds = sum(
        float(item.get("http_seconds") or 0) for item in phase_totals.values()
    )
    sql_seconds = sum(
        float(item.get("sql_seconds") or 0) for item in phase_totals.values()
    )
    local_seconds = sum(
        float(item.get("local_seconds") or 0) for item in phase_totals.values()
    )
    return {
        "profiled_phase_seconds": round(total_seconds, 6),
        "http_seconds": round(http_seconds, 6),
        "sql_seconds": round(sql_seconds, 6),
        "local_seconds": round(local_seconds, 6),
        "http_percent": _percent(http_seconds, total_seconds),
        "sql_percent": _percent(sql_seconds, total_seconds),
        "local_percent": _percent(local_seconds, total_seconds),
    }


def _http_breakdown(payload: dict[str, Any]) -> dict[str, Any]:
    family_totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "elapsed_seconds": 0.0, "max_seconds": 0.0}
    )
    total_count = 0
    total_seconds = 0.0
    exact_duplicate_extra_count = 0
    exact_duplicate_extra_seconds = 0.0
    for target in payload.get("targets") or []:
        http = target.get("http") or {}
        total_count += int(http.get("count") or 0)
        total_seconds += float(http.get("elapsed_seconds") or 0)
        duplicates = http.get("exact_duplicates") or {}
        exact_duplicate_extra_count += int(duplicates.get("extra_count") or 0)
        exact_duplicate_extra_seconds += float(
            duplicates.get("extra_elapsed_seconds") or 0
        )
        for family in http.get("endpoint_families") or []:
            name = str(family.get("name") or "unknown")
            bucket = family_totals[name]
            bucket["count"] += int(family.get("count") or 0)
            bucket["elapsed_seconds"] += float(family.get("elapsed_seconds") or 0)
            bucket["max_seconds"] = max(
                float(bucket["max_seconds"]), float(family.get("max_seconds") or 0)
            )
    return {
        "request_count": total_count,
        "elapsed_seconds": round(total_seconds, 6),
        "exact_duplicate_extra_requests": exact_duplicate_extra_count,
        "exact_duplicate_extra_seconds": round(exact_duplicate_extra_seconds, 6),
        "endpoint_families": [
            {
                "name": name,
                "count": int(value["count"]),
                "elapsed_seconds": round(float(value["elapsed_seconds"]), 6),
                "avg_seconds": round(
                    float(value["elapsed_seconds"]) / int(value["count"]), 6
                )
                if int(value["count"])
                else 0.0,
                "max_seconds": round(float(value["max_seconds"]), 6),
            }
            for name, value in sorted(
                family_totals.items(),
                key=lambda item: (
                    float(item[1]["elapsed_seconds"]),
                    int(item[1]["count"]),
                ),
                reverse=True,
            )
        ],
    }


def _target_http_summary(target: dict[str, Any]) -> dict[str, Any]:
    http = target.get("http") or {}
    duplicates = http.get("exact_duplicates") or {}
    repeated = http.get("repeated_endpoints") or {}
    return {
        "ohlcv_series_id": target.get("ohlcv_series_id"),
        "ticker": target.get("ticker"),
        "category": target.get("category"),
        "request_count": http.get("count", 0),
        "elapsed_seconds": http.get("elapsed_seconds", 0),
        "endpoint_families": http.get("endpoint_families", []),
        "exact_duplicate_group_count": duplicates.get("group_count", 0),
        "exact_duplicate_extra_requests": duplicates.get("extra_count", 0),
        "exact_duplicate_extra_seconds": duplicates.get("extra_elapsed_seconds", 0),
        "repeated_endpoint_group_count": repeated.get("group_count", 0),
        "repeated_endpoint_elapsed_seconds": repeated.get("elapsed_seconds", 0),
        "slowest_requests": (http.get("slowest_requests") or [])[:5],
    }


def _percent(part: float, total: float) -> float:
    return round((part / total) * 100, 2) if total else 0.0


def _sql_event_payload(event: SqlEvent) -> dict[str, Any]:
    sql = " ".join(str(event.sql).split())
    return {
        "label": event.label,
        "operation": event.operation,
        "seconds": round(float(event.duration_seconds or 0), 6),
        "sql": sql[:300],
        "error_type": event.error_type,
        "error_message": event.error_message,
    }


def _evidence_needed_payload(result: EvidenceNeeded) -> dict[str, Any]:
    return {
        "request_count": len(result.requests),
        "request_kinds": _counts(request.kind for request in result.requests),
        "requests": [
            {
                "kind": request.kind,
                "key": list(request.key),
            }
            for request in result.requests
        ],
        "decisions": [
            {
                "rule_name": decision.rule_name,
                "outcome": decision.outcome,
                "reason": decision.reason,
                "decision_id": decision.decision_id,
            }
            for decision in result.decisions
        ],
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", default=str(CANONICAL_DB_PATH), help="SQLite database path."
    )
    parser.add_argument("--run-dir", default="", help="Profile artifact directory.")
    parser.add_argument(
        "--target-limit",
        type=int,
        default=3,
        help="Number of catch-up targets to profile.",
    )
    parser.add_argument(
        "--target-offset",
        type=int,
        default=0,
        help="Skip this many materialized catch-up targets before profiling.",
    )
    parser.add_argument(
        "--boundary-search-mode",
        choices=("current", "legacy-linear-260"),
        default="current",
        help="Boundary search implementation to use during planning.",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Quality category filter. May repeat.",
    )
    parser.add_argument(
        "--exchange",
        action="append",
        default=[],
        help="Primary exchange filter. May repeat.",
    )
    parser.add_argument(
        "--security-type",
        action="append",
        default=[],
        help="Security type filter. May repeat.",
    )
    parser.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="OHLCV series ID filter. May repeat.",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Latest ticker filter. May repeat.",
    )
    parser.add_argument(
        "--stale-before", default=None, help="Override stale-date classification."
    )
    parser.add_argument("--from-date", default=None, help="Override target from_date.")
    parser.add_argument("--to-date", default=None, help="Override target to_date.")
    parser.add_argument("--max-rounds", type=int, default=8)
    parser.add_argument("--no-caution", action="store_true")
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Execute writes and aggregate-bar fetches.",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    parser.add_argument("--base-url", default="https://api.massive.com")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=90.0,
        help="Per-request Massive HTTP timeout.",
    )
    parser.add_argument(
        "--capture-raw",
        action="store_true",
        help="Capture raw Massive responses under the run dir.",
    )
    parser.add_argument(
        "--http-detail-limit",
        type=int,
        default=1000,
        help="Maximum per-target request details stored in profile.json.",
    )
    parser.add_argument(
        "--sql-threshold-seconds",
        type=float,
        default=0.05,
        help="Slow SQL threshold included in phase details.",
    )
    args = parser.parse_args(argv)
    if args.target_limit < 1:
        parser.error("--target-limit must be positive")
    if args.target_offset < 0:
        parser.error("--target-offset must be non-negative")
    if args.max_rounds < 1:
        parser.error("--max-rounds must be positive")
    if args.sql_threshold_seconds < 0:
        parser.error("--sql-threshold-seconds must be non-negative")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.http_detail_limit < 0:
        parser.error("--http-detail-limit must be non-negative")
    return args


def _default_run_dir() -> Path:
    return DEFAULT_RUN_ROOT / dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
