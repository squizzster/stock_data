"""Database catch-up planning and committed execution artifacts."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import os
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from stock_universe.agent_reporting import (
    catch_up_reporting_policy,
    validate_db_reporting_policy,
)
from stock_universe.domain.common import normalize_bar_grain, stable_json_hash
from stock_universe.market_calendar import (
    default_us_equity_history_start_date,
    next_us_equity_trading_date,
)
from stock_universe.paths import CANONICAL_DB_PATH
from stock_universe.quality_audit import quality_audit
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    connect_readonly_sqlite,
    readonly_db_uri,
)
from stock_universe.workflows.reference_universe import DEFAULT_SERIES_ID_SEED_FROM_DATE


CATCH_UP_PLAN_SCHEMA_VERSION = "stock_universe.catch_up_plan.v1"
CATCH_UP_RUN_SCHEMA_VERSION = "stock_universe.catch_up_run.v1"
CATCH_UP_BATCH_SCHEMA_VERSION = "stock_universe.catch_up_batch.v1"
CATCH_UP_STOP_SCHEMA_VERSION = "stock_universe.catch_up_stop.v1"
CATCH_UP_RECONCILIATION_SCHEMA_VERSION = "stock_universe.catch_up_reconciliation.v1"
DEFAULT_CATCH_UP_WORKERS = 10
MAX_CATCH_UP_WORKERS = 20
DEFAULT_CATCH_UP_BATCH_SIZE = 25
DEFAULT_CATCH_UP_RUN_ROOT = CANONICAL_DB_PATH.parent / "catch_up_runs"
DEFAULT_RESOURCE_CHECK_SECONDS = 600
DEFAULT_NO_PROGRESS_WARNING_SECONDS = 180
STALE_RUNNING_SECONDS = 180
STOP_REQUEST_FILENAME = "stop_request.json"
RECONCILIATION_FILENAME = "reconciliation.json"
DISK_WARNING_BYTES = 10 * 1024 * 1024 * 1024
DISK_CRITICAL_BYTES = 5 * 1024 * 1024 * 1024
DISK_DRAIN_BYTES = 3 * 1024 * 1024 * 1024

EXECUTABLE_CATCH_UP_CATEGORIES = {
    "bar_expected_but_missing",
    "covered_series_data_stale",
    "data_not_loaded",
    "listed_common_stock_data_stale",
    "plan_session_gap",
    "provider_zero_bar_response_stale",
}
INCREMENTAL_CATCH_UP_CATEGORIES = {
    "covered_series_data_stale",
    "listed_common_stock_data_stale",
}
REVIEW_ONLY_CATCH_UP_CATEGORIES = {
    "approved_plan_missing_receipt",
    "execution_error",
    "no_action_needed",
    "provider_not_authorized",
}
CATCH_UP_STOP_MODES = {"drain", "quiesce", "abort"}
DEFAULT_CATCH_UP_STOP_MODE = "drain"


@dataclass(frozen=True)
class CatchUpTarget:
    ohlcv_series_id: int
    ticker: str
    bar_grain: str
    multiplier: int
    timespan: str
    category: str
    from_date: str
    max_bar_date: str
    min_bar_date: str
    bar_count: int
    plan_count: int
    receipt_count: int
    snapshot_as_of_date: str
    company_name: str
    security_type: str
    primary_exchange: str
    market: str
    cik: str
    composite_figi: str
    share_class_figi: str
    suggested_next_command: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ohlcv_series_id": self.ohlcv_series_id,
            "ticker": self.ticker,
            "bar_grain": self.bar_grain,
            "multiplier": self.multiplier,
            "timespan": self.timespan,
            "category": self.category,
            "from_date": self.from_date,
            "max_bar_date": self.max_bar_date,
            "min_bar_date": self.min_bar_date,
            "bar_count": self.bar_count,
            "plan_count": self.plan_count,
            "receipt_count": self.receipt_count,
            "snapshot_as_of_date": self.snapshot_as_of_date,
            "company_name": self.company_name,
            "security_type": self.security_type,
            "primary_exchange": self.primary_exchange,
            "market": self.market,
            "cik": self.cik,
            "composite_figi": self.composite_figi,
            "share_class_figi": self.share_class_figi,
            "suggested_next_command": self.suggested_next_command,
        }


@dataclass(frozen=True)
class CatchUpBatch:
    batch_index: int
    ohlcv_series_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_index": self.batch_index,
            "target_count": len(self.ohlcv_series_ids),
            "ohlcv_series_ids": list(self.ohlcv_series_ids),
        }


@dataclass(frozen=True)
class CatchUpPlan:
    db: str
    generated_at_utc: str
    reference_snapshot_as_of_date: str
    quality_audit_summary: dict[str, Any]
    target_policy: dict[str, Any]
    targets: tuple[CatchUpTarget, ...]
    batches: tuple[CatchUpBatch, ...]
    worker_count: int
    batch_size: int
    plan_hash: str
    run_dir: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CATCH_UP_PLAN_SCHEMA_VERSION,
            "db": self.db,
            "generated_at_utc": self.generated_at_utc,
            "reference_snapshot_as_of_date": self.reference_snapshot_as_of_date,
            "quality_audit_summary": self.quality_audit_summary,
            "target_policy": self.target_policy,
            "target_count": len(self.targets),
            "targets": [target.to_dict() for target in self.targets],
            "ohlcv_series_ids": [target.ohlcv_series_id for target in self.targets],
            "batches": [batch.to_dict() for batch in self.batches],
            "worker_count": self.worker_count,
            "batch_size": self.batch_size,
            "plan_hash": self.plan_hash,
            "run_dir": self.run_dir,
            "expected_reads": [self.db, "sqlite.quality_audit"],
            "expected_writes": [],
            "commit_expected_reads": [self.db, "Massive API"],
            "commit_expected_writes": [self.db, f"{self.run_dir}/*.json"],
            "next_actions": catch_up_plan_next_actions(self),
            "repair_hints": catch_up_plan_repair_hints(self),
        }


TargetExecutor = Callable[[CatchUpTarget], dict[str, Any]]
ProgressSink = Callable[[dict[str, Any]], None]
ResourceProbe = Callable[[CatchUpPlan], dict[str, Any]]
StopProbe = Callable[[CatchUpPlan], dict[str, Any] | None]


class _CatchUpStopState:
    def __init__(self, operator_stop: dict[str, Any] | None = None) -> None:
        self._lock = threading.Lock()
        self._operator_stop = (
            _normalized_stop_request(operator_stop)
            if operator_stop is not None
            else None
        )

    def set(self, operator_stop: dict[str, Any] | None) -> None:
        if operator_stop is None:
            return
        normalized = _normalized_stop_request(operator_stop)
        with self._lock:
            if self._operator_stop is None:
                self._operator_stop = normalized

    def get(self) -> dict[str, Any] | None:
        with self._lock:
            return (
                dict(self._operator_stop) if self._operator_stop is not None else None
            )

    def stop_between_targets(self) -> bool:
        stop_request = self.get()
        return stop_request is not None and _stop_mode(stop_request) in {
            "quiesce",
            "abort",
        }


class _CatchUpActivityTracker:
    def __init__(
        self, plan: CatchUpPlan, *, started_monotonic: float, started_at_utc: str
    ) -> None:
        self._lock = threading.Lock()
        self._target_by_id = {target.ohlcv_series_id: target for target in plan.targets}
        self._started_monotonic = started_monotonic
        self._started_at_utc = started_at_utc
        self._active_batches: dict[int, dict[str, Any]] = {}
        self._last_target_completed_monotonic: float | None = None
        self._last_target_completed_at_utc = ""
        self._last_completed_target: dict[str, Any] | None = None
        self._last_batch_completed_monotonic: float | None = None
        self._last_batch_completed_at_utc = ""
        self._last_completed_batch_index: int | None = None

    def batch_started(self, batch: CatchUpBatch) -> None:
        now = time.monotonic()
        with self._lock:
            self._active_batches[batch.batch_index] = {
                "batch_index": batch.batch_index,
                "started_monotonic": now,
                "started_at_utc": _utc_now(),
                "target_count": len(batch.ohlcv_series_ids),
                "completed_target_count": 0,
                "current_target": None,
                "current_target_started_monotonic": None,
                "current_target_started_at_utc": "",
            }

    def target_started(self, batch: CatchUpBatch, target: CatchUpTarget) -> None:
        now = time.monotonic()
        with self._lock:
            active = self._active_batches.setdefault(
                batch.batch_index,
                {
                    "batch_index": batch.batch_index,
                    "started_monotonic": now,
                    "started_at_utc": _utc_now(),
                    "target_count": len(batch.ohlcv_series_ids),
                    "completed_target_count": 0,
                },
            )
            active["current_target"] = _target_progress_payload(target)
            active["current_target_started_monotonic"] = now
            active["current_target_started_at_utc"] = _utc_now()

    def target_finished(
        self, batch: CatchUpBatch, target: CatchUpTarget, result: dict[str, Any]
    ) -> None:
        now = time.monotonic()
        finished_at = _utc_now()
        with self._lock:
            active = self._active_batches.get(batch.batch_index)
            if active is not None:
                active["completed_target_count"] = (
                    int(active.get("completed_target_count") or 0) + 1
                )
                active["current_target"] = None
                active["current_target_started_monotonic"] = None
                active["current_target_started_at_utc"] = ""
            self._last_target_completed_monotonic = now
            self._last_target_completed_at_utc = finished_at
            completed_target = _target_progress_payload(target)
            completed_target["status"] = str(result.get("status") or "")
            self._last_completed_target = completed_target

    def batch_finished(self, batch: CatchUpBatch) -> None:
        now = time.monotonic()
        with self._lock:
            self._active_batches.pop(batch.batch_index, None)
            self._last_batch_completed_monotonic = now
            self._last_batch_completed_at_utc = _utc_now()
            self._last_completed_batch_index = batch.batch_index

    def snapshot(
        self, *, no_progress_warning_seconds: int, now_monotonic: float | None = None
    ) -> dict[str, Any]:
        now = now_monotonic if now_monotonic is not None else time.monotonic()
        threshold = max(1, int(no_progress_warning_seconds))
        with self._lock:
            active_batches = []
            oldest_active_batch_seconds = 0.0
            for active in sorted(
                self._active_batches.values(),
                key=lambda item: int(item.get("batch_index") or 0),
            ):
                active_seconds = max(
                    now - float(active.get("started_monotonic") or now), 0
                )
                oldest_active_batch_seconds = max(
                    oldest_active_batch_seconds, active_seconds
                )
                current_target_started = active.get("current_target_started_monotonic")
                active_batches.append(
                    {
                        "batch_index": int(active.get("batch_index") or 0),
                        "active_batch_age_seconds": round(active_seconds, 3),
                        "started_at_utc": str(active.get("started_at_utc") or ""),
                        "target_count": int(active.get("target_count") or 0),
                        "completed_target_count": int(
                            active.get("completed_target_count") or 0
                        ),
                        "current_target": active.get("current_target"),
                        "current_target_age_seconds": (
                            round(max(now - float(current_target_started), 0), 3)
                            if current_target_started is not None
                            else None
                        ),
                        "current_target_started_at_utc": str(
                            active.get("current_target_started_at_utc") or ""
                        ),
                    }
                )
            last_target_seconds = max(
                now
                - (self._last_target_completed_monotonic or self._started_monotonic),
                0,
            )
            last_batch_seconds = max(
                now - (self._last_batch_completed_monotonic or self._started_monotonic),
                0,
            )
            last_progress_monotonic = max(
                self._last_target_completed_monotonic or self._started_monotonic,
                self._last_batch_completed_monotonic or self._started_monotonic,
            )
            no_progress_seconds = max(now - last_progress_monotonic, 0)
            progress_health = (
                "no_recent_completion"
                if no_progress_seconds >= threshold
                else "progress_active"
            )
            return {
                "activity": {
                    "active_batch_count": len(active_batches),
                    "active_batches": active_batches,
                    "oldest_active_batch_seconds": round(
                        oldest_active_batch_seconds, 3
                    ),
                    "last_target_completed_at_utc": self._last_target_completed_at_utc,
                    "seconds_since_last_target_completion": round(
                        last_target_seconds, 3
                    ),
                    "last_completed_target": self._last_completed_target,
                    "last_batch_completed_at_utc": self._last_batch_completed_at_utc,
                    "seconds_since_last_batch_completion": round(last_batch_seconds, 3),
                    "last_completed_batch_index": self._last_completed_batch_index,
                    "progress_health": progress_health,
                    "no_progress_warning_seconds": threshold,
                    "no_progress_seconds": round(no_progress_seconds, 3),
                    "run_started_at_utc": self._started_at_utc,
                }
            }


def build_catch_up_plan(
    db_path: str | Path | None = None,
    *,
    workers: int = DEFAULT_CATCH_UP_WORKERS,
    batch_size: int = DEFAULT_CATCH_UP_BATCH_SIZE,
    stale_before: str | None = None,
    categories: Iterable[str] = (),
    exchanges: Iterable[str] = (),
    security_types: Iterable[str] = (),
    series_ids: Iterable[int] = (),
    tickers: Iterable[str] = (),
    target_limit: int = 0,
    seed_from_date: str | None = DEFAULT_SERIES_ID_SEED_FROM_DATE,
    from_date: str | None = None,
    to_date: str | None = None,
    run_root: str | Path | None = None,
    run_dir: str | Path | None = None,
    bar_grain: str = "1d",
) -> CatchUpPlan:
    _validate_workers(workers)
    _validate_batch_size(batch_size)
    if target_limit < 0:
        raise ValueError("target_limit must be non-negative")
    grain = normalize_bar_grain(bar_grain)
    db = str(Path(db_path or CANONICAL_DB_PATH))
    selected_categories = tuple(
        dict.fromkeys(str(category) for category in categories if str(category))
    )
    audit = quality_audit(
        db,
        stale_before=stale_before,
        limit=10_000_000,
        categories=selected_categories,
        exchanges=tuple(exchanges),
        security_types=tuple(security_types),
        series_ids=tuple(int(series_id) for series_id in series_ids),
        tickers=tuple(tickers),
        include_healthy=False,
        bar_grain=grain.bar_grain,
    )
    targets = tuple(
        _target_from_audit_row(
            row,
            seed_from_date=seed_from_date,
            from_date_override=from_date,
        )
        for row in audit["issues"]
        if row["category"] in EXECUTABLE_CATCH_UP_CATEGORIES
    )
    if target_limit:
        targets = targets[:target_limit]
    batches = _batches_for_targets(targets, batch_size=batch_size)
    resolved_to_date = to_date or str(audit.get("global_max_bar_date") or "")
    target_policy = {
        "name": "quality-audit-executable-catch-up",
        "bar_grain": grain.bar_grain,
        "multiplier": grain.multiplier,
        "timespan": grain.timespan,
        "executable_categories": sorted(EXECUTABLE_CATCH_UP_CATEGORIES),
        "incremental_categories": sorted(INCREMENTAL_CATCH_UP_CATEGORIES),
        "review_only_categories": sorted(REVIEW_ONLY_CATCH_UP_CATEGORIES),
        "category_filter": list(selected_categories),
        "exchange_filter": list(exchanges),
        "security_type_filter": list(security_types),
        "ohlcv_series_id_filter": [int(series_id) for series_id in series_ids],
        "ticker_filter": list(tickers),
        "seed_from_date": seed_from_date,
        "from_date_override": from_date or "",
        "to_date": resolved_to_date,
        "stale_before": stale_before or audit["stale_before"],
        "target_limit": target_limit,
    }
    quality_summary = _quality_audit_summary(audit)
    hash_payload = {
        "schema_version": CATCH_UP_PLAN_SCHEMA_VERSION,
        "db": db,
        "reference_snapshot_as_of_date": audit["latest_reference_snapshot_as_of_date"],
        "quality_audit_summary": quality_summary,
        "target_policy": target_policy,
        "targets": [target.to_dict() for target in targets],
        "batches": [batch.to_dict() for batch in batches],
        "worker_count": workers,
        "batch_size": batch_size,
    }
    plan_hash = stable_json_hash(hash_payload)
    resolved_run_dir = (
        str(run_dir) if run_dir else _default_run_dir(plan_hash, run_root=run_root)
    )
    return CatchUpPlan(
        db=db,
        generated_at_utc=_utc_now(),
        reference_snapshot_as_of_date=audit["latest_reference_snapshot_as_of_date"],
        quality_audit_summary=quality_summary,
        target_policy=target_policy,
        targets=targets,
        batches=batches,
        worker_count=workers,
        batch_size=batch_size,
        plan_hash=plan_hash,
        run_dir=resolved_run_dir,
    )


def request_catch_up_stop(
    run_dir: str | Path,
    *,
    reason: str = "operator requested stop",
    requested_by: str = "operator",
    mode: str = DEFAULT_CATCH_UP_STOP_MODE,
) -> dict[str, Any]:
    stop_mode = _validate_stop_mode(mode)
    path = Path(run_dir)
    plan_path = path / "plan.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"catch-up plan artifact path is absent: {plan_path}")
    existing = _read_stop_request(path)
    if existing is not None:
        return existing
    payload = {
        "schema_version": CATCH_UP_STOP_SCHEMA_VERSION,
        "run_dir": str(path),
        "reason": reason or "operator requested stop",
        "requested_by": requested_by or "operator",
        "requested_at_utc": _utc_now(),
        "mode": stop_mode,
    }
    _write_json(path / STOP_REQUEST_FILENAME, payload)
    return payload


def reconcile_catch_up_run(
    run_dir: str | Path,
    *,
    commit: bool = False,
) -> dict[str, Any]:
    path = Path(run_dir)
    plan_path = path / "plan.json"
    status_path = path / "status.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"catch-up plan artifact path is absent: {plan_path}")
    plan_payload = _read_json(plan_path)
    plan_hash = str(plan_payload.get("plan_hash") or "")
    db = str(plan_payload.get("db") or "")
    if not db:
        raise ValueError("catch-up plan artifact lacks a DB path")
    if not Path(db).exists():
        raise FileNotFoundError(f"catch-up DB path is absent: {db}")
    persisted_status = _read_json(status_path) if status_path.exists() else {}
    progress_events = _progress_events(path / "progress.jsonl", limit=50)
    stale_running = _stale_running_status(persisted_status, progress_events)
    if _is_active_running_status(persisted_status) and not stale_running:
        return _catch_up_reconciliation_error(
            path,
            plan_payload,
            code="catch_up_run_still_active",
            detail="The catch-up run still appears active; wait for it to stop before reconciling artifacts.",
        )
    started_at_utc = str(persisted_status.get("started_at_utc") or "")
    if not started_at_utc:
        return _catch_up_reconciliation_error(
            path,
            plan_payload,
            code="catch_up_started_at_missing",
            detail="Reconciliation needs the run started_at_utc boundary for DB receipt matching.",
        )

    validation = SQLiteStockUniverseRepository(db).validate()
    if not validation.ok:
        return _catch_up_reconciliation_error(
            path,
            plan_payload,
            code="catch_up_db_validation_failed",
            detail="DB integrity needs a clean validation result before reconciliation.",
            validation={
                "ok": validation.ok,
                "checks": list(validation.checks),
                "failures": list(validation.failures),
            },
        )

    existing_batch_payloads = _existing_batch_payloads(path, plan_hash=plan_hash)
    before = _db_reconciliation(plan_payload, existing_batch_payloads, started_at_utc)
    recovered_receipts = _unartifacted_receipt_rows(
        plan_payload, existing_batch_payloads, started_at_utc
    )
    recovered_batch_payloads = _recovered_batch_payloads(
        path,
        plan_payload,
        recovered_receipts,
        reconciled_at_utc=_utc_now(),
    )
    reconciliation_path = path / RECONCILIATION_FILENAME
    did_write: list[str] = []
    if commit:
        for payload in recovered_batch_payloads:
            artifact_path = _recovered_batch_path(path, int(payload["batch_index"]))
            _write_json(artifact_path, payload)
            did_write.append(str(artifact_path))
        after = _db_reconciliation(
            plan_payload,
            _existing_batch_payloads(path, plan_hash=plan_hash),
            started_at_utc,
        )
    else:
        after = _db_reconciliation(
            plan_payload,
            existing_batch_payloads + recovered_batch_payloads,
            started_at_utc,
        )

    payload = {
        "schema_version": CATCH_UP_RECONCILIATION_SCHEMA_VERSION,
        "ok": bool(validation.ok and not after.get("requires_reconciliation")),
        "command": "stock-universe catch-up-reconcile",
        "result_type": "CatchUpReconciliation",
        "run_dir": str(path),
        "db": db,
        "plan_hash": plan_hash,
        "commit": commit,
        "dry_run": not commit,
        "started_at_utc": started_at_utc,
        "reconciled_at_utc": _utc_now(),
        "recovery_policy": "adopt_validated_db_receipts_as_recovered_artifacts",
        "validation": {
            "ok": validation.ok,
            "checks": list(validation.checks),
            "failures": list(validation.failures),
        },
        "reconciliation_before": before,
        "reconciliation_after": after,
        "recovered_series_count": sum(
            len(item.get("results") or []) for item in recovered_batch_payloads
        ),
        "recovered_batch_artifact_count": len(recovered_batch_payloads),
        "recovered_batch_artifacts": [
            str(_recovered_batch_path(path, int(item["batch_index"])))
            for item in recovered_batch_payloads
        ],
        "effects": {
            "will_read": [db, str(path)],
            "will_write": [str(reconciliation_path)]
            + [
                str(_recovered_batch_path(path, int(item["batch_index"])))
                for item in recovered_batch_payloads
            ],
            "did_write": did_write,
        },
        "next_actions": _catch_up_reconciliation_next_actions(
            plan_payload, after, committed=commit
        ),
    }
    if commit:
        _write_json(reconciliation_path, payload)
        payload["effects"]["did_write"].append(str(reconciliation_path))
    return payload


def execute_catch_up_plan(
    plan: CatchUpPlan,
    *,
    execute_target: TargetExecutor,
    strict: bool = False,
    fail_fast: bool = False,
    resume: bool = False,
    heartbeat_seconds: int = 60,
    mini_summary_seconds: int = 240,
    summary_seconds: int = 720,
    resource_check_seconds: int = DEFAULT_RESOURCE_CHECK_SECONDS,
    no_progress_warning_seconds: int = DEFAULT_NO_PROGRESS_WARNING_SECONDS,
    progress_sink: ProgressSink | None = None,
    resource_probe: ResourceProbe | None = None,
    stop_probe: StopProbe | None = None,
) -> dict[str, Any]:
    run_dir = Path(plan.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    plan_path = run_dir / "plan.json"
    if plan_path.exists() and not resume:
        existing_plan = _read_json(plan_path)
        if existing_plan.get("plan_hash") != plan.plan_hash:
            raise ValueError(f"run_dir already contains a different plan: {plan_path}")
    _write_json(plan_path, plan.to_dict())

    started_at = _utc_now()
    started_monotonic = time.monotonic()
    if resume:
        _archive_prior_stop_request(run_dir, resumed_at_utc=started_at)
    existing_batch_payloads = (
        _existing_batch_payloads(run_dir, plan_hash=plan.plan_hash) if resume else []
    )
    run_started_at = _previous_started_at(run_dir) if resume else ""
    if resume:
        reconciliation = _db_reconciliation(
            plan.to_dict(), existing_batch_payloads, run_started_at
        )
        if int(reconciliation.get("db_receipts_without_artifact_count") or 0):
            raise ValueError(
                "run has DB execution receipts awaiting recovered batch artifacts; "
                "inspect xctx catch-up-status before resuming"
            )
    resumed_series_ids = _artifact_completed_series(existing_batch_payloads)
    pending_batches = (
        _pending_batches_after_completed_series(
            plan, completed_series_ids=resumed_series_ids
        )
        if resume
        else list(plan.batches)
    )
    status_path = run_dir / "status.json"
    progress_path = run_dir / "progress.jsonl"
    probe_resources = resource_probe or _resource_snapshot
    last_resource_check = probe_resources(plan)
    resource_event_state = {"warning": False, "critical": False, "draining": False}
    resource_stop = (
        _resource_stop_payload(last_resource_check)
        if _should_drain_for_disk(last_resource_check)
        else None
    )
    operator_stop = _operator_stop_request(run_dir, plan=plan, stop_probe=stop_probe)
    stop_state = _CatchUpStopState(operator_stop)
    activity = _CatchUpActivityTracker(
        plan, started_monotonic=started_monotonic, started_at_utc=started_at
    )
    status = _run_status_payload(
        plan,
        started_at=started_at,
        finished_at="",
        state=_active_state(
            resource_stop=resource_stop,
            operator_stop=operator_stop,
            stopped_for_failure=False,
        ),
        batch_payloads=existing_batch_payloads,
        pending_batch_count=len(pending_batches),
        strict=strict,
        fail_fast=fail_fast,
        resume=resume,
        hard_error=None,
        resource_stop=resource_stop,
        operator_stop=operator_stop,
        last_resource_check=last_resource_check,
    )
    _write_json(status_path, status)
    _emit_progress_event(
        progress_path,
        _progress_event(
            "started",
            "catch-up started",
            plan=plan,
            status=status,
            started_at_monotonic=started_monotonic,
        ),
        progress_sink=progress_sink,
    )
    _emit_resource_events(
        progress_path,
        plan=plan,
        status=status,
        resource_check=last_resource_check,
        event_state=resource_event_state,
        started_at_monotonic=started_monotonic,
        progress_sink=progress_sink,
    )
    if operator_stop is not None:
        _emit_progress_event(
            progress_path,
            _progress_event(
                "operator_stop_requested",
                _operator_stop_progress_message(operator_stop),
                plan=plan,
                status=status,
                started_at_monotonic=started_monotonic,
                extra={"operator_stop": operator_stop},
            ),
            progress_sink=progress_sink,
        )

    completed_batch_payloads = list(existing_batch_payloads)
    hard_error: dict[str, Any] | None = None
    hard_error_reported = False
    stopped_for_failure = False
    next_heartbeat_at = started_monotonic + max(1, heartbeat_seconds)
    next_mini_summary_at = started_monotonic + max(1, mini_summary_seconds)
    next_summary_at = started_monotonic + max(1, summary_seconds)
    next_resource_check_at = started_monotonic + max(1, resource_check_seconds)
    with concurrent.futures.ThreadPoolExecutor(max_workers=plan.worker_count) as pool:
        in_flight: dict[concurrent.futures.Future[dict[str, Any]], CatchUpBatch] = {}
        next_batch_index = 0
        while (
            resource_stop is None
            and operator_stop is None
            and next_batch_index < len(pending_batches)
            and len(in_flight) < plan.worker_count
        ):
            batch = pending_batches[next_batch_index]
            in_flight[
                pool.submit(
                    _execute_batch, plan, batch, execute_target, activity, stop_state
                )
            ] = batch
            next_batch_index += 1
        while in_flight:
            done, _ = concurrent.futures.wait(
                tuple(in_flight),
                timeout=1,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            now_monotonic = time.monotonic()
            if now_monotonic >= next_heartbeat_at:
                activity_snapshot = activity.snapshot(
                    no_progress_warning_seconds=no_progress_warning_seconds,
                    now_monotonic=now_monotonic,
                )
                heartbeat_message = (
                    "no target or batch completed within threshold"
                    if (activity_snapshot.get("activity") or {}).get("progress_health")
                    == "no_recent_completion"
                    else "progress active"
                )
                _emit_progress_event(
                    progress_path,
                    _progress_event(
                        "heartbeat",
                        heartbeat_message,
                        plan=plan,
                        status=status,
                        started_at_monotonic=started_monotonic,
                        extra=activity_snapshot,
                    ),
                    progress_sink=progress_sink,
                )
                next_heartbeat_at = now_monotonic + max(1, heartbeat_seconds)
            if now_monotonic >= next_mini_summary_at:
                _emit_progress_event(
                    progress_path,
                    _progress_event(
                        "mini_summary",
                        "catch-up mini-summary",
                        plan=plan,
                        status=status,
                        started_at_monotonic=started_monotonic,
                    ),
                    progress_sink=progress_sink,
                )
                next_mini_summary_at = now_monotonic + max(1, mini_summary_seconds)
            if now_monotonic >= next_summary_at:
                _emit_progress_event(
                    progress_path,
                    _progress_event(
                        "summary",
                        "catch-up summary",
                        plan=plan,
                        status=status,
                        started_at_monotonic=started_monotonic,
                    ),
                    progress_sink=progress_sink,
                )
                next_summary_at = now_monotonic + max(1, summary_seconds)
            if now_monotonic >= next_resource_check_at:
                last_resource_check = probe_resources(plan)
                if (
                    _should_drain_for_disk(last_resource_check)
                    and resource_stop is None
                ):
                    resource_stop = _resource_stop_payload(last_resource_check)
                    stopped_for_failure = True
                status = _run_status_payload(
                    plan,
                    started_at=started_at,
                    finished_at="",
                    state=_active_state(
                        resource_stop=resource_stop,
                        operator_stop=operator_stop,
                        stopped_for_failure=stopped_for_failure,
                    ),
                    batch_payloads=completed_batch_payloads,
                    pending_batch_count=max(
                        len(plan.batches) - len(completed_batch_payloads), 0
                    ),
                    strict=strict,
                    fail_fast=fail_fast,
                    resume=resume,
                    hard_error=hard_error,
                    resource_stop=resource_stop,
                    operator_stop=operator_stop,
                    last_resource_check=last_resource_check,
                )
                _write_json(status_path, status)
                _emit_resource_events(
                    progress_path,
                    plan=plan,
                    status=status,
                    resource_check=last_resource_check,
                    event_state=resource_event_state,
                    started_at_monotonic=started_monotonic,
                    progress_sink=progress_sink,
                )
                next_resource_check_at = now_monotonic + max(1, resource_check_seconds)
            if operator_stop is None:
                operator_stop = _operator_stop_request(
                    run_dir, plan=plan, stop_probe=stop_probe
                )
                if operator_stop is not None:
                    stop_state.set(operator_stop)
                    status = _run_status_payload(
                        plan,
                        started_at=started_at,
                        finished_at="",
                        state=_active_state(
                            resource_stop=resource_stop,
                            operator_stop=operator_stop,
                            stopped_for_failure=stopped_for_failure,
                        ),
                        batch_payloads=completed_batch_payloads,
                        pending_batch_count=max(
                            len(plan.batches) - len(completed_batch_payloads), 0
                        ),
                        strict=strict,
                        fail_fast=fail_fast,
                        resume=resume,
                        hard_error=hard_error,
                        resource_stop=resource_stop,
                        operator_stop=operator_stop,
                        last_resource_check=last_resource_check,
                    )
                    _write_json(status_path, status)
                    _emit_progress_event(
                        progress_path,
                        _progress_event(
                            "operator_stop_requested",
                            _operator_stop_progress_message(operator_stop),
                            plan=plan,
                            status=status,
                            started_at_monotonic=started_monotonic,
                            extra={"operator_stop": operator_stop},
                        ),
                        progress_sink=progress_sink,
                    )
            if not done:
                continue
            for future in done:
                batch = in_flight.pop(future)
                if future.cancelled():
                    continue
                try:
                    batch_payload = future.result()
                except Exception as exc:
                    batch_payload = _hard_error_batch_payload(plan, batch, exc)
                    hard_error = _hard_error_payload(plan, batch, exc)
                    stopped_for_failure = True
                if operator_stop is None and batch_payload.get("operator_stop"):
                    operator_stop = _normalized_stop_request(
                        dict(batch_payload["operator_stop"])
                    )
                    stop_state.set(operator_stop)
                    _emit_progress_event(
                        progress_path,
                        _progress_event(
                            "operator_stop_requested",
                            _operator_stop_progress_message(operator_stop),
                            plan=plan,
                            status=status,
                            started_at_monotonic=started_monotonic,
                            extra={"operator_stop": operator_stop},
                        ),
                        progress_sink=progress_sink,
                    )
                _write_json(_batch_path(run_dir, batch.batch_index), batch_payload)
                completed_batch_payloads.append(batch_payload)
                if hard_error is not None and not hard_error_reported:
                    _emit_progress_event(
                        progress_path,
                        _progress_event(
                            "hard_error",
                            "hard error stopped catch-up import",
                            plan=plan,
                            status=status,
                            started_at_monotonic=started_monotonic,
                            extra={"hard_error": hard_error},
                        ),
                        progress_sink=progress_sink,
                    )
                    hard_error_reported = True
                    for pending_future in tuple(in_flight):
                        pending_future.cancel()
                if fail_fast and _batch_has_failure(batch_payload):
                    stopped_for_failure = True
                if (
                    hard_error is None
                    and resource_stop is None
                    and operator_stop is None
                    and not stopped_for_failure
                ):
                    while (
                        next_batch_index < len(pending_batches)
                        and len(in_flight) < plan.worker_count
                    ):
                        next_batch = pending_batches[next_batch_index]
                        in_flight[
                            pool.submit(
                                _execute_batch,
                                plan,
                                next_batch,
                                execute_target,
                                activity,
                                stop_state,
                            )
                        ] = next_batch
                        next_batch_index += 1
            status = _run_status_payload(
                plan,
                started_at=started_at,
                finished_at="",
                state=_active_state(
                    resource_stop=resource_stop,
                    operator_stop=operator_stop,
                    stopped_for_failure=stopped_for_failure,
                ),
                batch_payloads=completed_batch_payloads,
                pending_batch_count=max(
                    len(plan.batches) - len(completed_batch_payloads), 0
                ),
                strict=strict,
                fail_fast=fail_fast,
                resume=resume,
                hard_error=hard_error,
                resource_stop=resource_stop,
                operator_stop=operator_stop,
                last_resource_check=last_resource_check,
            )
            _write_json(status_path, status)

    finished_at = _utc_now()
    batch_payloads = _existing_batch_payloads(run_dir, plan_hash=plan.plan_hash)
    validation = SQLiteStockUniverseRepository(plan.db).validate()
    status = _run_status_payload(
        plan,
        started_at=started_at,
        finished_at=finished_at,
        state=_final_state(
            hard_error=hard_error,
            resource_stop=resource_stop,
            operator_stop=operator_stop,
        ),
        batch_payloads=batch_payloads,
        pending_batch_count=max(len(plan.batches) - len(batch_payloads), 0),
        strict=strict,
        fail_fast=fail_fast,
        resume=resume,
        hard_error=hard_error,
        resource_stop=resource_stop,
        operator_stop=operator_stop,
        last_resource_check=last_resource_check,
    )
    status["validation"] = {
        "ok": validation.ok,
        "checks": list(validation.checks),
        "failures": list(validation.failures),
    }
    counts = status["counts"]
    status["ok"] = (
        hard_error is None
        and resource_stop is None
        and operator_stop is None
        and validation.ok
        and counts["pending"] == 0
        and (not strict or (counts["error"] == 0 and counts["skipped"] == 0))
    )
    status["post_run_next_actions"] = catch_up_post_run_next_actions(plan)
    _write_json(status_path, status)
    summary_path = run_dir / "summary.json"
    _write_json(summary_path, status)
    _emit_progress_event(
        progress_path,
        _progress_event(
            "finished"
            if hard_error is None and resource_stop is None and operator_stop is None
            else "stopped",
            _final_progress_message(
                hard_error=hard_error,
                resource_stop=resource_stop,
                operator_stop=operator_stop,
            ),
            plan=plan,
            status=status,
            started_at_monotonic=started_monotonic,
            extra=_final_progress_extra(
                hard_error=hard_error,
                resource_stop=resource_stop,
                operator_stop=operator_stop,
            ),
        ),
        progress_sink=progress_sink,
    )
    return status | {
        "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
        "command": "stock-universe catch-up",
        "result_type": "CatchUpRunStatus",
        "commit": True,
        "dry_run": False,
        "effects": {
            "will_read": [plan.db, "Massive API"],
            "will_write": [plan.db, str(run_dir)],
            "did_write": [plan.db, str(plan_path), str(status_path), str(summary_path)]
            + [str(progress_path)]
            + [
                str(
                    item.get("_artifact_path")
                    or _batch_path(run_dir, int(item["batch_index"]))
                )
                for item in batch_payloads
            ],
        },
        "resumed_batch_count": len(existing_batch_payloads),
        "resumed_series_count": len(resumed_series_ids),
        "hard_error": hard_error,
        "resource_stop": resource_stop,
        "operator_stop": operator_stop,
        "last_resource_check": last_resource_check,
    }


def catch_up_run_status(run_dir: str | Path) -> dict[str, Any]:
    path = Path(run_dir)
    plan_path = path / "plan.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"catch-up plan artifact path is absent: {plan_path}")
    plan_payload = _read_json(plan_path)
    batch_payloads = _existing_batch_payloads(
        path, plan_hash=str(plan_payload.get("plan_hash") or "")
    )
    status_path = path / "status.json"
    persisted_status = _read_json(status_path) if status_path.exists() else {}
    reconciliation_artifact = _read_reconciliation_artifact(path)
    progress_events = _progress_events(path / "progress.jsonl", limit=50)
    counts = _aggregate_batch_counts(
        batch_payloads,
        target_count=int(plan_payload.get("target_count") or 0),
    )
    ok = bool(persisted_status.get("ok", counts["pending"] == 0))
    hard_error = persisted_status.get("hard_error")
    resource_stop = persisted_status.get("resource_stop")
    operator_stop = persisted_status.get("operator_stop") or _read_stop_request(path)
    persisted_state = persisted_status.get("state") or (
        "finished" if counts["pending"] == 0 else "unknown"
    )
    stale_running = _stale_running_status(persisted_status, progress_events)
    state = "stale_running" if stale_running else persisted_state
    reconciliation = _db_reconciliation(
        plan_payload,
        batch_payloads,
        str(persisted_status.get("started_at_utc") or ""),
    )
    return {
        "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
        "ok": ok
        and not stale_running
        and not bool(reconciliation.get("requires_reconciliation")),
        "result_type": "CatchUpRunStatus",
        "run_dir": str(path),
        "plan_hash": plan_payload.get("plan_hash") or "",
        "db": plan_payload.get("db") or "",
        "state": state,
        "persisted_state": persisted_state,
        "stale_running": stale_running,
        "target_count": int(plan_payload.get("target_count") or 0),
        "batch_count": len(plan_payload.get("batches") or []),
        "completed_batch_count": len(batch_payloads),
        "counts": counts,
        "started_at_utc": persisted_status.get("started_at_utc") or "",
        "finished_at_utc": persisted_status.get("finished_at_utc") or "",
        "failed_results": _failed_results(batch_payloads, limit=50),
        "hard_error": hard_error,
        "resource_stop": resource_stop,
        "operator_stop": operator_stop,
        "db_reconciliation": reconciliation,
        "reconciliation_repair": reconciliation_artifact,
        "last_resource_check": persisted_status.get("last_resource_check") or {},
        "progress_events": progress_events,
        "batch_artifacts": [
            str(
                item.get("_artifact_path")
                or _batch_path(path, int(item["batch_index"]))
            )
            for item in batch_payloads
        ],
        "plan_artifact": str(plan_path),
        "status_artifact": str(status_path),
        "reconciliation_artifact": str(path / RECONCILIATION_FILENAME)
        if reconciliation_artifact
        else "",
        "agent_reporting": catch_up_reporting_policy(
            run_dir=path,
            target_count=int(plan_payload.get("target_count") or 0),
        ),
        "post_run_next_actions": catch_up_post_run_next_actions_from_payload(
            plan_payload
        ),
        "repairs": _catch_up_status_repairs(
            plan_payload,
            hard_error=hard_error,
            resource_stop=resource_stop,
            operator_stop=operator_stop,
            stale_running=stale_running,
            reconciliation=reconciliation,
            reconciliation_artifact=reconciliation_artifact,
        ),
    }


def catch_up_runs(
    run_root: str | Path | None = None, *, limit: int = 5
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("limit must be positive")
    root = Path(run_root or DEFAULT_CATCH_UP_RUN_ROOT)
    runs: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if not root.exists():
        return {
            "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
            "ok": True,
            "result_type": "CatchUpRunList",
            "run_root": str(root),
            "limit": limit,
            "run_count": 0,
            "runs": [],
            "errors": [],
        }
    for path in _catch_up_run_dirs(root)[:limit]:
        try:
            runs.append(_catch_up_run_summary(catch_up_run_status(path)))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                {
                    "run_dir": str(path),
                    "error": str(exc),
                }
            )
    return {
        "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
        "ok": not errors,
        "result_type": "CatchUpRunList",
        "run_root": str(root),
        "limit": limit,
        "run_count": len(runs),
        "runs": runs,
        "errors": errors,
    }


def _catch_up_run_summary(status: dict[str, Any]) -> dict[str, Any]:
    progress_events = list(status.get("progress_events") or [])
    last_progress = progress_events[-1] if progress_events else {}
    reconciliation = dict(status.get("db_reconciliation") or {})
    payload = {
        "ok": bool(status.get("ok")),
        "run_dir": str(status.get("run_dir") or ""),
        "db": str(status.get("db") or ""),
        "state": str(status.get("state") or ""),
        "persisted_state": str(status.get("persisted_state") or ""),
        "stale_running": bool(status.get("stale_running")),
        "plan_hash": str(status.get("plan_hash") or ""),
        "target_count": int(status.get("target_count") or 0),
        "batch_count": int(status.get("batch_count") or 0),
        "completed_batch_count": int(status.get("completed_batch_count") or 0),
        "counts": dict(status.get("counts") or {}),
        "started_at_utc": str(status.get("started_at_utc") or ""),
        "finished_at_utc": str(status.get("finished_at_utc") or ""),
        "last_progress_at_utc": str(last_progress.get("emitted_at_utc") or ""),
        "last_progress_event_type": str(last_progress.get("event_type") or ""),
        "progress_event_count": len(progress_events),
        "requires_reconciliation": bool(reconciliation.get("requires_reconciliation")),
        "hard_error": bool(status.get("hard_error")),
        "resource_stop": bool(status.get("resource_stop")),
        "operator_stop": bool(status.get("operator_stop")),
        "plan_artifact": str(status.get("plan_artifact") or ""),
        "status_artifact": str(status.get("status_artifact") or ""),
    }
    repairs = list(status.get("repairs") or [])
    post_run_actions = list(status.get("post_run_next_actions") or [])
    if repairs:
        payload["repair_actions"] = [
            _compact_action_summary(action) for action in repairs
        ]
    if post_run_actions:
        payload["post_run_next_actions"] = [
            _compact_action_summary(action) for action in post_run_actions
        ]
    return payload


def _compact_action_summary(action: dict[str, Any]) -> dict[str, Any]:
    command = dict(action.get("command") or {})
    return {
        "name": str(action.get("name") or ""),
        "kind": str(action.get("kind") or ""),
        "command_name": str(command.get("name") or ""),
        "requires_approval": bool(action.get("requires_approval")),
        "reason": str(action.get("reason") or ""),
    }


def catch_up_plan_from_run_dir(run_dir: str | Path) -> CatchUpPlan:
    path = Path(run_dir)
    plan_path = path / "plan.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"catch-up plan artifact path is absent: {plan_path}")
    payload = _read_json(plan_path)
    return CatchUpPlan(
        db=str(payload["db"]),
        generated_at_utc=str(payload.get("generated_at_utc") or ""),
        reference_snapshot_as_of_date=str(
            payload.get("reference_snapshot_as_of_date") or ""
        ),
        quality_audit_summary=dict(payload.get("quality_audit_summary") or {}),
        target_policy=dict(payload.get("target_policy") or {}),
        targets=tuple(
            _target_from_payload(item) for item in payload.get("targets") or []
        ),
        batches=tuple(
            _batch_from_payload(item) for item in payload.get("batches") or []
        ),
        worker_count=int(payload.get("worker_count") or DEFAULT_CATCH_UP_WORKERS),
        batch_size=int(payload.get("batch_size") or DEFAULT_CATCH_UP_BATCH_SIZE),
        plan_hash=str(payload.get("plan_hash") or ""),
        run_dir=str(path),
    )


def _catch_up_status_repairs(
    plan_payload: dict[str, Any],
    *,
    hard_error: Any,
    resource_stop: Any,
    operator_stop: Any,
    stale_running: bool,
    reconciliation: dict[str, Any],
    reconciliation_artifact: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if reconciliation.get("requires_reconciliation"):
        return catch_up_reconciliation_repairs(
            plan_payload, reconciliation, stale_running=stale_running
        )
    if stale_running:
        return catch_up_stale_running_repairs(
            plan_payload, reconciled=bool(reconciliation_artifact)
        )
    if hard_error:
        return catch_up_hard_error_repairs(plan_payload, hard_error)
    if resource_stop:
        return catch_up_resource_stop_repairs(plan_payload, resource_stop)
    if operator_stop:
        return catch_up_operator_stop_repairs(plan_payload, operator_stop)
    return []


def catch_up_hard_error_repairs(
    plan_payload: dict[str, Any],
    hard_error: Any,
) -> list[dict[str, Any]]:
    db = str(plan_payload.get("db") or CANONICAL_DB_PATH)
    run_dir = str(plan_payload.get("run_dir") or "")
    return [
        {
            "name": "inspect-catch-up-status",
            "kind": "command",
            "command": {
                "name": "xctx catch-up-status",
                "description": "Read the hard-error status and completed batch artifacts.",
                "args": {"run_dir": run_dir},
                "reads": [run_dir],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": run_dir,
                    "description": "Read catch-up artifacts.",
                }
            ],
            "requires_approval": False,
            "reason": "The catch-up import stopped on a hard error.",
        },
        {
            "name": "validate-db",
            "kind": "command",
            "command": {
                "name": "stock-universe validate-db",
                "description": "Validate DB integrity before resuming catch-up.",
                "args": _db_args(db),
                "reads": [db],
                "writes": [db],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read SQLite integrity state.",
                }
            ],
            "requires_approval": True,
            "reason": str(
                (hard_error or {}).get("error")
                or "Hard error requires operator review before resume."
            ),
        },
    ]


def catch_up_resource_stop_repairs(
    plan_payload: dict[str, Any],
    resource_stop: Any,
) -> list[dict[str, Any]]:
    run_dir = str(plan_payload.get("run_dir") or "")
    checked_paths = list((resource_stop or {}).get("checked_paths") or [])
    return [
        {
            "name": "free-disk-space",
            "kind": "repair",
            "command": {
                "name": "xctx catch-up-status",
                "description": "Recheck catch-up status after freeing disk space.",
                "args": {"run_dir": run_dir},
                "reads": [run_dir],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": run_dir,
                    "description": "Read catch-up artifacts.",
                }
            ],
            "requires_approval": False,
            "reason": (
                "Catch-up stopped scheduling new work because free disk space fell below "
                f"{_format_bytes(DISK_DRAIN_BYTES)}. Checked paths: {checked_paths}"
            ),
        },
        {
            "name": "resume-catch-up",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up",
                "description": "Resume after freeing disk space; completed batch artifacts are reused.",
                "args": {"run_dir": run_dir, "commit": True, "resume": True},
                "reads": [run_dir],
                "writes": [run_dir],
            },
            "effects": [
                {
                    "kind": "write",
                    "target": run_dir,
                    "description": "Continue pending catch-up batches.",
                }
            ],
            "requires_approval": True,
            "reason": "Resume after disk pressure is resolved.",
        },
    ]


def catch_up_operator_stop_repairs(
    plan_payload: dict[str, Any],
    operator_stop: Any,
) -> list[dict[str, Any]]:
    run_dir = str(plan_payload.get("run_dir") or "")
    return [
        {
            "name": "resume-catch-up",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up",
                "description": "Resume after an operator-requested stop; completed batch artifacts are reused.",
                "args": {"run_dir": run_dir, "commit": True, "resume": True},
                "reads": [run_dir],
                "writes": [run_dir],
            },
            "effects": [
                {
                    "kind": "write",
                    "target": run_dir,
                    "description": "Continue pending catch-up batches.",
                }
            ],
            "requires_approval": True,
            "reason": str(
                (operator_stop or {}).get("reason")
                or "Catch-up stopped after an operator request."
            ),
        }
    ]


def catch_up_stale_running_repairs(
    plan_payload: dict[str, Any], *, reconciled: bool = False
) -> list[dict[str, Any]]:
    db = str(plan_payload.get("db") or CANONICAL_DB_PATH)
    run_dir = str(plan_payload.get("run_dir") or "")
    repairs = [
        {
            "name": "validate-db",
            "kind": "command",
            "command": {
                "name": "stock-universe validate-db",
                "description": "Validate DB integrity after a catch-up run stopped before final artifacts were written.",
                "args": _db_args(db),
                "reads": [db],
                "writes": [db],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read SQLite integrity state.",
                }
            ],
            "requires_approval": True,
            "reason": "The run status still says running, but the run appears stale.",
        },
        {
            "name": "inspect-catch-up-status",
            "kind": "command",
            "command": {
                "name": "xctx catch-up-status",
                "description": "Re-read stale catch-up artifacts and DB reconciliation before any resume.",
                "args": {"run_dir": run_dir},
                "reads": [run_dir, db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": run_dir,
                    "description": "Read catch-up artifacts and reconciliation.",
                }
            ],
            "requires_approval": False,
            "reason": "Resume is ready once reconciliation shows DB receipts and batch artifacts aligned.",
        },
    ]
    if reconciled:
        repairs.append(
            {
                "name": "resume-catch-up",
                "kind": "command",
                "command": {
                    "name": "stock-universe catch-up",
                    "description": "Resume after reconciliation adopted DB-completed targets into recovered artifacts.",
                    "args": {"run_dir": run_dir, "commit": True, "resume": True},
                    "reads": [run_dir],
                    "writes": [run_dir],
                },
                "effects": [
                    {
                        "kind": "write",
                        "target": run_dir,
                        "description": "Continue pending catch-up targets.",
                    }
                ],
                "requires_approval": True,
                "reason": "Reconciliation artifacts exist and DB receipts align with artifact coverage.",
            }
        )
    return repairs


def catch_up_reconciliation_repairs(
    plan_payload: dict[str, Any],
    reconciliation: dict[str, Any],
    *,
    stale_running: bool,
) -> list[dict[str, Any]]:
    db = str(plan_payload.get("db") or CANONICAL_DB_PATH)
    run_dir = str(plan_payload.get("run_dir") or "")
    return [
        {
            "name": "validate-db",
            "kind": "command",
            "command": {
                "name": "stock-universe validate-db",
                "description": "Validate DB integrity before repairing or resuming a partially artifacted catch-up run.",
                "args": _db_args(db),
                "reads": [db],
                "writes": [db],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read SQLite integrity state.",
                }
            ],
            "requires_approval": True,
            "reason": (
                "DB receipts exist for target executions awaiting batch artifacts. "
                f"Missing artifact receipt count: {reconciliation.get('db_receipts_without_artifact_count')}"
            ),
        },
        {
            "name": "reconcile-catch-up-artifacts",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up-reconcile",
                "description": "Adopt DB-completed catch-up receipts into explicit recovered artifacts before resume.",
                "args": {"run_dir": run_dir, "commit": True},
                "reads": [run_dir, db],
                "writes": [run_dir],
            },
            "effects": [
                {
                    "kind": "write",
                    "target": run_dir,
                    "description": "Write recovered catch-up artifacts.",
                }
            ],
            "requires_approval": True,
            "reason": (
                "The current resume implementation uses batch artifacts as its durable boundary. "
                "This run needs a reconciliation repair before resume so artifact coverage matches DB receipts."
                + (" The run also appears stale." if stale_running else "")
            ),
        },
    ]


def catch_up_plan_next_actions(plan: CatchUpPlan) -> list[dict[str, Any]]:
    db_args = _db_args(plan.db)
    grain_args = _bar_grain_args(plan.target_policy)
    commit_argv = _catch_up_plan_commit_argv(plan)
    source_checkout_commit_argv = ["./stock_universe.cli", *commit_argv[1:]]
    status_argv = ["xctx", "catch-up-status", "--run-dir", plan.run_dir]
    source_checkout_status_argv = [
        "./stock_universe.cli",
        "xctx",
        "catch-up-status",
        "--run-dir",
        plan.run_dir,
    ]
    if not plan.targets:
        return [
            {
                "name": "inspect-quality-audit",
                "kind": "command",
                "command": {
                    "name": "xctx quality-audit",
                    "description": "Inspect current quality categories before planning a catch-up run.",
                    "args": {**db_args, **grain_args, "limit": 50},
                    "reads": [plan.db],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": plan.db,
                        "description": "Read active reference-series quality state.",
                    }
                ],
                "requires_approval": False,
                "reason": "The current catch-up policy selected zero executable quality-audit issues.",
            }
        ]
    return [
        {
            "name": "commit-catch-up-run",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up",
                "description": "Execute exactly the materialized catch-up target set.",
                "args": {
                    **db_args,
                    "workers": plan.worker_count,
                    "batch_size": plan.batch_size,
                    "run_dir": plan.run_dir,
                    "commit": True,
                    "fail_fast": True,
                    **grain_args,
                },
                "reads": [plan.db, "Massive API"],
                "writes": [plan.db, plan.run_dir],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": plan.db,
                    "description": "Load selected reference-universe identities.",
                },
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Collect planning evidence and aggregate bars.",
                },
                {
                    "kind": "write",
                    "target": plan.db,
                    "description": "Persist plans, approvals, bars, and receipts.",
                },
                {
                    "kind": "write",
                    "target": plan.run_dir,
                    "description": "Persist catch-up plan, status, and batch artifacts.",
                },
            ],
            "requires_approval": True,
            "reason": "The plan is read-oriented; --commit enables DB and run-artifact writes.",
            "agent_reporting": catch_up_reporting_policy(
                run_dir=plan.run_dir, target_count=len(plan.targets)
            ),
            "argv": source_checkout_commit_argv,
            "logical_argv": commit_argv,
            "source_checkout_argv": source_checkout_commit_argv,
        },
        {
            "name": "inspect-catch-up-status",
            "kind": "command",
            "command": {
                "name": "xctx catch-up-status",
                "description": "Read durable catch-up artifacts for this planned run directory.",
                "args": {"run_dir": plan.run_dir},
                "reads": [plan.run_dir],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": plan.run_dir,
                    "description": "Read catch-up run artifacts.",
                }
            ],
            "requires_approval": False,
            "argv": source_checkout_status_argv,
            "logical_argv": status_argv,
            "source_checkout_argv": source_checkout_status_argv,
        },
    ]


def _catch_up_plan_commit_argv(plan: CatchUpPlan) -> list[str]:
    policy = plan.target_policy
    argv = [
        "stock-universe",
        "catch-up",
        "--db",
        plan.db,
        "--workers",
        str(plan.worker_count),
        "--batch-size",
        str(plan.batch_size),
    ]
    for category in policy.get("category_filter") or ():
        argv.extend(["--category", str(category)])
    for exchange in policy.get("exchange_filter") or ():
        argv.extend(["--exchange", str(exchange)])
    for security_type in policy.get("security_type_filter") or ():
        argv.extend(["--security-type", str(security_type)])
    for series_id in policy.get("ohlcv_series_id_filter") or ():
        argv.extend(["--ohlcv-series-id", str(series_id)])
    for ticker in policy.get("ticker_filter") or ():
        argv.extend(["--ticker", str(ticker)])
    if str(policy.get("bar_grain") or "1d") != "1d":
        argv.extend(["--bar-grain", str(policy["bar_grain"])])
    target_limit = int(policy.get("target_limit") or 0)
    if target_limit:
        argv.extend(["--target-limit", str(target_limit)])
    if policy.get("from_date_override"):
        argv.extend(["--from-date", str(policy["from_date_override"])])
    if policy.get("to_date"):
        argv.extend(["--to-date", str(policy["to_date"])])
    if policy.get("stale_before"):
        argv.extend(["--stale-before", str(policy["stale_before"])])
    argv.extend(["--run-dir", plan.run_dir, "--commit", "--fail-fast"])
    return argv


def _bar_grain_args(policy: dict[str, Any]) -> dict[str, str]:
    grain = str(policy.get("bar_grain") or "1d")
    return {} if grain == "1d" else {"bar_grain": grain}


def catch_up_plan_repair_hints(plan: CatchUpPlan) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    counts = plan.quality_audit_summary.get("category_counts") or {}
    if counts.get("approved_plan_missing_receipt"):
        hints.append(
            {
                "name": "repair-missing-execution-receipts",
                "kind": "command",
                "command": {
                    "name": "stock-universe repair-missing-receipts",
                    "description": "Persist durable error receipts for approved plans missing receipts.",
                    "args": {
                        **_db_args(plan.db),
                        "limit": int(counts["approved_plan_missing_receipt"]),
                        "commit": True,
                    },
                    "reads": [plan.db],
                    "writes": [plan.db],
                },
                "effects": [
                    {
                        "kind": "write",
                        "target": plan.db,
                        "description": "Insert repair receipts.",
                    }
                ],
                "requires_approval": True,
                "reason": "Approved-plan accounting issues use the repair workflow.",
            }
        )
    if counts.get("execution_error"):
        hints.append(
            {
                "name": "observe-error-receipts",
                "kind": "command",
                "command": {
                    "name": "xctx observe",
                    "description": "Inspect recent execution errors before retrying affected series.",
                    "args": {**_db_args(plan.db), "limit": 50},
                    "reads": [plan.db],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": plan.db,
                        "description": "Read execution receipt errors.",
                    }
                ],
                "requires_approval": False,
                "reason": "Series with latest error receipts use review-first handling by default.",
            }
        )
    return hints


def catch_up_post_run_next_actions(plan: CatchUpPlan) -> list[dict[str, Any]]:
    return catch_up_post_run_next_actions_from_payload(plan.to_dict())


def catch_up_post_run_next_actions_from_payload(
    plan_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    db = str(plan_payload.get("db") or CANONICAL_DB_PATH)
    run_dir = str(plan_payload.get("run_dir") or "")
    db_args = _db_args(db)
    return [
        {
            "name": "validate-db",
            "kind": "command",
            "command": {
                "name": "stock-universe validate-db",
                "description": "Validate SQLite schema, foreign keys, bars, receipts, and reference integrity after catch-up.",
                "args": db_args,
                "reads": [db],
                "writes": [db],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read SQLite integrity state.",
                }
            ],
            "requires_approval": True,
            "agent_reporting": validate_db_reporting_policy(),
        },
        {
            "name": "inspect-quality-audit",
            "kind": "command",
            "command": {
                "name": "xctx quality-audit",
                "description": "Re-audit remaining stale, missing, and review-first series after catch-up.",
                "args": {**db_args, "limit": 50},
                "reads": [db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read quality audit state.",
                }
            ],
            "requires_approval": False,
        },
        {
            "name": "observe-executions",
            "kind": "command",
            "command": {
                "name": "xctx observe",
                "description": "Inspect recent execution receipts from the catch-up window.",
                "args": {**db_args, "limit": 50},
                "reads": [db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read execution receipts.",
                }
            ],
            "requires_approval": False,
        },
        {
            "name": "inspect-catch-up-status",
            "kind": "command",
            "command": {
                "name": "xctx catch-up-status",
                "description": "Read durable catch-up status artifacts.",
                "args": {"run_dir": run_dir},
                "reads": [run_dir],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": run_dir,
                    "description": "Read catch-up run artifacts.",
                }
            ],
            "requires_approval": False,
        },
    ]


def _execute_batch(
    plan: CatchUpPlan,
    batch: CatchUpBatch,
    execute_target: TargetExecutor,
    activity: _CatchUpActivityTracker,
    stop_state: _CatchUpStopState,
) -> dict[str, Any]:
    started_at = _utc_now()
    target_by_id = {target.ohlcv_series_id: target for target in plan.targets}
    results: list[dict[str, Any]] = []
    stopped_by_operator: dict[str, Any] | None = None
    activity.batch_started(batch)
    try:
        for series_id in batch.ohlcv_series_ids:
            operator_stop = _batch_operator_stop(plan, stop_state)
            if operator_stop is not None and _stop_mode(operator_stop) in {
                "quiesce",
                "abort",
            }:
                stopped_by_operator = operator_stop
                break
            target = target_by_id[series_id]
            activity.target_started(batch, target)
            try:
                result = execute_target(target)
            except Exception as exc:
                activity.target_finished(
                    batch,
                    target,
                    {
                        "status": "error",
                        "error_type": exc.__class__.__name__,
                        "error": str(exc),
                    },
                )
                raise
            result.setdefault("ohlcv_series_id", target.ohlcv_series_id)
            result["catch_up_target"] = target.to_dict()
            results.append(result)
            activity.target_finished(batch, target, result)
            operator_stop = _batch_operator_stop(plan, stop_state)
            if operator_stop is not None and _stop_mode(operator_stop) in {
                "quiesce",
                "abort",
            }:
                stopped_by_operator = operator_stop
                break
    finally:
        activity.batch_finished(batch)
    finished_at = _utc_now()
    counts = _result_counts(results)
    completed_ids = [int(result.get("ohlcv_series_id") or 0) for result in results]
    original_ids = list(batch.ohlcv_series_ids)
    partial_batch = stopped_by_operator is not None and len(completed_ids) < len(
        original_ids
    )
    status = "ok" if counts["error"] == 0 and counts["skipped"] == 0 else "failed"
    if partial_batch:
        status = f"operator_{_stop_mode(stopped_by_operator)}_partial"
    return {
        "schema_version": CATCH_UP_BATCH_SCHEMA_VERSION,
        "ok": stopped_by_operator is None
        and counts["error"] == 0
        and counts["skipped"] == 0,
        "plan_hash": plan.plan_hash,
        "batch_index": batch.batch_index,
        "status": status,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "target_count": len(completed_ids) if partial_batch else len(original_ids),
        "original_batch_target_count": len(original_ids),
        "ohlcv_series_ids": completed_ids if partial_batch else original_ids,
        "counts": counts,
        "results": results,
        "partial_batch": partial_batch,
        "operator_stop": stopped_by_operator,
        "stop_mode": _stop_mode(stopped_by_operator)
        if stopped_by_operator is not None
        else "",
    }


def _hard_error_batch_payload(
    plan: CatchUpPlan, batch: CatchUpBatch, exc: Exception
) -> dict[str, Any]:
    return {
        "schema_version": CATCH_UP_BATCH_SCHEMA_VERSION,
        "ok": False,
        "plan_hash": plan.plan_hash,
        "batch_index": batch.batch_index,
        "status": "hard_error",
        "hard_error": _hard_error_payload(plan, batch, exc),
        "started_at_utc": "",
        "finished_at_utc": _utc_now(),
        "target_count": len(batch.ohlcv_series_ids),
        "ohlcv_series_ids": list(batch.ohlcv_series_ids),
        "counts": {"ok": 0, "skipped": 0, "error": len(batch.ohlcv_series_ids)},
        "results": [
            {
                "status": "error",
                "ohlcv_series_id": series_id,
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            }
            for series_id in batch.ohlcv_series_ids
        ],
    }


def _hard_error_payload(
    plan: CatchUpPlan, batch: CatchUpBatch, exc: Exception
) -> dict[str, Any]:
    return {
        "error_type": str(getattr(exc, "error_type", exc.__class__.__name__)),
        "error": str(getattr(exc, "error", str(exc))),
        "batch_index": batch.batch_index,
        "ohlcv_series_ids": list(batch.ohlcv_series_ids),
        "plan_hash": plan.plan_hash,
        "run_dir": plan.run_dir,
        "db": plan.db,
        "reported_at_utc": _utc_now(),
    }


def _run_status_payload(
    plan: CatchUpPlan,
    *,
    started_at: str,
    finished_at: str,
    state: str,
    batch_payloads: list[dict[str, Any]],
    pending_batch_count: int,
    strict: bool,
    fail_fast: bool,
    resume: bool,
    hard_error: dict[str, Any] | None,
    resource_stop: dict[str, Any] | None,
    operator_stop: dict[str, Any] | None,
    last_resource_check: dict[str, Any] | None,
) -> dict[str, Any]:
    counts = _aggregate_batch_counts(batch_payloads, target_count=len(plan.targets))
    return {
        "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
        "ok": hard_error is None
        and resource_stop is None
        and operator_stop is None
        and counts["pending"] == 0
        and (not strict or (counts["error"] == 0 and counts["skipped"] == 0)),
        "result_type": "CatchUpRunStatus",
        "state": state,
        "runner": {"pid": os.getpid()},
        "db": plan.db,
        "run_dir": plan.run_dir,
        "plan_hash": plan.plan_hash,
        "target_count": len(plan.targets),
        "batch_count": len(plan.batches),
        "completed_batch_count": len(batch_payloads),
        "pending_batch_count": pending_batch_count,
        "counts": counts,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "strict": strict,
        "fail_fast": fail_fast,
        "resume": resume,
        "hard_error": hard_error,
        "resource_stop": resource_stop,
        "operator_stop": operator_stop,
        "last_resource_check": last_resource_check or {},
        "failed_results": _failed_results(batch_payloads, limit=50),
    }


def _target_from_audit_row(
    row: dict[str, Any],
    *,
    seed_from_date: str | None,
    from_date_override: str | None,
) -> CatchUpTarget:
    category = str(row["category"])
    from_date = from_date_override or _target_from_date(
        row, seed_from_date=seed_from_date
    )
    grain = normalize_bar_grain(row.get("bar_grain") or "1d")
    return CatchUpTarget(
        ohlcv_series_id=int(row["ohlcv_series_id"]),
        ticker=str(row["ticker"] or ""),
        bar_grain=grain.bar_grain,
        multiplier=int(row.get("multiplier") or grain.multiplier),
        timespan=str(row.get("timespan") or grain.timespan),
        category=category,
        from_date=from_date,
        max_bar_date=str(row["max_bar_date"] or ""),
        min_bar_date=str(row["min_bar_date"] or ""),
        bar_count=int(row["bar_count"] or 0),
        plan_count=int(row["plan_count"] or 0),
        receipt_count=int(row["receipt_count"] or 0),
        snapshot_as_of_date=str(row["snapshot_as_of_date"] or ""),
        company_name=str(row["company_name"] or ""),
        security_type=str(row["security_type"] or ""),
        primary_exchange=str(row["primary_exchange"] or ""),
        market=str(row["market"] or ""),
        cik=str(row["cik"] or ""),
        composite_figi=str(row["composite_figi"] or ""),
        share_class_figi=str(row["share_class_figi"] or ""),
        suggested_next_command=str(row["suggested_next_command"] or ""),
    )


def _target_from_payload(payload: dict[str, Any]) -> CatchUpTarget:
    grain = normalize_bar_grain(payload.get("bar_grain") or "1d")
    return CatchUpTarget(
        ohlcv_series_id=int(payload["ohlcv_series_id"]),
        ticker=str(payload.get("ticker") or ""),
        bar_grain=grain.bar_grain,
        multiplier=int(payload.get("multiplier") or grain.multiplier),
        timespan=str(payload.get("timespan") or grain.timespan),
        category=str(payload.get("category") or ""),
        from_date=str(payload.get("from_date") or ""),
        max_bar_date=str(payload.get("max_bar_date") or ""),
        min_bar_date=str(payload.get("min_bar_date") or ""),
        bar_count=int(payload.get("bar_count") or 0),
        plan_count=int(payload.get("plan_count") or 0),
        receipt_count=int(payload.get("receipt_count") or 0),
        snapshot_as_of_date=str(payload.get("snapshot_as_of_date") or ""),
        company_name=str(payload.get("company_name") or ""),
        security_type=str(payload.get("security_type") or ""),
        primary_exchange=str(payload.get("primary_exchange") or ""),
        market=str(payload.get("market") or ""),
        cik=str(payload.get("cik") or ""),
        composite_figi=str(payload.get("composite_figi") or ""),
        share_class_figi=str(payload.get("share_class_figi") or ""),
        suggested_next_command=str(payload.get("suggested_next_command") or ""),
    )


def _target_progress_payload(target: CatchUpTarget) -> dict[str, Any]:
    return {
        "ohlcv_series_id": target.ohlcv_series_id,
        "ticker": target.ticker,
        "bar_grain": target.bar_grain,
        "category": target.category,
        "from_date": target.from_date,
        "max_bar_date": target.max_bar_date,
        "bar_count": target.bar_count,
        "security_type": target.security_type,
        "primary_exchange": target.primary_exchange,
    }


def _batch_from_payload(payload: dict[str, Any]) -> CatchUpBatch:
    return CatchUpBatch(
        batch_index=int(payload["batch_index"]),
        ohlcv_series_ids=tuple(
            int(series_id) for series_id in payload.get("ohlcv_series_ids") or []
        ),
    )


def _target_from_date(row: dict[str, Any], *, seed_from_date: str | None) -> str:
    if str(row["category"]) == "plan_session_gap" and row.get(
        "first_missing_session_date"
    ):
        return str(row["first_missing_session_date"])
    if str(row["category"]) in INCREMENTAL_CATCH_UP_CATEGORIES and row.get(
        "max_bar_date"
    ):
        return next_us_equity_trading_date(str(row["max_bar_date"]))
    if seed_from_date:
        return seed_from_date
    return default_us_equity_history_start_date(str(row["snapshot_as_of_date"] or ""))


def _batches_for_targets(
    targets: tuple[CatchUpTarget, ...], *, batch_size: int
) -> tuple[CatchUpBatch, ...]:
    return tuple(
        CatchUpBatch(
            batch_index=index,
            ohlcv_series_ids=tuple(
                target.ohlcv_series_id for target in targets[start : start + batch_size]
            ),
        )
        for index, start in enumerate(range(0, len(targets), batch_size))
    )


def _quality_audit_summary(audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "db": audit["db"],
        "bar_grain": audit["bar_grain"],
        "multiplier": audit["multiplier"],
        "timespan": audit["timespan"],
        "latest_reference_snapshot_as_of_date": audit[
            "latest_reference_snapshot_as_of_date"
        ],
        "global_min_bar_date": audit["global_min_bar_date"],
        "global_max_bar_date": audit["global_max_bar_date"],
        "stale_before": audit["stale_before"],
        "active_reference_series": audit["active_reference_series"],
        "matched_series_count": audit["matched_series_count"],
        "issue_count": audit["issue_count"],
        "category_counts": audit["category_counts"],
        "unfiltered_issue_count": audit["unfiltered_issue_count"],
        "unfiltered_category_counts": audit["unfiltered_category_counts"],
        "filters": audit["filters"],
    }


def _result_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "ok": sum(1 for result in results if result.get("status") == "ok"),
        "skipped": sum(1 for result in results if result.get("status") == "skipped"),
        "error": sum(1 for result in results if result.get("status") == "error"),
    }


def _aggregate_batch_counts(
    batch_payloads: list[dict[str, Any]], *, target_count: int
) -> dict[str, int]:
    ok = sum(
        int((batch.get("counts") or {}).get("ok") or 0) for batch in batch_payloads
    )
    skipped = sum(
        int((batch.get("counts") or {}).get("skipped") or 0) for batch in batch_payloads
    )
    error = sum(
        int((batch.get("counts") or {}).get("error") or 0) for batch in batch_payloads
    )
    completed = ok + skipped + error
    return {
        "ok": ok,
        "skipped": skipped,
        "error": error,
        "completed": completed,
        "pending": max(target_count - completed, 0),
    }


def _failed_results(
    batch_payloads: list[dict[str, Any]], *, limit: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for batch in sorted(
        batch_payloads, key=lambda item: int(item.get("batch_index") or 0)
    ):
        for result in batch.get("results") or []:
            if result.get("status") != "ok":
                rows.append(result)
                if len(rows) >= limit:
                    return rows
    return rows


def _unartifacted_receipt_rows(
    plan_payload: dict[str, Any],
    batch_payloads: list[dict[str, Any]],
    started_at_utc: str,
) -> list[dict[str, Any]]:
    db = str(plan_payload.get("db") or "")
    if not db or not started_at_utc or not Path(db).exists():
        return []
    target_ids = {
        int(series_id) for series_id in plan_payload.get("ohlcv_series_ids") or []
    }
    artifact_completed_series = _artifact_completed_series(batch_payloads)
    try:
        with connect_readonly_sqlite(Path(db)) as conn:
            if not _sqlite_table_exists(conn, "execution_receipts"):
                return []
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT execution_receipt_id, request_hash, evidence_ledger_hash, ohlcv_series_id,
                           status, approved_by, started_at_utc, finished_at_utc, planned_segment_count,
                           fetched_bar_count, inserted_bar_count, request_log_json, receipt_json,
                           receipt_hash
                    FROM execution_receipts
                    WHERE started_at_utc >= ?
                    ORDER BY started_at_utc, execution_receipt_id
                    """,
                    (started_at_utc,),
                ).fetchall()
            ]
    except sqlite3.Error:
        return []
    return [
        row
        for row in rows
        if int(row["ohlcv_series_id"]) in target_ids
        and int(row["ohlcv_series_id"]) not in artifact_completed_series
    ]


def _recovered_batch_payloads(
    run_dir: Path,
    plan_payload: dict[str, Any],
    recovered_receipts: list[dict[str, Any]],
    *,
    reconciled_at_utc: str,
) -> list[dict[str, Any]]:
    target_by_id = {
        int(target["ohlcv_series_id"]): dict(target)
        for target in plan_payload.get("targets") or []
    }
    series_to_batch: dict[int, int] = {}
    batch_targets: dict[int, list[int]] = {}
    for batch in plan_payload.get("batches") or []:
        batch_index = int(batch.get("batch_index") or 0)
        ids = [int(series_id) for series_id in batch.get("ohlcv_series_ids") or []]
        batch_targets[batch_index] = ids
        for series_id in ids:
            series_to_batch[series_id] = batch_index

    receipts_by_batch: dict[int, list[dict[str, Any]]] = {}
    for receipt in recovered_receipts:
        series_id = int(receipt["ohlcv_series_id"])
        batch_index = series_to_batch.get(series_id)
        if batch_index is None:
            continue
        receipts_by_batch.setdefault(batch_index, []).append(receipt)

    payloads = []
    for batch_index, rows in sorted(receipts_by_batch.items()):
        results = [
            _recovered_result_from_receipt(
                row, target=target_by_id.get(int(row["ohlcv_series_id"])) or {}
            )
            for row in sorted(
                rows,
                key=lambda item: batch_targets.get(batch_index, []).index(
                    int(item["ohlcv_series_id"])
                ),
            )
        ]
        counts = _result_counts(results)
        first_started = min(
            (str(row.get("started_at_utc") or "") for row in rows), default=""
        )
        last_finished = max(
            (str(row.get("finished_at_utc") or "") for row in rows), default=""
        )
        payloads.append(
            {
                "schema_version": CATCH_UP_BATCH_SCHEMA_VERSION,
                "ok": counts["error"] == 0 and counts["skipped"] == 0,
                "plan_hash": str(plan_payload.get("plan_hash") or ""),
                "batch_index": batch_index,
                "artifact_kind": "recovered_from_db",
                "status": "recovered_from_db",
                "started_at_utc": first_started,
                "finished_at_utc": last_finished,
                "recovered_at_utc": reconciled_at_utc,
                "target_count": len(results),
                "original_batch_target_count": len(
                    batch_targets.get(batch_index) or []
                ),
                "ohlcv_series_ids": [
                    int(result["ohlcv_series_id"]) for result in results
                ],
                "counts": counts,
                "results": results,
                "recovery": {
                    "source": "execution_receipts",
                    "policy": "adopt_validated_db_receipts_as_recovered_artifacts",
                    "original_batch_artifact_missing": not _batch_path(
                        run_dir, batch_index
                    ).exists(),
                    "partial_batch_recovery": len(results)
                    < len(batch_targets.get(batch_index) or []),
                    "recovered_result_count": len(results),
                    "original_batch_target_count": len(
                        batch_targets.get(batch_index) or []
                    ),
                },
            }
        )
    return payloads


def _recovered_result_from_receipt(
    receipt: dict[str, Any], *, target: dict[str, Any]
) -> dict[str, Any]:
    series_id = int(receipt["ohlcv_series_id"])
    status = str(receipt.get("status") or "")
    return {
        "ohlcv_series_id": series_id,
        "status": status,
        "catch_up_target": target,
        "fetched_bar_count": int(receipt.get("fetched_bar_count") or 0),
        "inserted_bar_count": int(receipt.get("inserted_bar_count") or 0),
        "request_hash": str(receipt.get("request_hash") or ""),
        "evidence_ledger_hash": str(receipt.get("evidence_ledger_hash") or ""),
        "execution_receipt_id": int(receipt.get("execution_receipt_id") or 0),
        "receipt_hash": str(receipt.get("receipt_hash") or ""),
        "started_at_utc": str(receipt.get("started_at_utc") or ""),
        "finished_at_utc": str(receipt.get("finished_at_utc") or ""),
        "recovered_from_db": True,
        "recovery": {
            "source": "execution_receipts",
            "artifact_kind": "recovered_from_db",
            "original_batch_artifact_missing": True,
        },
    }


def _pending_batches_after_completed_series(
    plan: CatchUpPlan,
    *,
    completed_series_ids: set[int],
) -> list[CatchUpBatch]:
    pending_batches = []
    for batch in plan.batches:
        pending_ids = tuple(
            series_id
            for series_id in batch.ohlcv_series_ids
            if series_id not in completed_series_ids
        )
        if pending_ids:
            pending_batches.append(
                CatchUpBatch(
                    batch_index=batch.batch_index, ohlcv_series_ids=pending_ids
                )
            )
    return pending_batches


def _is_active_running_status(status: dict[str, Any]) -> bool:
    return str(status.get("state") or "") in {
        "running",
        "stopping",
        "resource_stopping",
        "operator_stopping",
    } and not status.get("finished_at_utc")


def _catch_up_reconciliation_error(
    run_dir: Path,
    plan_payload: dict[str, Any],
    *,
    code: str,
    detail: str,
    validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db = str(plan_payload.get("db") or "")
    payload = {
        "schema_version": CATCH_UP_RECONCILIATION_SCHEMA_VERSION,
        "ok": False,
        "command": "stock-universe catch-up-reconcile",
        "result_type": "RepairError",
        "run_dir": str(run_dir),
        "db": db,
        "errors": [
            {
                "code": code,
                "what_failed": "catch-up reconciliation stopped before producing a repair result",
                "minimal_fix": "Inspect xctx catch-up-status, validate the DB, then rerun reconciliation after the run is stopped.",
                "detail": detail,
            }
        ],
        "effects": {"will_read": [db, str(run_dir)], "will_write": [], "did_write": []},
    }
    if validation is not None:
        payload["validation"] = validation
    return payload


def _catch_up_reconciliation_next_actions(
    plan_payload: dict[str, Any],
    after: dict[str, Any],
    *,
    committed: bool,
) -> list[dict[str, Any]]:
    run_dir = str(plan_payload.get("run_dir") or "")
    if not committed:
        return [
            {
                "name": "commit-catch-up-reconciliation",
                "kind": "command",
                "command": {
                    "name": "stock-universe catch-up-reconcile",
                    "description": "Write recovered artifacts for DB-completed targets.",
                    "args": {"run_dir": run_dir, "commit": True},
                    "reads": [run_dir, str(plan_payload.get("db") or "")],
                    "writes": [run_dir],
                },
                "effects": [
                    {
                        "kind": "write",
                        "target": run_dir,
                        "description": "Persist recovered catch-up artifacts.",
                    }
                ],
                "requires_approval": True,
                "reason": "Dry-run reconciliation is clean; --commit is required to write recovered artifacts.",
            }
        ]
    if after.get("requires_reconciliation"):
        return [
            {
                "name": "inspect-catch-up-status",
                "kind": "command",
                "command": {
                    "name": "xctx catch-up-status",
                    "description": "Inspect remaining reconciliation gaps.",
                    "args": {"run_dir": run_dir},
                    "reads": [run_dir],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": run_dir,
                        "description": "Read catch-up status.",
                    }
                ],
                "requires_approval": False,
                "reason": "Reconciliation still reports gaps after repair.",
            }
        ]
    return [
        {
            "name": "resume-catch-up",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up",
                "description": "Resume after recovered artifacts cover DB-completed targets.",
                "args": {"run_dir": run_dir, "commit": True, "resume": True},
                "reads": [run_dir],
                "writes": [run_dir],
            },
            "effects": [
                {
                    "kind": "write",
                    "target": run_dir,
                    "description": "Continue pending catch-up targets.",
                }
            ],
            "requires_approval": True,
            "reason": "Recovered artifacts now cover DB receipts that were missing artifacts.",
        }
    ]


def _db_reconciliation(
    plan_payload: dict[str, Any],
    batch_payloads: list[dict[str, Any]],
    started_at_utc: str,
) -> dict[str, Any]:
    db = str(plan_payload.get("db") or "")
    target_ids = {
        int(series_id) for series_id in plan_payload.get("ohlcv_series_ids") or []
    }
    series_to_batch: dict[int, int] = {}
    for batch in plan_payload.get("batches") or []:
        batch_index = int(batch.get("batch_index") or 0)
        for series_id in batch.get("ohlcv_series_ids") or []:
            series_to_batch[int(series_id)] = batch_index
    artifact_completed_series = _artifact_completed_series(batch_payloads)
    artifact_ok_series = _artifact_ok_series(batch_payloads)
    base = {
        "checked": False,
        "requires_reconciliation": False,
        "db": db,
        "started_at_utc": started_at_utc,
        "artifact_completed_series_count": len(artifact_completed_series),
        "artifact_ok_series_count": len(artifact_ok_series),
        "db_receipts_since_run_start": 0,
        "db_receipt_series_since_run_start": 0,
        "db_approvals_since_run_start": 0,
        "db_approval_series_since_run_start": 0,
        "db_receipts_without_artifact_count": 0,
        "db_approvals_without_artifact_count": 0,
        "unartifacted_receipt_batches": [],
        "unartifacted_approval_batches": [],
        "unartifacted_receipt_series_sample": [],
        "unartifacted_approval_series_sample": [],
    }
    if not db or not started_at_utc or not Path(db).exists():
        return base
    try:
        with connect_readonly_sqlite(Path(db)) as conn:
            if not _sqlite_table_exists(
                conn, "execution_receipts"
            ) or not _sqlite_table_exists(conn, "execution_approvals"):
                return base
            receipt_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT execution_receipt_id, ohlcv_series_id, status, started_at_utc, finished_at_utc,
                           request_hash, fetched_bar_count, inserted_bar_count
                    FROM execution_receipts
                    WHERE started_at_utc >= ?
                    ORDER BY started_at_utc, execution_receipt_id
                    """,
                    (started_at_utc,),
                ).fetchall()
            ]
            approval_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT execution_approval_id, ohlcv_series_id, approved_at_utc, request_hash
                    FROM execution_approvals
                    WHERE approved_at_utc >= ?
                    ORDER BY approved_at_utc, execution_approval_id
                    """,
                    (started_at_utc,),
                ).fetchall()
            ]
    except sqlite3.Error as exc:
        return base | {"checked": False, "error": str(exc)}

    receipt_series = {
        int(row["ohlcv_series_id"])
        for row in receipt_rows
        if int(row["ohlcv_series_id"]) in target_ids
    }
    approval_series = {
        int(row["ohlcv_series_id"])
        for row in approval_rows
        if int(row["ohlcv_series_id"]) in target_ids
    }
    receipt_series_without_artifact = sorted(receipt_series - artifact_completed_series)
    approval_series_without_artifact = sorted(
        approval_series - artifact_completed_series
    )
    return base | {
        "checked": True,
        "requires_reconciliation": bool(
            receipt_series_without_artifact or approval_series_without_artifact
        ),
        "db_receipts_since_run_start": len(
            [row for row in receipt_rows if int(row["ohlcv_series_id"]) in target_ids]
        ),
        "db_receipt_series_since_run_start": len(receipt_series),
        "db_approvals_since_run_start": len(
            [row for row in approval_rows if int(row["ohlcv_series_id"]) in target_ids]
        ),
        "db_approval_series_since_run_start": len(approval_series),
        "db_receipts_without_artifact_count": len(receipt_series_without_artifact),
        "db_approvals_without_artifact_count": len(approval_series_without_artifact),
        "unartifacted_receipt_batches": sorted(
            {
                batch_index
                for series_id in receipt_series_without_artifact
                if (batch_index := series_to_batch.get(series_id)) is not None
            }
        ),
        "unartifacted_approval_batches": sorted(
            {
                batch_index
                for series_id in approval_series_without_artifact
                if (batch_index := series_to_batch.get(series_id)) is not None
            }
        ),
        "unartifacted_receipt_series_sample": receipt_series_without_artifact[:50],
        "unartifacted_approval_series_sample": approval_series_without_artifact[:50],
    }


def _artifact_completed_series(batch_payloads: list[dict[str, Any]]) -> set[int]:
    completed: set[int] = set()
    for batch in batch_payloads:
        for result in batch.get("results") or []:
            series_id = _result_series_id(result)
            if series_id is not None:
                completed.add(series_id)
    return completed


def _artifact_ok_series(batch_payloads: list[dict[str, Any]]) -> set[int]:
    completed: set[int] = set()
    for batch in batch_payloads:
        for result in batch.get("results") or []:
            if result.get("status") != "ok":
                continue
            series_id = _result_series_id(result)
            if series_id is not None:
                completed.add(series_id)
    return completed


def _result_series_id(result: dict[str, Any]) -> int | None:
    value = result.get("ohlcv_series_id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stale_running_status(
    persisted_status: dict[str, Any], progress_events: list[dict[str, Any]]
) -> bool:
    state = str(persisted_status.get("state") or "")
    if state not in {"running", "stopping", "resource_stopping", "operator_stopping"}:
        return False
    if persisted_status.get("finished_at_utc"):
        return False
    pid = int((persisted_status.get("runner") or {}).get("pid") or 0)
    if pid:
        return not _pid_is_alive(pid)
    last_event = progress_events[-1] if progress_events else {}
    last_emitted_at = _parse_utc(
        str(
            last_event.get("emitted_at_utc")
            or persisted_status.get("started_at_utc")
            or ""
        )
    )
    if last_emitted_at is None:
        return True
    return (
        dt.datetime.now(dt.UTC) - last_emitted_at
    ).total_seconds() > STALE_RUNNING_SECONDS


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _parse_utc(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _batch_has_failure(batch_payload: dict[str, Any]) -> bool:
    counts = batch_payload.get("counts") or {}
    return int(counts.get("error") or 0) > 0 or int(counts.get("skipped") or 0) > 0


def _active_state(
    *,
    resource_stop: dict[str, Any] | None,
    operator_stop: dict[str, Any] | None,
    stopped_for_failure: bool,
) -> str:
    if resource_stop is not None:
        return "resource_stopping"
    if operator_stop is not None:
        return "operator_stopping"
    if stopped_for_failure:
        return "stopping"
    return "running"


def _final_state(
    *,
    hard_error: dict[str, Any] | None,
    resource_stop: dict[str, Any] | None,
    operator_stop: dict[str, Any] | None,
) -> str:
    if hard_error is not None:
        return "hard_error"
    if resource_stop is not None:
        return "resource_stopped"
    if operator_stop is not None:
        return "operator_stopped"
    return "finished"


def _validate_stop_mode(mode: str) -> str:
    stop_mode = str(mode or DEFAULT_CATCH_UP_STOP_MODE).strip().lower()
    if stop_mode not in CATCH_UP_STOP_MODES:
        raise ValueError(
            f"catch-up stop mode must be one of {', '.join(sorted(CATCH_UP_STOP_MODES))}"
        )
    return stop_mode


def _stop_mode(stop_request: dict[str, Any] | None) -> str:
    if stop_request is None:
        return DEFAULT_CATCH_UP_STOP_MODE
    stop_mode = (
        str(stop_request.get("mode") or DEFAULT_CATCH_UP_STOP_MODE).strip().lower()
    )
    return stop_mode if stop_mode in CATCH_UP_STOP_MODES else DEFAULT_CATCH_UP_STOP_MODE


def _normalized_stop_request(stop_request: dict[str, Any]) -> dict[str, Any]:
    payload = dict(stop_request)
    payload["mode"] = _stop_mode(payload)
    return payload


def _operator_stop_progress_message(operator_stop: dict[str, Any] | None) -> str:
    mode = _stop_mode(operator_stop)
    if mode == "quiesce":
        return "operator requested catch-up quiesce; active batches stop between targets before scheduling pauses"
    if mode == "abort":
        return "operator requested catch-up abort; active batches stop before starting another target"
    return "operator requested catch-up drain; active batches drain before scheduling pauses"


def _batch_operator_stop(
    plan: CatchUpPlan, stop_state: _CatchUpStopState
) -> dict[str, Any] | None:
    operator_stop = stop_state.get()
    if operator_stop is not None:
        return operator_stop
    operator_stop = _read_stop_request(Path(plan.run_dir))
    if operator_stop is not None:
        stop_state.set(operator_stop)
    return stop_state.get()


def _operator_stop_request(
    run_dir: Path,
    *,
    plan: CatchUpPlan,
    stop_probe: StopProbe | None,
) -> dict[str, Any] | None:
    stop_request = _read_stop_request(run_dir)
    if stop_request is not None:
        return _normalized_stop_request(stop_request)
    if stop_probe is None:
        return None
    stop_request = stop_probe(plan)
    if stop_request is None:
        return None
    existing = _read_stop_request(run_dir)
    if existing is not None:
        return existing
    payload = {
        "schema_version": CATCH_UP_STOP_SCHEMA_VERSION,
        "run_dir": plan.run_dir,
        "reason": str(stop_request.get("reason") or "operator requested stop"),
        "requested_by": str(stop_request.get("requested_by") or "operator"),
        "requested_at_utc": str(stop_request.get("requested_at_utc") or _utc_now()),
        "mode": _validate_stop_mode(
            str(stop_request.get("mode") or DEFAULT_CATCH_UP_STOP_MODE)
        ),
    }
    _write_json(run_dir / STOP_REQUEST_FILENAME, payload)
    return payload


def _read_stop_request(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / STOP_REQUEST_FILENAME
    if not path.exists():
        return None
    try:
        payload = _read_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {
            "schema_version": CATCH_UP_STOP_SCHEMA_VERSION,
            "run_dir": str(run_dir),
            "reason": "stop request artifact needs readable JSON",
            "requested_by": "unknown",
            "requested_at_utc": "",
            "mode": DEFAULT_CATCH_UP_STOP_MODE,
            "unreadable": True,
        }
    return _normalized_stop_request(payload)


def _archive_prior_stop_request(run_dir: Path, *, resumed_at_utc: str) -> None:
    stop_path = run_dir / STOP_REQUEST_FILENAME
    if not stop_path.exists():
        return
    archive_name = f"stop_request.consumed.{_artifact_timestamp(resumed_at_utc)}.json"
    stop_path.replace(run_dir / archive_name)


def _read_reconciliation_artifact(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / RECONCILIATION_FILENAME
    if not path.exists():
        return None
    try:
        return _read_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {
            "schema_version": CATCH_UP_RECONCILIATION_SCHEMA_VERSION,
            "run_dir": str(run_dir),
            "ok": False,
            "unreadable": True,
        }


def _previous_started_at(run_dir: Path) -> str:
    status_path = run_dir / "status.json"
    if not status_path.exists():
        return ""
    try:
        status = _read_json(status_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return ""
    return str(status.get("started_at_utc") or "")


def _resource_snapshot(plan: CatchUpPlan) -> dict[str, Any]:
    disk_checks = _disk_checks(plan)
    min_free = min((int(item["free_bytes"]) for item in disk_checks), default=0)
    if min_free < DISK_DRAIN_BYTES:
        disk_status = "draining"
    elif min_free < DISK_CRITICAL_BYTES:
        disk_status = "critical"
    elif min_free < DISK_WARNING_BYTES:
        disk_status = "warning"
    else:
        disk_status = "ok"
    return {
        "checked_at_utc": _utc_now(),
        "disk": {
            "status": disk_status,
            "min_free_bytes": min_free,
            "min_free_gb": round(min_free / (1024**3), 3),
            "warning_threshold_bytes": DISK_WARNING_BYTES,
            "critical_threshold_bytes": DISK_CRITICAL_BYTES,
            "drain_threshold_bytes": DISK_DRAIN_BYTES,
            "checks": disk_checks,
        },
        "memory": _memory_snapshot(),
    }


def _disk_checks(plan: CatchUpPlan) -> list[dict[str, Any]]:
    paths = []
    for path in (Path(plan.db).parent, Path(plan.run_dir)):
        target = path if path.exists() else path.parent
        if target and target not in paths:
            paths.append(target)
    checks = []
    for path in paths:
        usage = shutil.disk_usage(path)
        checks.append(
            {
                "path": str(path),
                "total_bytes": usage.total,
                "used_bytes": usage.used,
                "free_bytes": usage.free,
                "free_gb": round(usage.free / (1024**3), 3),
            }
        )
    return checks


def _memory_snapshot() -> dict[str, Any]:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return {"status": "unavailable", "reason": "/proc/meminfo is unavailable"}
    values: dict[str, int] = {}
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            values[parts[0].rstrip(":")] = int(parts[1]) * 1024
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", 0)
    return {
        "status": "observed",
        "total_bytes": total,
        "available_bytes": available,
        "available_gb": round(available / (1024**3), 3) if available else 0,
        "policy": "observed_only",
    }


def _should_drain_for_disk(resource_check: dict[str, Any]) -> bool:
    disk = resource_check.get("disk") or {}
    return (
        str(disk.get("status") or "") == "draining"
        or int(disk.get("min_free_bytes") or 0) < DISK_DRAIN_BYTES
    )


def _resource_stop_payload(resource_check: dict[str, Any]) -> dict[str, Any]:
    disk = resource_check.get("disk") or {}
    checks = list(disk.get("checks") or [])
    return {
        "reason": "disk_free_below_drain_threshold",
        "stopped_at_utc": _utc_now(),
        "min_free_bytes": int(disk.get("min_free_bytes") or 0),
        "min_free_gb": disk.get("min_free_gb", 0),
        "drain_threshold_bytes": DISK_DRAIN_BYTES,
        "drain_threshold_gb": round(DISK_DRAIN_BYTES / (1024**3), 3),
        "checked_paths": [str(item.get("path") or "") for item in checks],
        "resource_check": resource_check,
    }


def _emit_resource_events(
    progress_path: Path,
    *,
    plan: CatchUpPlan,
    status: dict[str, Any],
    resource_check: dict[str, Any],
    event_state: dict[str, bool],
    started_at_monotonic: float,
    progress_sink: ProgressSink | None,
) -> None:
    _emit_progress_event(
        progress_path,
        _progress_event(
            "resource_check",
            "resource check",
            plan=plan,
            status=status,
            started_at_monotonic=started_at_monotonic,
            extra={"resource_check": resource_check},
        ),
        progress_sink=progress_sink,
    )
    disk = resource_check.get("disk") or {}
    min_free = int(disk.get("min_free_bytes") or 0)
    if min_free < DISK_WARNING_BYTES and not event_state["warning"]:
        event_state["warning"] = True
        _emit_progress_event(
            progress_path,
            _progress_event(
                "disk_warning",
                f"disk free below {_format_bytes(DISK_WARNING_BYTES)}",
                plan=plan,
                status=status,
                started_at_monotonic=started_at_monotonic,
                extra={"resource_check": resource_check},
            ),
            progress_sink=progress_sink,
        )
    if min_free < DISK_CRITICAL_BYTES and not event_state["critical"]:
        event_state["critical"] = True
        _emit_progress_event(
            progress_path,
            _progress_event(
                "disk_critical",
                f"disk free below {_format_bytes(DISK_CRITICAL_BYTES)}",
                plan=plan,
                status=status,
                started_at_monotonic=started_at_monotonic,
                extra={"resource_check": resource_check},
            ),
            progress_sink=progress_sink,
        )
    if min_free < DISK_DRAIN_BYTES and not event_state["draining"]:
        event_state["draining"] = True
        _emit_progress_event(
            progress_path,
            _progress_event(
                "disk_drain",
                f"disk free below {_format_bytes(DISK_DRAIN_BYTES)}; active batches drain before scheduling pauses",
                plan=plan,
                status=status,
                started_at_monotonic=started_at_monotonic,
                extra={"resource_check": resource_check},
            ),
            progress_sink=progress_sink,
        )


def _final_progress_message(
    *,
    hard_error: dict[str, Any] | None,
    resource_stop: dict[str, Any] | None,
    operator_stop: dict[str, Any] | None,
) -> str:
    if hard_error is not None:
        return "catch-up stopped on hard error"
    if resource_stop is not None:
        return "catch-up stopped scheduling new work because disk free space crossed the drain threshold"
    if operator_stop is not None:
        return f"catch-up stopped after operator {_stop_mode(operator_stop)} request"
    return "catch-up finished"


def _final_progress_extra(
    *,
    hard_error: dict[str, Any] | None,
    resource_stop: dict[str, Any] | None,
    operator_stop: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if hard_error is not None:
        return {"hard_error": hard_error}
    if resource_stop is not None:
        return {"resource_stop": resource_stop}
    if operator_stop is not None:
        return {"operator_stop": operator_stop}
    return None


def _format_bytes(value: int) -> str:
    return f"{value / (1024**3):.0f}GB"


def _progress_event(
    event_type: str,
    message: str,
    *,
    plan: CatchUpPlan,
    status: dict[str, Any],
    started_at_monotonic: float,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_monotonic = time.monotonic()
    payload = {
        "schema_version": CATCH_UP_RUN_SCHEMA_VERSION,
        "event_type": event_type,
        "message": message,
        "emitted_at_utc": _utc_now(),
        "elapsed_seconds": round(max(now_monotonic - started_at_monotonic, 0), 3),
        "run_dir": plan.run_dir,
        "plan_hash": plan.plan_hash,
        "target_count": len(plan.targets),
        "batch_count": len(plan.batches),
        "completed_batch_count": status.get("completed_batch_count", 0),
        "pending_batch_count": status.get("pending_batch_count", 0),
        "counts": status.get("counts", {}),
        "state": status.get("state", ""),
    }
    if extra:
        payload.update(extra)
    return payload


def _emit_progress_event(
    progress_path: Path,
    event: dict[str, Any],
    *,
    progress_sink: ProgressSink | None,
) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    if progress_sink is not None:
        progress_sink(event)


def _progress_events(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows[-limit:]


def _catch_up_run_dirs(root: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.is_dir() and (path / "plan.json").exists()
        ),
        key=lambda path: (_catch_up_run_sort_time(path), path.name),
        reverse=True,
    )


def _catch_up_run_sort_time(path: Path) -> float:
    times = []
    for name in ("status.json", "progress.jsonl", "plan.json"):
        artifact = path / name
        if artifact.exists():
            times.append(artifact.stat().st_mtime)
    return max(times, default=path.stat().st_mtime)


def _existing_batch_payloads(run_dir: Path, *, plan_hash: str) -> list[dict[str, Any]]:
    payloads = []
    artifact_paths = [
        *run_dir.glob("batch_*.json"),
        *run_dir.glob("recovered_batch_*.json"),
    ]
    for path in sorted(artifact_paths):
        payload = _read_json(path)
        if str(payload.get("plan_hash") or "") == plan_hash:
            payload["_artifact_path"] = str(path)
            payloads.append(payload)
    return sorted(payloads, key=lambda item: int(item.get("batch_index") or 0))


def _batch_path(run_dir: Path, batch_index: int) -> Path:
    return run_dir / f"batch_{batch_index:04d}.json"


def _recovered_batch_path(run_dir: Path, batch_index: int) -> Path:
    return run_dir / f"recovered_batch_{batch_index:04d}.json"


def _artifact_timestamp(value: str) -> str:
    safe = value.replace("+00:00", "Z")
    return "".join(char if char.isalnum() else "" for char in safe) or str(
        int(time.time())
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    tmp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _readonly_db_uri(path: Path) -> str:
    return readonly_db_uri(path)


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _default_run_dir(plan_hash: str, *, run_root: str | Path | None) -> str:
    root = Path(run_root) if run_root else DEFAULT_CATCH_UP_RUN_ROOT
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return str(root / f"{timestamp}_{plan_hash[:12]}")


def _validate_workers(workers: int) -> None:
    if workers < 1:
        raise ValueError("workers must be positive")
    if workers > MAX_CATCH_UP_WORKERS:
        raise ValueError(f"workers must be at most {MAX_CATCH_UP_WORKERS}")


def _validate_batch_size(batch_size: int) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if batch_size > 1000:
        raise ValueError("batch_size must be at most 1000")


def _db_args(db: str) -> dict[str, str]:
    try:
        if Path(db).resolve() == CANONICAL_DB_PATH.resolve():
            return {}
    except OSError:
        pass
    return {"db": db}


def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
