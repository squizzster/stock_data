from __future__ import annotations

import concurrent.futures
import json
import time
from pathlib import Path

import pytest

from stock_universe import cli as stock_cli_module
from stock_universe.market_calendar import MARKET_CALENDAR_ENV
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    StoredOhlcvBar,
    StoredReferenceSnapshot,
)
from stock_universe.workflows import (
    build_catch_up_plan,
    catch_up_plan_from_run_dir,
    catch_up_run_status,
    execute_catch_up_plan,
    reconcile_catch_up_run,
    request_catch_up_stop,
)
from stock_universe.xctx.cli import main as xctx_main


def test_catch_up_plan_materializes_exact_targets_and_incremental_from_dates(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)

    plan = build_catch_up_plan(
        db, workers=2, batch_size=1, stale_before="2021-05-09", run_dir=tmp_path / "run"
    )

    assert plan.worker_count == 2
    assert plan.batch_size == 1
    assert [target.ticker for target in plan.targets] == ["STALE", "NEWCO"]
    assert {target.ticker: target.from_date for target in plan.targets} == {
        "STALE": "2021-05-06",
        "NEWCO": "2021-05-10",
    }
    assert plan.target_policy["to_date"] == "2021-05-10"
    assert [batch.ohlcv_series_ids for batch in plan.batches] == [
        (plan.targets[0].ohlcv_series_id,),
        (plan.targets[1].ohlcv_series_id,),
    ]
    assert (
        plan.plan_hash
        == build_catch_up_plan(
            db,
            workers=2,
            batch_size=1,
            stale_before="2021-05-09",
            run_dir=tmp_path / "other-run",
        ).plan_hash
    )


def test_catch_up_plan_propagates_intraday_bar_grain(tmp_path: Path) -> None:
    db = _seed_catch_up_db(tmp_path)

    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        bar_grain="1m",
        target_limit=1,
        run_dir=tmp_path / "minute-run",
    )
    payload = plan.to_dict()
    commit_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "commit-catch-up-run"
    )

    assert plan.target_policy["bar_grain"] == "1m"
    assert plan.target_policy["multiplier"] == 1
    assert plan.target_policy["timespan"] == "minute"
    assert payload["quality_audit_summary"]["bar_grain"] == "1m"
    assert payload["targets"][0]["bar_grain"] == "1m"
    assert "--bar-grain" in commit_action["argv"]
    assert "1m" in commit_action["argv"]


def test_catch_up_plan_incremental_stale_targets_resume_on_weekday(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.ensure_schema()
    repository.upsert_reference_snapshots(
        [
            _snapshot("STALE", "Stale Test Company", "figi-stale", "share-stale"),
            _snapshot("FRESH", "Fresh Test Company", "figi-fresh", "share-fresh"),
        ]
    )
    stale_id = repository.lookup_ohlcv_series_id("test:STALE")
    fresh_id = repository.lookup_ohlcv_series_id("test:FRESH")
    assert stale_id is not None
    assert fresh_id is not None
    repository.insert_bars(
        [
            _bar(stale_id, "STALE", "2021-05-07", 1620345600000),
            _bar(fresh_id, "FRESH", "2021-05-10", 1620604800000),
        ]
    )

    plan = build_catch_up_plan(
        db, stale_before="2021-05-09", run_dir=tmp_path / "weekday-run"
    )

    assert [target.ticker for target in plan.targets] == ["STALE"]
    assert plan.targets[0].from_date == "2021-05-10"


def test_catch_up_plan_incremental_stale_targets_resume_on_market_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tmp_path / "sessions.json"
    calendar.write_text(
        json.dumps(
            [
                {"date": "2021-05-07", "open": "09:30", "close": "16:00"},
                {"date": "2021-05-11", "open": "09:30", "close": "16:00"},
                {"date": "2021-05-12", "open": "09:30", "close": "16:00"},
            ]
        )
    )
    monkeypatch.setenv(MARKET_CALENDAR_ENV, str(calendar))

    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.ensure_schema()
    repository.upsert_reference_snapshots(
        [
            _snapshot("STALE", "Stale Test Company", "figi-stale", "share-stale"),
            _snapshot("FRESH", "Fresh Test Company", "figi-fresh", "share-fresh"),
        ]
    )
    stale_id = repository.lookup_ohlcv_series_id("test:STALE")
    fresh_id = repository.lookup_ohlcv_series_id("test:FRESH")
    assert stale_id is not None
    assert fresh_id is not None
    repository.insert_bars(
        [
            _bar(stale_id, "STALE", "2021-05-07", 1620345600000),
            _bar(fresh_id, "FRESH", "2021-05-12", 1620777600000),
        ]
    )

    plan = build_catch_up_plan(
        db, stale_before="2021-05-12", run_dir=tmp_path / "market-session-run"
    )

    assert [target.ticker for target in plan.targets] == ["STALE"]
    assert plan.targets[0].from_date == "2021-05-11"


def test_stock_universe_catch_up_is_dry_run_by_default(tmp_path: Path, capsys) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "dry-run-artifacts"

    assert (
        stock_cli_module.main(
            [
                "catch-up",
                "--db",
                str(db),
                "--workers",
                "2",
                "--batch-size",
                "1",
                "--stale-before",
                "2021-05-09",
                "--run-dir",
                str(run_dir),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "CatchUpPlan"
    assert payload["dry_run"] is True
    assert payload["target_count"] == 2
    assert payload["effects"]["did_write"] == []
    assert run_dir.exists() is False


def test_stock_universe_catch_up_commit_honors_target_limit(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "bounded-commit"
    executed: list[int] = []

    def fake_execute_one_series_id(series_id, args, api_key, repository):
        executed.append(int(series_id))
        return {"status": "ok", "ohlcv_series_id": int(series_id)}

    monkeypatch.setattr(
        stock_cli_module, "_execute_one_series_id", fake_execute_one_series_id
    )

    assert (
        stock_cli_module.main(
            [
                "catch-up",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--target-limit",
                "1",
                "--run-dir",
                str(run_dir),
                "--commit",
                "--api-key",
                "secret",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["target_count"] == 1
    assert payload["counts"]["completed"] == 1
    assert len(executed) == 1
    assert (
        json.loads((run_dir / "plan.json").read_text(encoding="utf-8"))["target_count"]
        == 1
    )


def test_stock_universe_catch_up_commit_loads_existing_run_dir_plan(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "existing-plan"
    run_dir.mkdir()
    plan = build_catch_up_plan(
        db, workers=1, batch_size=1, target_limit=1, run_dir=run_dir
    )
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    executed: list[int] = []

    def fake_execute_one_series_id(series_id, args, api_key, repository):
        executed.append(int(series_id))
        return {"status": "ok", "ohlcv_series_id": int(series_id)}

    monkeypatch.setattr(
        stock_cli_module, "_execute_one_series_id", fake_execute_one_series_id
    )

    assert (
        stock_cli_module.main(
            [
                "catch-up",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--run-dir",
                str(run_dir),
                "--commit",
                "--api-key",
                "secret",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["target_count"] == 1
    assert payload["plan_hash"] == plan.plan_hash
    assert len(executed) == 1


def test_catch_up_execution_writes_progress_events(tmp_path: Path) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        stale_before="2021-05-09",
        target_limit=1,
        run_dir=tmp_path / "progress-run",
    )

    def execute_target(target):
        time.sleep(1.2)
        return {"status": "ok", "ohlcv_series_id": target.ohlcv_series_id}

    result = execute_catch_up_plan(
        plan,
        execute_target=execute_target,
        heartbeat_seconds=1,
        mini_summary_seconds=1,
        summary_seconds=1,
    )
    status = catch_up_run_status(plan.run_dir)

    assert result["ok"] is True
    assert (Path(plan.run_dir) / "progress.jsonl").exists()
    assert {"started", "heartbeat", "mini_summary", "summary", "finished"} <= {
        event["event_type"] for event in status["progress_events"]
    }


def test_catch_up_heartbeat_reports_active_target_and_no_recent_completion(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        stale_before="2021-05-09",
        target_limit=1,
        run_dir=tmp_path / "active-target-heartbeat-run",
    )
    progress_events: list[dict] = []

    def execute_target(target):
        time.sleep(1.2)
        return {
            "status": "skipped",
            "reason": "planner returned EvidenceNeeded",
            "ohlcv_series_id": target.ohlcv_series_id,
        }

    execute_catch_up_plan(
        plan,
        execute_target=execute_target,
        heartbeat_seconds=1,
        no_progress_warning_seconds=1,
        progress_sink=progress_events.append,
    )

    heartbeat = next(
        event for event in progress_events if event["event_type"] == "heartbeat"
    )
    activity = heartbeat["activity"]

    assert heartbeat["message"] == "no target or batch completed within threshold"
    assert heartbeat["message"] != "all good"
    assert activity["progress_health"] == "no_recent_completion"
    assert activity["active_batch_count"] == 1
    assert activity["oldest_active_batch_seconds"] >= 1
    assert activity["seconds_since_last_target_completion"] >= 1
    assert activity["active_batches"][0]["current_target"]["ticker"] == "STALE"
    assert activity["active_batches"][0]["current_target_age_seconds"] >= 1


def test_catch_up_hard_error_stops_and_reports_via_status(tmp_path: Path) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        stale_before="2021-05-09",
        run_dir=tmp_path / "hard-error-run",
    )

    def execute_target(target):
        raise RuntimeError(f"hard failure for {target.ohlcv_series_id}")

    result = execute_catch_up_plan(plan, execute_target=execute_target)
    status = catch_up_run_status(plan.run_dir)

    assert result["ok"] is False
    assert result["state"] == "hard_error"
    assert result["hard_error"]["error_type"] == "RuntimeError"
    assert status["ok"] is False
    assert status["state"] == "hard_error"
    assert status["repairs"][0]["command"]["name"] == "xctx catch-up-status"
    assert any(
        event["event_type"] == "hard_error" for event in status["progress_events"]
    )


def test_catch_up_disk_drain_lets_running_batch_finish_then_stops_scheduling(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        stale_before="2021-05-09",
        run_dir=tmp_path / "resource-stop-run",
    )
    checks = iter(
        [
            _resource_check(4 * 1024 * 1024 * 1024),
            _resource_check(2 * 1024 * 1024 * 1024),
        ]
    )

    def execute_target(target):
        time.sleep(1.2)
        return {"status": "ok", "ohlcv_series_id": target.ohlcv_series_id}

    result = execute_catch_up_plan(
        plan,
        execute_target=execute_target,
        resource_check_seconds=1,
        resource_probe=lambda plan: next(
            checks, _resource_check(2 * 1024 * 1024 * 1024)
        ),
    )
    status = catch_up_run_status(plan.run_dir)

    assert result["ok"] is False
    assert result["state"] == "resource_stopped"
    assert result["counts"]["ok"] == 1
    assert result["counts"]["pending"] == 1
    assert status["resource_stop"]["reason"] == "disk_free_below_drain_threshold"
    assert catch_up_plan_from_run_dir(plan.run_dir).plan_hash == plan.plan_hash
    assert {event["event_type"] for event in status["progress_events"]} >= {
        "disk_warning",
        "disk_critical",
        "disk_drain",
    }
    assert status["repairs"][0]["name"] == "free-disk-space"


def test_catch_up_stop_request_drains_inflight_and_writes_summary(
    tmp_path: Path, capsys
) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=1,
        stale_before="2021-05-09",
        run_dir=tmp_path / "operator-stop-run",
    )

    def execute_target(target):
        request_catch_up_stop(plan.run_dir, reason="test stop", requested_by="pytest")
        return {"status": "ok", "ohlcv_series_id": target.ohlcv_series_id}

    result = execute_catch_up_plan(plan, execute_target=execute_target)
    status = catch_up_run_status(plan.run_dir)

    assert result["ok"] is False
    assert result["state"] == "operator_stopped"
    assert result["counts"]["ok"] == 1
    assert result["counts"]["pending"] == 1
    assert result["operator_stop"]["reason"] == "test stop"
    assert (Path(plan.run_dir) / "stop_request.json").exists()
    assert (Path(plan.run_dir) / "summary.json").exists()
    assert catch_up_plan_from_run_dir(plan.run_dir).plan_hash == plan.plan_hash
    assert status["operator_stop"]["requested_by"] == "pytest"
    assert status["repairs"][0]["name"] == "resume-catch-up"
    assert any(
        event["event_type"] == "operator_stop_requested"
        for event in status["progress_events"]
    )

    assert (
        xctx_main(
            [
                "catch-up-runs",
                "--run-root",
                str(tmp_path),
                "--limit",
                "1",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    runs_payload = json.loads(capsys.readouterr().out)
    assert runs_payload["next_actions"][0]["name"] == "resume-latest-catch-up-run"
    assert runs_payload["next_actions"][0]["command"]["args"] == {
        "run_dir": str(plan.run_dir),
        "commit": True,
        "resume": True,
        "fail_fast": True,
    }

    assert (
        xctx_main(
            [
                "catch-up-status",
                "--latest",
                "--run-root",
                str(tmp_path),
                "--view",
                "detail",
            ]
        )
        == 0
    )
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["next_actions"][0]["name"] == "resume-catch-up"
    assert status_payload["repairs"][0]["command_name"] == "stock-universe catch-up"


def test_catch_up_quiesce_stop_writes_partial_batch_between_targets(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    plan = build_catch_up_plan(
        db,
        workers=1,
        batch_size=2,
        stale_before="2021-05-09",
        run_dir=tmp_path / "operator-quiesce-run",
    )
    executed_series_ids: list[int] = []

    def execute_target(target):
        executed_series_ids.append(target.ohlcv_series_id)
        request_catch_up_stop(
            plan.run_dir,
            reason="quiesce after target",
            requested_by="pytest",
            mode="quiesce",
        )
        return {"status": "ok", "ohlcv_series_id": target.ohlcv_series_id}

    result = execute_catch_up_plan(plan, execute_target=execute_target)
    status = catch_up_run_status(plan.run_dir)
    batch_artifact = json.loads(
        (Path(plan.run_dir) / "batch_0000.json").read_text(encoding="utf-8")
    )

    assert executed_series_ids == [plan.targets[0].ohlcv_series_id]
    assert result["state"] == "operator_stopped"
    assert result["operator_stop"]["mode"] == "quiesce"
    assert result["counts"]["ok"] == 1
    assert result["counts"]["pending"] == 1
    assert status["operator_stop"]["mode"] == "quiesce"
    assert batch_artifact["partial_batch"] is True
    assert batch_artifact["status"] == "operator_quiesce_partial"
    assert batch_artifact["target_count"] == 1
    assert batch_artifact["original_batch_target_count"] == 2
    assert batch_artifact["ohlcv_series_ids"] == [plan.targets[0].ohlcv_series_id]
    assert any(
        event["event_type"] == "operator_stop_requested"
        and event["operator_stop"]["mode"] == "quiesce"
        for event in status["progress_events"]
    )


def test_catch_up_status_marks_stale_running_without_final_artifacts(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "stale-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=1, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    (run_dir / "status.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_universe.catch_up_run.v1",
                "state": "running",
                "ok": False,
                "started_at_utc": "2026-05-09T00:00:00+00:00",
                "finished_at_utc": "",
                "runner": {},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "progress.jsonl").write_text(
        json.dumps(
            {"event_type": "heartbeat", "emitted_at_utc": "2026-05-09T00:01:00+00:00"}
        )
        + "\n",
        encoding="utf-8",
    )

    status = catch_up_run_status(run_dir)

    assert status["ok"] is False
    assert status["state"] == "stale_running"
    assert status["persisted_state"] == "running"
    assert status["stale_running"] is True
    assert status["repairs"][0]["name"] == "validate-db"


def test_catch_up_resume_refuses_unartifacted_db_receipts(tmp_path: Path) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "killed-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=1, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    started_at = "2026-05-09T00:00:00+00:00"
    series_id = plan.targets[0].ohlcv_series_id
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    (run_dir / "status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "ok": False,
                "started_at_utc": started_at,
                "finished_at_utc": "",
                "runner": {},
            }
        ),
        encoding="utf-8",
    )
    _insert_unartifacted_receipt(
        db, series_id=series_id, at_utc="2026-05-09T00:01:00+00:00"
    )

    status = catch_up_run_status(run_dir)

    assert status["ok"] is False
    assert status["db_reconciliation"]["requires_reconciliation"] is True
    assert status["db_reconciliation"]["db_receipts_without_artifact_count"] == 1
    assert status["db_reconciliation"]["unartifacted_receipt_batches"] == [0]
    assert status["repairs"][0]["name"] == "validate-db"
    with pytest.raises(ValueError, match="DB execution receipts"):
        execute_catch_up_plan(
            plan,
            execute_target=lambda target: {
                "status": "ok",
                "ohlcv_series_id": target.ohlcv_series_id,
            },
            resume=True,
        )


def test_catch_up_reconcile_adopts_db_receipts_with_fidelity_and_resume_skips_them(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "reconcile-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=2, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    started_at = "2026-05-09T00:00:00+00:00"
    recovered_series_id = plan.targets[0].ohlcv_series_id
    pending_series_id = plan.targets[1].ohlcv_series_id
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    (run_dir / "status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "ok": False,
                "started_at_utc": started_at,
                "finished_at_utc": "",
                "runner": {},
            }
        ),
        encoding="utf-8",
    )
    _insert_unartifacted_receipt(
        db, series_id=recovered_series_id, at_utc="2026-05-09T00:01:00+00:00"
    )

    dry_run = reconcile_catch_up_run(run_dir)
    before_commit_status = catch_up_run_status(run_dir)
    committed = reconcile_catch_up_run(run_dir, commit=True)
    after_commit_status = catch_up_run_status(run_dir)
    repeated = reconcile_catch_up_run(run_dir, commit=True)

    assert dry_run["ok"] is True
    assert dry_run["dry_run"] is True
    assert dry_run["effects"]["did_write"] == []
    assert before_commit_status["db_reconciliation"]["requires_reconciliation"] is True
    assert committed["ok"] is True
    assert committed["recovered_series_count"] == 1
    assert committed["recovered_batch_artifact_count"] == 1
    assert (run_dir / "reconciliation.json").exists()
    recovered_artifact = json.loads(
        (run_dir / "recovered_batch_0000.json").read_text(encoding="utf-8")
    )
    assert recovered_artifact["artifact_kind"] == "recovered_from_db"
    assert recovered_artifact["recovery"]["partial_batch_recovery"] is True
    assert recovered_artifact["results"][0]["ohlcv_series_id"] == recovered_series_id
    assert recovered_artifact["results"][0]["recovered_from_db"] is True
    assert after_commit_status["ok"] is False
    assert after_commit_status["db_reconciliation"]["requires_reconciliation"] is False
    assert after_commit_status["counts"]["ok"] == 1
    assert repeated["recovered_series_count"] == 0

    executed_series_ids: list[int] = []

    def execute_target(target):
        executed_series_ids.append(target.ohlcv_series_id)
        return {"status": "ok", "ohlcv_series_id": target.ohlcv_series_id}

    resumed = execute_catch_up_plan(plan, execute_target=execute_target, resume=True)

    assert executed_series_ids == [pending_series_id]
    assert resumed["ok"] is True
    assert resumed["counts"]["ok"] == 2
    assert resumed["counts"]["pending"] == 0
    assert catch_up_run_status(run_dir)["ok"] is True


def test_long_evidence_needed_partial_batch_operator_stop_reconciles_and_resumes(
    tmp_path: Path,
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "long-evidence-needed-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=2, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    started_at = "2026-05-09T00:00:00+00:00"
    recovered_series_id = plan.targets[0].ohlcv_series_id
    long_evidence_needed_id = plan.targets[1].ohlcv_series_id
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    (run_dir / "status.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_universe.catch_up_run.v1",
                "state": "operator_stopping",
                "ok": False,
                "started_at_utc": started_at,
                "finished_at_utc": "",
                "runner": {"pid": 99999999},
                "operator_stop": {
                    "schema_version": "stock_universe.catch_up_stop.v1",
                    "run_dir": str(run_dir),
                    "reason": "operator requested investigation",
                    "requested_by": "pytest",
                    "requested_at_utc": "2026-05-09T00:01:00+00:00",
                    "mode": "quiesce",
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "progress.jsonl").write_text(
        json.dumps(
            {
                "event_type": "heartbeat",
                "message": "no target or batch completed within threshold",
                "emitted_at_utc": "2026-05-09T00:01:00+00:00",
                "activity": {
                    "progress_health": "no_recent_completion",
                    "active_batches": [
                        {
                            "batch_index": 0,
                            "current_target": {
                                "ohlcv_series_id": long_evidence_needed_id,
                                "ticker": plan.targets[1].ticker,
                                "category": plan.targets[1].category,
                            },
                        }
                    ],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    request_catch_up_stop(
        run_dir,
        reason="operator requested investigation",
        requested_by="pytest",
        mode="quiesce",
    )
    _insert_unartifacted_receipt(
        db, series_id=recovered_series_id, at_utc="2026-05-09T00:01:00+00:00"
    )

    before = catch_up_run_status(run_dir)
    committed = reconcile_catch_up_run(run_dir, commit=True)
    after = catch_up_run_status(run_dir)
    executed_series_ids: list[int] = []

    def execute_target(target):
        executed_series_ids.append(target.ohlcv_series_id)
        return {
            "status": "skipped",
            "reason": "planner returned EvidenceNeeded",
            "evidence_needed": ["alias_history", "coverage_gap"],
            "ohlcv_series_id": target.ohlcv_series_id,
        }

    resumed = execute_catch_up_plan(plan, execute_target=execute_target, resume=True)

    assert before["state"] == "stale_running"
    assert before["operator_stop"]["mode"] == "quiesce"
    assert before["db_reconciliation"]["requires_reconciliation"] is True
    assert before["db_reconciliation"]["db_receipts_without_artifact_count"] == 1
    assert committed["ok"] is True
    assert committed["recovered_series_count"] == 1
    assert after["db_reconciliation"]["requires_reconciliation"] is False
    assert executed_series_ids == [long_evidence_needed_id]
    assert resumed["counts"]["ok"] == 1
    assert resumed["counts"]["skipped"] == 1
    assert resumed["counts"]["pending"] == 0
    assert resumed["failed_results"][0]["reason"] == "planner returned EvidenceNeeded"


def test_stock_universe_catch_up_reconcile_cli_is_dry_run_by_default(
    tmp_path: Path, capsys
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "cli-reconcile-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=2, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    (run_dir / "status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "ok": False,
                "started_at_utc": "2026-05-09T00:00:00+00:00",
                "finished_at_utc": "",
                "runner": {},
            }
        ),
        encoding="utf-8",
    )
    _insert_unartifacted_receipt(
        db,
        series_id=plan.targets[0].ohlcv_series_id,
        at_utc="2026-05-09T00:01:00+00:00",
    )

    assert stock_cli_module.main(["catch-up-reconcile", "--run-dir", str(run_dir)]) == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["ok"] is True
    assert dry_run["dry_run"] is True
    assert (run_dir / "recovered_batch_0000.json").exists() is False

    assert (
        stock_cli_module.main(
            ["catch-up-reconcile", "--run-dir", str(run_dir), "--commit"]
        )
        == 0
    )
    committed = json.loads(capsys.readouterr().out)
    assert committed["ok"] is True
    assert committed["commit"] is True
    assert (run_dir / "recovered_batch_0000.json").exists()


def test_stock_universe_catch_up_stop_writes_stop_request(
    tmp_path: Path, capsys
) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "cli-stop-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=1, stale_before="2021-05-09", run_dir=run_dir
    )
    run_dir.mkdir()
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict()), encoding="utf-8")

    assert (
        stock_cli_module.main(
            [
                "catch-up-stop",
                "--run-dir",
                str(run_dir),
                "--reason",
                "maintenance window",
                "--requested-by",
                "pytest",
                "--mode",
                "abort",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "CatchUpStopRequest"
    assert payload["stop_request"]["reason"] == "maintenance window"
    assert payload["stop_request"]["mode"] == "abort"
    assert (
        json.loads((run_dir / "stop_request.json").read_text(encoding="utf-8"))[
            "requested_by"
        ]
        == "pytest"
    )


def test_xctx_catch_up_plan_and_status_surfaces(tmp_path: Path, capsys) -> None:
    db = _seed_catch_up_db(tmp_path)
    run_dir = tmp_path / "xctx-status-run"
    plan = build_catch_up_plan(
        db, workers=1, batch_size=1, stale_before="2021-05-09", run_dir=run_dir
    )
    execute_catch_up_plan(
        plan,
        execute_target=lambda target: {
            "status": "ok",
            "ohlcv_series_id": target.ohlcv_series_id,
        },
    )

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--stale-before",
                "2021-05-09",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    plan_payload = json.loads(capsys.readouterr().out)
    assert plan_payload["ok"] is True
    assert plan_payload["result_type"] == "CatchUpPlan"
    assert plan_payload["target_count"] == 2
    commit_action = next(
        action
        for action in plan_payload["next_actions"]
        if action["name"] == "commit-catch-up-run"
    )
    assert commit_action["agent_reporting"]["routine"]["system_poll_seconds"] == 60
    assert commit_action["agent_reporting"]["routine"]["first_update_seconds"] == 180
    assert str(plan_payload["run_dir"]) in " ".join(
        commit_action["agent_reporting"]["status_sources"]
    )
    assert (
        "regular status paths"
        in commit_action["agent_reporting"]["monitoring_guidance"]
    )
    assert commit_action["source_checkout_argv"] == [
        "./stock_universe.cli",
        "catch-up",
        "--db",
        str(db),
        "--workers",
        "1",
        "--batch-size",
        "1",
        "--to-date",
        "2021-05-10",
        "--stale-before",
        "2021-05-09",
        "--run-dir",
        plan_payload["run_dir"],
        "--commit",
        "--fail-fast",
    ]

    assert xctx_main(["catch-up-status", "--run-dir", str(run_dir)]) == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["ok"] is True
    assert status_payload["result_type"] == "CatchUpRunStatus"
    assert status_payload["view"] == "simple"
    assert status_payload["counts"]["ok"] == 2
    assert status_payload["monitoring"]["routine_update_seconds"] == 300
    assert "batch_artifacts" not in status_payload
    assert status_payload["batch_artifact_count"] == 2

    assert (
        xctx_main(
            ["catch-up-status", "--run-dir", str(run_dir), "--view", "extra_detail"]
        )
        == 0
    )
    full_status_payload = json.loads(capsys.readouterr().out)
    assert full_status_payload["view"] == "extra_detail"
    assert len(full_status_payload["batch_artifacts"]) == 2
    assert (
        str(run_dir / "progress.jsonl")
        in full_status_payload["agent_reporting"]["status_sources"]
    )

    assert (
        xctx_main(["catch-up-runs", "--run-root", str(tmp_path), "--limit", "2"]) == 0
    )
    runs_payload = json.loads(capsys.readouterr().out)
    assert runs_payload["ok"] is True
    assert runs_payload["result_type"] == "CatchUpRunList"
    assert runs_payload["run_count"] == 1
    assert runs_payload["runs"][0]["run_dir"] == str(run_dir)
    assert runs_payload["runs"][0]["counts"]["ok"] == 2
    assert "post_run_next_actions" not in runs_payload["runs"][0]

    assert xctx_main(["catch-up-status", "--latest", "--run-root", str(tmp_path)]) == 0
    latest_payload = json.loads(capsys.readouterr().out)
    assert latest_payload["ok"] is True
    assert latest_payload["run_dir"] == str(run_dir)


def test_xctx_quality_audit_and_catch_up_plan_simple_views(
    tmp_path: Path, capsys
) -> None:
    db = _seed_catch_up_db(tmp_path)

    assert xctx_main(["quality-audit", "--db", str(db)]) == 0
    audit_payload = json.loads(capsys.readouterr().out)

    assert audit_payload["ok"] is True
    assert audit_payload["view"] == "simple"
    assert audit_payload["issue_count"] == 2
    assert audit_payload["category_counts"]["data_not_loaded"] == 1
    assert "issues" not in audit_payload
    audit_action = audit_payload["next_moves"][0]
    assert audit_action["command_name"] == "xctx dry-run"
    assert audit_action["name"] == "dry-run-ohlcv-series-backfill"

    assert xctx_main(["quality-audit", "--db", str(db), "--view", "detail"]) == 0
    audit_detail = json.loads(capsys.readouterr().out)
    audit_action = audit_detail["next_actions"][0]
    assert audit_action["category"] in {
        "data_not_loaded",
        "listed_common_stock_data_stale",
    }
    assert audit_action["source_checkout_argv"][:5] == [
        "./stock_universe.cli",
        "xctx",
        "dry-run",
        "--ohlcv-series-id",
        str(audit_action["ohlcv_series_id"]),
    ]
    assert audit_action["authority_level"] == "network_read"

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--stale-before",
                "2021-05-09",
            ]
        )
        == 0
    )
    summary_payload = json.loads(capsys.readouterr().out)

    assert summary_payload["ok"] is True
    assert summary_payload["view"] == "simple"
    assert summary_payload["target_count"] == 2
    assert summary_payload["batch_count"] == 2
    assert str(db) in summary_payload["commit_expected_writes"]
    assert "targets" not in summary_payload
    assert "batches" not in summary_payload
    assert "ohlcv_series_ids" not in summary_payload
    assert summary_payload["executable_category_counts"] == {
        "data_not_loaded": 1,
        "listed_common_stock_data_stale": 1,
    }
    assert summary_payload["monitoring"]["poll_seconds"] == 60

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--stale-before",
                "2021-05-09",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    detail_payload = json.loads(capsys.readouterr().out)
    commit_action = next(
        action
        for action in detail_payload["next_actions"]
        if action["name"] == "commit-catch-up-run"
    )
    assert "--run-dir" in commit_action["argv"]
    assert "--stale-before" in commit_action["argv"]

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--target-limit",
                "1",
            ]
        )
        == 0
    )
    bounded_payload = json.loads(capsys.readouterr().out)
    assert bounded_payload["target_count"] == 1

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--target-limit",
                "1",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    bounded_detail = json.loads(capsys.readouterr().out)
    bounded_action = next(
        action
        for action in bounded_detail["next_actions"]
        if action["name"] == "commit-catch-up-run"
    )
    assert bounded_payload["target_count"] == 1
    assert "--target-limit" in bounded_action["argv"]
    assert "1" in bounded_action["argv"]


def test_xctx_catch_up_plan_detail_view_bounds_large_arrays(
    tmp_path: Path, capsys
) -> None:
    db = _seed_catch_up_db(tmp_path)

    assert (
        xctx_main(
            [
                "catch-up-plan",
                "--db",
                str(db),
                "--workers",
                "1",
                "--batch-size",
                "1",
                "--stale-before",
                "2021-05-09",
                "--view",
                "detail",
                "--detail-limit",
                "1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["view"] == "detail"
    assert payload["detail_limit"] == 1
    assert len(payload["target_detail"]) == 1
    assert len(payload["batch_detail"]) == 1
    assert payload["omitted_target_count"] == 1
    assert payload["omitted_batch_count"] == 1
    assert "targets" not in payload
    assert "batches" not in payload


def test_sqlite_repository_uses_busy_timeout_for_concurrent_small_writes(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    def write_key(index: int) -> int:
        return SQLiteStockUniverseRepository(db).ensure_ohlcv_series_id(
            f"concurrent:{index}"
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(write_key, range(40)))

    assert len(set(ids)) == 40
    assert SQLiteStockUniverseRepository(db).counts()["ohlcv_series_id_lookup"] == 40


def _seed_catch_up_db(tmp_path: Path) -> Path:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.ensure_schema()
    repository.upsert_reference_snapshots(
        [
            _snapshot("NEWCO", "New Test Company", "figi-new", "share-new"),
            _snapshot("STALE", "Stale Test Company", "figi-stale", "share-stale"),
            _snapshot("FRESH", "Fresh Test Company", "figi-fresh", "share-fresh"),
        ]
    )
    stale_id = repository.lookup_ohlcv_series_id("test:STALE")
    fresh_id = repository.lookup_ohlcv_series_id("test:FRESH")
    assert stale_id is not None
    assert fresh_id is not None
    repository.insert_bars(
        [
            _bar(stale_id, "STALE", "2021-05-05", 1620172800000),
            _bar(fresh_id, "FRESH", "2021-05-10", 1620604800000),
        ]
    )
    return db


def _snapshot(
    ticker: str, name: str, composite_figi: str, share_class_figi: str
) -> StoredReferenceSnapshot:
    return StoredReferenceSnapshot(
        provider="massive.reference_tickers",
        snapshot_as_of_date="2026-05-08",
        ticker=ticker,
        active=True,
        company_name=name,
        cik="0000000000",
        composite_figi=composite_figi,
        share_class_figi=share_class_figi,
        security_type="CS",
        primary_exchange="XNAS",
        market="stocks",
        locale="us",
        identity_status="active",
        natural_key=f"test:{ticker}",
        raw={"ticker": ticker},
    )


def _bar(
    series_id: int, ticker: str, bar_date: str, bar_start_ts: int
) -> StoredOhlcvBar:
    return StoredOhlcvBar(
        series_id=series_id,
        ticker=ticker,
        bar_date=bar_date,
        bar_start_ts=bar_start_ts,
        multiplier=1,
        timespan="day",
        adjusted=True,
        open=10,
        high=11,
        low=9,
        close=10,
        volume=100,
    )


def _resource_check(free_bytes: int) -> dict:
    status = "ok"
    if free_bytes < 3 * 1024 * 1024 * 1024:
        status = "draining"
    elif free_bytes < 5 * 1024 * 1024 * 1024:
        status = "critical"
    elif free_bytes < 10 * 1024 * 1024 * 1024:
        status = "warning"
    return {
        "checked_at_utc": "2026-05-09T00:00:00+00:00",
        "disk": {
            "status": status,
            "min_free_bytes": free_bytes,
            "min_free_gb": round(free_bytes / (1024**3), 3),
            "checks": [{"path": "/tmp", "free_bytes": free_bytes}],
        },
        "memory": {
            "status": "observed",
            "available_bytes": 1,
            "available_gb": 1,
            "policy": "observed_only",
        },
    }


def _insert_unartifacted_receipt(db: Path, *, series_id: int, at_utc: str) -> None:
    repository = SQLiteStockUniverseRepository(db)
    request_hash = f"request-{series_id}"
    ledger_hash = f"ledger-{series_id}"
    with repository.connect() as conn:
        conn.execute(
            """
            INSERT INTO execution_approvals(
              request_hash, evidence_ledger_hash, plan_hash, ohlcv_series_id, plan_status,
              approved_by, allow_caution_flag, reason, approved_at_utc, approval_json,
              approval_hash, inserted_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_hash,
                ledger_hash,
                f"plan-{series_id}",
                series_id,
                "approved",
                "pytest",
                0,
                "test",
                at_utc,
                "{}",
                f"approval-{series_id}",
                at_utc,
            ),
        )
    repository.insert_execution_receipt(
        {
            "request_hash": request_hash,
            "evidence_ledger_hash": ledger_hash,
            "ohlcv_series_id": series_id,
            "status": "ok",
            "approved_by": "pytest",
            "started_at_utc": at_utc,
            "finished_at_utc": at_utc,
            "planned_segment_count": 1,
            "fetched_bar_count": 1,
            "inserted_bar_count": 1,
            "request_log": [],
        }
    )
