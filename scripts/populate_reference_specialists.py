#!/usr/bin/env python3
"""Populate the canonical DB from reference-universe batches with a fixed worker pool."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TextIO


REPO_ROOT = Path(__file__).resolve().parents[1]
CLI = REPO_ROOT / "stock_universe.cli"
DEFAULT_DB = REPO_ROOT / "production_build" / "stock_universe.sqlite"
DEFAULT_RUN_ROOT = REPO_ROOT / "production_build" / "population_runs"


@dataclass
class TaskRecord:
    block_index: int
    offset: int
    limit: int
    command: list[str]
    stdout_json: str
    stderr_log: str
    status: str = "pending"
    started_at_utc: str = ""
    finished_at_utc: str = ""
    returncode: int | None = None
    selected: int = 0
    ok_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    pending_count: int = 0
    fetched_bar_count: int = 0
    inserted_bar_count: int = 0
    error_message: str = ""


@dataclass
class RunningTask:
    record: TaskRecord
    process: subprocess.Popen[str]
    stdout_handle: TextIO
    stderr_handle: TextIO


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(args.run_dir) if args.run_dir else _default_run_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    args.db = str(Path(args.db))

    command_log = run_dir / "commands.jsonl"
    state_path = run_dir / "status.json"
    summary_path = run_dir / "summary.json"

    if not CLI.exists():
        print(f"population: missing source checkout CLI: {CLI}", file=sys.stderr)
        return 2
    if not os.environ.get("MASSIVE_API_KEY"):
        print(
            "population: MASSIVE_API_KEY is required for committed live population",
            file=sys.stderr,
        )
        return 2

    started_at = _utc_now()
    print(
        "population: starting "
        f"run_dir={_display_path(run_dir)} db={args.db} "
        f"parallel={args.parallel_tasks} block_size={args.block_size}",
        flush=True,
    )

    try:
        doctor = _run_json(
            [str(CLI), "xctx", "doctor", "--db", args.db],
            run_dir / "preflight_doctor.json",
            run_dir / "preflight_doctor.stderr.log",
            command_log,
        )
        if not doctor.get("ok"):
            raise PopulationStop("xctx doctor failed; see preflight_doctor.json")

        before_status = _run_json(
            [str(CLI), "xctx", "universe-status", "--db", args.db],
            run_dir / "preflight_universe_status.json",
            run_dir / "preflight_universe_status.stderr.log",
            command_log,
        )
        if not before_status.get("ok"):
            raise PopulationStop(
                "xctx universe-status failed; see preflight_universe_status.json"
            )

        if not args.skip_reference_update:
            reference_payload = _update_reference_universe(args, run_dir, command_log)
            if not reference_payload.get("ok"):
                raise PopulationStop(
                    "reference-universe update failed; see reference_update.json"
                )
            if (
                not reference_payload.get("complete")
                and not args.allow_incomplete_reference
            ):
                raise PopulationStop(
                    "reference-universe update has pending requests; rerun with a wider limit/max-pages or --allow-incomplete-reference"
                )

        manifest = _run_json(
            _batch_manifest_command(args, limit=1, offset=args.start_offset),
            run_dir / "initial_batch_manifest.json",
            run_dir / "initial_batch_manifest.stderr.log",
            command_log,
        )
        total_available = int(
            ((manifest.get("counts") or {}).get("total_available")) or 0
        )
        if total_available <= args.start_offset:
            raise PopulationStop(
                f"no reference rows available at start offset {args.start_offset}; total_available={total_available}"
            )

        tasks = _planned_tasks(args, run_dir, total_available)
        if not tasks:
            raise PopulationStop("no batch tasks were planned")

        _write_state(
            state_path,
            args=args,
            run_dir=run_dir,
            started_at=started_at,
            total_available=total_available,
            tasks=tasks,
            stop_requested=False,
        )
        print(
            "population: planned "
            f"tasks={len(tasks)} total_available={total_available} "
            f"offsets={tasks[0].offset}..{tasks[-1].offset}",
            flush=True,
        )

        ok = _run_pool(
            args=args,
            run_dir=run_dir,
            command_log=command_log,
            state_path=state_path,
            started_at=started_at,
            total_available=total_available,
            tasks=tasks,
        )
        final_status = _run_json(
            [str(CLI), "xctx", "universe-status", "--db", args.db],
            run_dir / "final_universe_status.json",
            run_dir / "final_universe_status.stderr.log",
            command_log,
        )
        summary = _summary_payload(
            args=args,
            run_dir=run_dir,
            started_at=started_at,
            total_available=total_available,
            tasks=tasks,
            ok=ok,
            final_status=final_status,
        )
        summary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if ok:
            print(
                f"population: complete ok=true summary={_display_path(summary_path)}",
                flush=True,
            )
            return 0
        print(
            f"population: all STOP ok=false summary={_display_path(summary_path)}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    except PopulationStop as exc:
        stopped = {
            "ok": False,
            "status": "stopped",
            "reason": str(exc),
            "run_dir": str(run_dir),
            "started_at_utc": started_at,
            "finished_at_utc": _utc_now(),
        }
        summary_path.write_text(
            json.dumps(stopped, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"population: all STOP {exc}", file=sys.stderr, flush=True)
        return 1


class PopulationStop(RuntimeError):
    pass


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", default=str(DEFAULT_DB), help="Canonical SQLite DB path."
    )
    parser.add_argument(
        "--run-dir",
        default="",
        help="Output directory for this run. Defaults under production_build/population_runs.",
    )
    parser.add_argument(
        "--parallel-tasks",
        type=int,
        default=10,
        help="Concurrent committed batch tasks.",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=30,
        help="Reference rows per committed batch task.",
    )
    parser.add_argument(
        "--blocks",
        type=int,
        default=10,
        help="Number of blocks to schedule unless --all is set.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Schedule blocks from --start-offset through the current available universe.",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Reference-universe offset for the first block.",
    )
    parser.add_argument(
        "--monitor-seconds",
        type=int,
        default=180,
        help="Polling interval for child task status.",
    )
    parser.add_argument(
        "--update-seconds",
        type=int,
        default=600,
        help="Minimum interval for stdout progress updates.",
    )
    parser.add_argument(
        "--reference-limit", type=int, default=1000, help="Reference update page size."
    )
    parser.add_argument(
        "--reference-max-pages",
        type=int,
        default=100,
        help="Reference update page cap.",
    )
    parser.add_argument(
        "--skip-reference-update",
        action="store_true",
        help="Use the existing persisted reference universe.",
    )
    parser.add_argument(
        "--allow-incomplete-reference",
        action="store_true",
        help="Continue if update-reference-universe reports pending requests.",
    )
    parser.add_argument(
        "--market",
        default="stocks",
        help="Market filter passed to reference update and batches.",
    )
    parser.add_argument(
        "--exchange",
        default="",
        help="Optional primary exchange filter, for example XNAS.",
    )
    parser.add_argument(
        "--active", choices=("active", "inactive", "all"), default="active"
    )
    parser.add_argument(
        "--from-date", default="", help="Optional backfill start date override."
    )
    parser.add_argument(
        "--to-date", default="", help="Optional backfill end date override."
    )
    parser.add_argument(
        "--max-rounds", type=int, default=8, help="Planner rounds per selected series."
    )
    parser.add_argument(
        "--no-caution",
        action="store_true",
        help="Skip caution plans instead of approving them.",
    )
    args = parser.parse_args(argv)
    if args.parallel_tasks < 1:
        parser.error("--parallel-tasks must be positive")
    if args.block_size < 1 or args.block_size > 1000:
        parser.error("--block-size must be between 1 and 1000")
    if args.blocks < 1:
        parser.error("--blocks must be positive")
    if args.start_offset < 0:
        parser.error("--start-offset must be non-negative")
    if args.monitor_seconds < 1:
        parser.error("--monitor-seconds must be positive")
    if args.update_seconds < args.monitor_seconds:
        parser.error(
            "--update-seconds must be greater than or equal to --monitor-seconds"
        )
    return args


def _update_reference_universe(
    args: argparse.Namespace, run_dir: Path, command_log: Path
) -> dict[str, Any]:
    command = [
        str(CLI),
        "update-reference-universe",
        "--db",
        args.db,
        "--limit",
        str(args.reference_limit),
        "--max-pages",
        str(args.reference_max_pages),
        "--market",
        args.market,
        "--active",
        args.active,
        "--commit",
    ]
    if args.exchange:
        command.extend(["--exchange", args.exchange])
    print(
        "population: updating reference universe "
        f"limit={args.reference_limit} max_pages={args.reference_max_pages}",
        flush=True,
    )
    return _run_json(
        command,
        run_dir / "reference_update.json",
        run_dir / "reference_update.stderr.log",
        command_log,
    )


def _batch_manifest_command(
    args: argparse.Namespace, *, limit: int, offset: int
) -> list[str]:
    command = [
        str(CLI),
        "backfill-reference-batch",
        "--db",
        args.db,
        "--limit",
        str(limit),
        "--offset",
        str(offset),
        "--market",
        args.market,
        "--active",
        args.active,
    ]
    if args.exchange:
        command.extend(["--exchange", args.exchange])
    return command


def _batch_commit_command(
    args: argparse.Namespace, *, limit: int, offset: int
) -> list[str]:
    command = _batch_manifest_command(args, limit=limit, offset=offset)
    command.extend(["--commit", "--max-rounds", str(args.max_rounds)])
    if args.from_date:
        command.extend(["--from-date", args.from_date])
    if args.to_date:
        command.extend(["--to-date", args.to_date])
    if args.no_caution:
        command.append("--no-caution")
    return command


def _planned_tasks(
    args: argparse.Namespace, run_dir: Path, total_available: int
) -> list[TaskRecord]:
    remaining = max(total_available - args.start_offset, 0)
    if args.all:
        block_count = (remaining + args.block_size - 1) // args.block_size
    else:
        block_count = min(
            args.blocks, (remaining + args.block_size - 1) // args.block_size
        )
    tasks = []
    for block_index in range(block_count):
        offset = args.start_offset + (block_index * args.block_size)
        task_name = f"block_{block_index:04d}_offset_{offset:06d}"
        tasks.append(
            TaskRecord(
                block_index=block_index,
                offset=offset,
                limit=args.block_size,
                command=_batch_commit_command(
                    args, limit=args.block_size, offset=offset
                ),
                stdout_json=str(run_dir / f"{task_name}.json"),
                stderr_log=str(run_dir / f"{task_name}.stderr.log"),
            )
        )
    return tasks


def _run_pool(
    *,
    args: argparse.Namespace,
    run_dir: Path,
    command_log: Path,
    state_path: Path,
    started_at: str,
    total_available: int,
    tasks: list[TaskRecord],
) -> bool:
    running: dict[int, RunningTask] = {}
    next_index = 0
    stop_requested = False
    last_update = time.monotonic()
    next_monitor = time.monotonic()

    while (not stop_requested and next_index < len(tasks)) or running:
        while (
            not stop_requested
            and next_index < len(tasks)
            and len(running) < args.parallel_tasks
        ):
            task = tasks[next_index]
            running[task.block_index] = _start_task(task, command_log)
            next_index += 1

        sleep_for = max(next_monitor - time.monotonic(), 0)
        if sleep_for:
            time.sleep(sleep_for)
        next_monitor = time.monotonic() + args.monitor_seconds

        finished = _collect_finished(running)
        if any(task.status == "failed" for task in finished):
            stop_requested = True

        _write_state(
            state_path,
            args=args,
            run_dir=run_dir,
            started_at=started_at,
            total_available=total_available,
            tasks=tasks,
            stop_requested=stop_requested,
        )

        now = time.monotonic()
        if stop_requested or now - last_update >= args.update_seconds or not running:
            _print_progress(
                tasks, running_count=len(running), stop_requested=stop_requested
            )
            last_update = now

    return all(task.status == "ok" for task in tasks)


def _start_task(task: TaskRecord, command_log: Path) -> RunningTask:
    task.status = "running"
    task.started_at_utc = _utc_now()
    stdout_handle = Path(task.stdout_json).open("w", encoding="utf-8")
    stderr_handle = Path(task.stderr_log).open("w", encoding="utf-8")
    _append_command_log(
        command_log,
        task.command,
        stdout_path=task.stdout_json,
        stderr_path=task.stderr_log,
    )
    process = subprocess.Popen(
        task.command,
        cwd=REPO_ROOT,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
    )
    return RunningTask(
        record=task,
        process=process,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
    )


def _collect_finished(running: dict[int, RunningTask]) -> list[TaskRecord]:
    finished: list[TaskRecord] = []
    for block_index, running_task in list(running.items()):
        returncode = running_task.process.poll()
        if returncode is None:
            continue
        running_task.stdout_handle.close()
        running_task.stderr_handle.close()
        task = running_task.record
        task.returncode = returncode
        task.finished_at_utc = _utc_now()
        _finish_task_from_output(task)
        finished.append(task)
        del running[block_index]
    return finished


def _finish_task_from_output(task: TaskRecord) -> None:
    payload = _read_json(Path(task.stdout_json))
    counts = payload.get("counts") if isinstance(payload, dict) else {}
    task.selected = int((counts or {}).get("selected") or 0)
    task.pending_count = int((counts or {}).get("pending") or 0)
    task.ok_count = int((counts or {}).get("ok") or 0)
    task.skipped_count = int((counts or {}).get("skipped") or 0)
    task.error_count = int((counts or {}).get("error") or 0)
    results = payload.get("results") if isinstance(payload, dict) else []
    if isinstance(results, list):
        task.fetched_bar_count = sum(
            int(item.get("fetched_bar_count") or 0)
            for item in results
            if isinstance(item, dict)
        )
        task.inserted_bar_count = sum(
            int(item.get("inserted_bar_count") or 0)
            for item in results
            if isinstance(item, dict)
        )
    validation = payload.get("validation") if isinstance(payload, dict) else {}
    validation_ok = not isinstance(validation, dict) or bool(validation.get("ok", True))
    payload_readable = isinstance(payload, dict) and "error" not in payload
    if (
        task.returncode == 0
        and payload_readable
        and validation_ok
        and task.error_count == 0
    ):
        task.status = "ok"
        return
    task.status = "failed"
    if isinstance(payload, dict) and payload.get("repair_hints"):
        task.error_message = "batch reported repair_hints"
    elif isinstance(payload, dict) and payload.get("error"):
        task.error_message = str(payload.get("error"))
    else:
        stderr = (
            Path(task.stderr_log).read_text(encoding="utf-8", errors="replace").strip()
        )
        task.error_message = (
            stderr[-2000:]
            if stderr
            else f"batch failed with returncode={task.returncode}"
        )


def _run_json(
    command: list[str], stdout_path: Path, stderr_path: Path, command_log: Path
) -> dict[str, Any]:
    _append_command_log(
        command_log, command, stdout_path=str(stdout_path), stderr_path=str(stderr_path)
    )
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_handle,
        stderr_path.open("w", encoding="utf-8") as stderr_handle,
    ):
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            check=False,
        )
    payload = _read_json(stdout_path)
    if completed.returncode != 0:
        stderr = stderr_path.read_text(encoding="utf-8", errors="replace").strip()
        message = (
            stderr[-2000:]
            if stderr
            else f"{command[1:]} failed with returncode={completed.returncode}"
        )
        raise PopulationStop(message)
    return payload


def _read_json(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {"ok": False, "error": str(exc)}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return {
            "ok": False,
            "error": f"JSON parse failed: {exc}",
            "preview": text[-2000:],
        }
    return (
        payload
        if isinstance(payload, dict)
        else {"ok": False, "error": "JSON payload is not an object"}
    )


def _write_state(
    path: Path,
    *,
    args: argparse.Namespace,
    run_dir: Path,
    started_at: str,
    total_available: int,
    tasks: list[TaskRecord],
    stop_requested: bool,
) -> None:
    path.write_text(
        json.dumps(
            {
                "ok": not stop_requested
                and all(task.status in {"pending", "running", "ok"} for task in tasks),
                "status": "running"
                if any(task.status == "running" for task in tasks)
                else "idle",
                "stop_requested": stop_requested,
                "run_dir": str(run_dir),
                "db": args.db,
                "started_at_utc": started_at,
                "last_checked_utc": _utc_now(),
                "config": {
                    "parallel_tasks": args.parallel_tasks,
                    "block_size": args.block_size,
                    "blocks": args.blocks,
                    "all": args.all,
                    "start_offset": args.start_offset,
                    "monitor_seconds": args.monitor_seconds,
                    "update_seconds": args.update_seconds,
                    "market": args.market,
                    "exchange": args.exchange,
                    "active": args.active,
                },
                "total_available": total_available,
                "totals": _task_totals(tasks),
                "tasks": [asdict(task) for task in tasks],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _summary_payload(
    *,
    args: argparse.Namespace,
    run_dir: Path,
    started_at: str,
    total_available: int,
    tasks: list[TaskRecord],
    ok: bool,
    final_status: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": ok,
        "status": "complete" if ok else "stopped",
        "run_dir": str(run_dir),
        "db": args.db,
        "started_at_utc": started_at,
        "finished_at_utc": _utc_now(),
        "total_available": total_available,
        "totals": _task_totals(tasks),
        "tasks": [asdict(task) for task in tasks],
        "final_universe_status": final_status.get("status", final_status),
    }


def _task_totals(tasks: list[TaskRecord]) -> dict[str, int]:
    return {
        "planned": len(tasks),
        "pending": sum(1 for task in tasks if task.status == "pending"),
        "running": sum(1 for task in tasks if task.status == "running"),
        "ok": sum(1 for task in tasks if task.status == "ok"),
        "failed": sum(1 for task in tasks if task.status == "failed"),
        "selected": sum(task.selected for task in tasks),
        "series_ok": sum(task.ok_count for task in tasks),
        "series_skipped": sum(task.skipped_count for task in tasks),
        "series_error": sum(task.error_count for task in tasks),
        "fetched_bars": sum(task.fetched_bar_count for task in tasks),
        "inserted_bars": sum(task.inserted_bar_count for task in tasks),
    }


def _print_progress(
    tasks: list[TaskRecord], *, running_count: int, stop_requested: bool
) -> None:
    totals = _task_totals(tasks)
    status = "STOP requested" if stop_requested else "running"
    print(
        f"population: {_utc_now()} {status} "
        f"tasks_ok={totals['ok']}/{totals['planned']} "
        f"failed={totals['failed']} running={running_count} "
        f"series_ok={totals['series_ok']} skipped={totals['series_skipped']} "
        f"errors={totals['series_error']} inserted_bars={totals['inserted_bars']}",
        flush=True,
    )


def _append_command_log(
    command_log: Path, command: list[str], *, stdout_path: str, stderr_path: str
) -> None:
    command_log.parent.mkdir(parents=True, exist_ok=True)
    with command_log.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "at_utc": _utc_now(),
                    "command": command,
                    "stdout": stdout_path,
                    "stderr": stderr_path,
                },
                sort_keys=True,
            )
            + "\n"
        )


def _default_run_dir() -> Path:
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_RUN_ROOT / f"reference_specialists_{stamp}"


def _utc_now() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
