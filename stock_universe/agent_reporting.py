"""User-facing long-running command reporting policies."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable


DEFAULT_IMMEDIATE_UPDATE_ON = (
    "hard_error",
    "nonzero_exit",
    "resource_warning",
    "resource_critical",
    "resource_drain",
    "validation_failure",
    "no_progress_or_stall",
)
AGENT_REPORTING_VERSION = "agent_reporting.v2"
DEFAULT_POLL_SECONDS = 60
DEFAULT_FIRST_USER_UPDATE_SECONDS = 180
DEFAULT_USER_UPDATE_SECONDS = 300
DEFAULT_STALL_SECONDS = 180
DEFAULT_FINAL_REPORT = ("ok", "counts", "errors", "warnings", "next_actions")


def native_progress_policy(
    *,
    mode: str = "none",
    prefix: str = "",
    events: Iterable[str] = (),
    heartbeat_arg: str = "",
    summary_arg: str = "",
    artifacts: Iterable[str] = (),
) -> dict[str, Any]:
    """Return the machine-readable command-native progress contract."""
    payload: dict[str, Any] = {"mode": mode}
    if prefix:
        payload["prefix"] = prefix
    event_list = list(events)
    if event_list:
        payload["events"] = event_list
    if heartbeat_arg:
        payload["heartbeat_arg"] = heartbeat_arg
    if summary_arg:
        payload["summary_arg"] = summary_arg
    artifact_list = list(artifacts)
    if artifact_list:
        payload["artifacts"] = artifact_list
    return payload


def agent_reporting_policy(
    *,
    applies_when: str,
    begin: str,
    final: str,
    status_sources: Iterable[str] = (),
    monitoring_guidance: str = "",
    immediate_update_on: Iterable[str] = DEFAULT_IMMEDIATE_UPDATE_ON,
    quiet_when_healthy: bool = True,
    native_progress: dict[str, Any] | None = None,
    final_report: Iterable[str] = DEFAULT_FINAL_REPORT,
    stall_seconds: int = DEFAULT_STALL_SECONDS,
) -> dict[str, Any]:
    """Return the compact user-facing reporting policy contract."""
    immediate_updates = list(immediate_update_on)
    payload: dict[str, Any] = {
        "version": AGENT_REPORTING_VERSION,
        "applies_when": applies_when,
        "native_progress": native_progress or native_progress_policy(),
        "poll_seconds": DEFAULT_POLL_SECONDS,
        "first_user_update_seconds": DEFAULT_FIRST_USER_UPDATE_SECONDS,
        "user_update_seconds": DEFAULT_USER_UPDATE_SECONDS,
        "stall_seconds": stall_seconds,
        "quiet_when_healthy": quiet_when_healthy,
        "immediate_on": immediate_updates,
        "final_report": list(final_report),
        "begin": begin,
        "routine": {
            "system_poll_seconds": DEFAULT_POLL_SECONDS,
            "first_update_seconds": DEFAULT_FIRST_USER_UPDATE_SECONDS,
            "default_update_seconds": DEFAULT_USER_UPDATE_SECONDS,
            "quiet_when_healthy": quiet_when_healthy,
        },
        "immediate_update_on": immediate_updates,
        "final": final,
        "operator_override": "Latest user instruction wins.",
    }
    sources = list(status_sources)
    if sources:
        payload["status_sources"] = sources
    if monitoring_guidance:
        payload["monitoring_guidance"] = monitoring_guidance
    return payload


def catch_up_reporting_policy(
    *,
    run_dir: str | Path | None = None,
    target_count: int | None = None,
) -> dict[str, Any]:
    run_text = str(run_dir) if run_dir else "{run_dir}"
    target_text = "" if target_count is None else f" Target count: {target_count}."
    return agent_reporting_policy(
        applies_when="commit=true and expected_duration_seconds >= 10",
        begin=(
            "Tell the end-user the catch-up commit run is starting, that it writes SQLite rows "
            "and catch_up_runs artifacts, where status evidence will exist, and what would "
            f"count as an issue.{target_text}"
        ),
        status_sources=(
            "stderr JSON lines prefixed 'catch-up progress:'",
            _run_artifact(run_text, "progress.jsonl"),
            _run_artifact(run_text, "status.json"),
            f"xctx catch-up-status --run-dir {run_text}",
        ),
        native_progress=native_progress_policy(
            mode="stderr_jsonl",
            prefix="catch-up progress: ",
            events=(
                "started",
                "heartbeat",
                "mini_summary",
                "summary",
                "hard_error",
                "operator_stop_requested",
                "finished",
                "stopped",
            ),
            heartbeat_arg="--heartbeat-seconds",
            summary_arg="--summary-seconds",
            artifacts=(
                _run_artifact(run_text, "progress.jsonl"),
                _run_artifact(run_text, "status.json"),
            ),
        ),
        monitoring_guidance=(
            "Use status_sources once every 60 seconds for routine updates. During committed writes, "
            "progress.jsonl, stderr, and xctx catch-up-status are the regular status paths."
        ),
        final=(
            "Summarize finished/stopped state, target and result counts, hard/resource/operator "
            "errors or warnings, evidence paths, and the next xctx action."
        ),
    )


def validate_db_reporting_policy() -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="expected_duration_seconds >= 10",
        begin=(
            "Tell the end-user validate-db is starting, whether schema initialization may write, "
            "that validation progress is emitted as raw JSON lines on stderr, and that validation failures or "
            "nonzero exit are issues."
        ),
        status_sources=("stderr raw JSON lines",),
        native_progress=native_progress_policy(
            mode="stderr_jsonl",
            events=("starting", "heartbeat", "summary", "finished", "error"),
            heartbeat_arg="--heartbeat-seconds",
            summary_arg="--summary-seconds",
        ),
        immediate_update_on=(
            "validation_failure",
            "hard_error",
            "nonzero_exit",
            "no_progress_or_stall",
        ),
        final="Summarize validation ok/fail, counts, failures, warnings, and the next action.",
    )


def update_reference_universe_reporting_policy() -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="commit=true or max_pages > 1 or expected_duration_seconds >= 10",
        begin=(
            "Tell the end-user reference-universe refresh is starting, whether --commit writes "
            "the SQLite DB, whether raw capture is enabled, and what failures or stalls would be issues."
        ),
        status_sources=(
            "stderr JSON lines prefixed 'update-reference-universe progress:'",
            "stdout ReferenceUniverseUpdate result",
            "capture-dir raw files when capture_dir is provided",
        ),
        native_progress=native_progress_policy(
            mode="stderr_jsonl",
            prefix="update-reference-universe progress: ",
            events=(
                "started",
                "heartbeat",
                "summary",
                "page_fetched",
                "finished",
                "error",
            ),
            heartbeat_arg="--heartbeat-seconds",
            summary_arg="--summary-seconds",
        ),
        final="Summarize fetched pages, persisted rows, errors or warnings, evidence path, and next action.",
    )


def backfill_reference_batch_reporting_policy() -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="commit=true and (limit > 1 or len(ohlcv_series_id) > 1 or expected_duration_seconds >= 10)",
        begin=(
            "Tell the end-user the reference-batch backfill is starting, what bounded selection "
            "will execute, whether --commit writes, and what error/stall signals require attention."
        ),
        status_sources=(
            "stderr JSON lines prefixed 'backfill-reference-batch progress:'",
            "stdout ReferenceBatchManifest result",
            "SQLite execution receipts when commit=true",
        ),
        native_progress=native_progress_policy(
            mode="stderr_jsonl",
            prefix="backfill-reference-batch progress: ",
            events=(
                "started",
                "heartbeat",
                "summary",
                "input_started",
                "input_finished",
                "finished",
                "error",
            ),
            heartbeat_arg="--heartbeat-seconds",
            summary_arg="--summary-seconds",
        ),
        final="Summarize selected series, execution results, errors or warnings, and next action.",
    )


def backfill_reporting_policy() -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="multiple inputs are supplied or one live execution crosses 10 seconds",
        begin=(
            "Tell the end-user the live backfill is starting, which input set will execute, "
            "that it writes SQLite DB output, and what failures or stalls would be issues."
        ),
        status_sources=(
            "stderr JSON lines prefixed 'backfill progress:'",
            "stdout ResultEnvelope result",
            "SQLite execution receipts",
        ),
        native_progress=native_progress_policy(
            mode="stderr_jsonl",
            prefix="backfill progress: ",
            events=(
                "started",
                "heartbeat",
                "summary",
                "input_started",
                "input_finished",
                "finished",
                "error",
            ),
            heartbeat_arg="--heartbeat-seconds",
            summary_arg="--summary-seconds",
        ),
        final="Summarize per-input outcome, receipt evidence, errors or warnings, and next action.",
    )


def soft_long_running_reporting_policy(
    *, action: str, status_source: str
) -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="expected_duration_seconds >= 10 or runtime crosses 10 seconds",
        begin=(
            f"Begin is optional for short {action} runs; after 10 seconds, tell the end-user "
            "it is still gathering evidence and where status can be checked."
        ),
        status_sources=(status_source,),
        immediate_update_on=("hard_error", "nonzero_exit", "no_progress_or_stall"),
        final="Summarize outcome, evidence gathered, errors or warnings, and next action.",
    )


def recipe_reporting_policy(*, workflow: str) -> dict[str, Any]:
    return agent_reporting_policy(
        applies_when="any workflow step expects duration >= 10 seconds or crosses 10 seconds",
        begin=(
            f"Tell the end-user the {workflow} workflow is starting, which steps may write, "
            "where status evidence will exist, and what would count as an issue."
        ),
        status_sources=("step-level agent_reporting status_sources",),
        final="Summarize workflow outcome, evidence paths, warnings/errors, and next action.",
    )


def _run_artifact(run_dir: str, name: str) -> str:
    if run_dir == "{run_dir}":
        return f"{run_dir}/{name}"
    return str(Path(run_dir) / name)
