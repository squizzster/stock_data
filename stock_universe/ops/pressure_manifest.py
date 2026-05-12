"""Pressure-run manifest helpers.

The manifest is intentionally independent from the live backfill harness. It
records enough context to compare pressure runs across commits without making
the runner itself responsible for planning or execution.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class PressureCohortPlan:
    name: str
    target_size: int | None
    description: str
    selection: tuple[str, ...]
    success_factors: tuple[str, ...]
    next_cohort: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "known": True,
            "name": self.name,
            "target_size": self.target_size,
            "description": self.description,
            "selection": list(self.selection),
            "success_factors": list(self.success_factors),
            "next_cohort": self.next_cohort,
        }


PRESSURE_COHORT_PLANS = (
    PressureCohortPlan(
        name="baseline-50",
        target_size=50,
        description="Original pressure cohort that proved the full live engine path.",
        selection=(
            "Use the committed baseline pressure report order.",
            "Include ordinary, renamed, warrant, omitted, and historical rekey cases.",
        ),
        success_factors=(
            "attempted equals 50",
            "ok_count equals attempted",
            "skipped_count equals 0",
            "error_count equals 0",
            "SQLite validation is clean",
            "each receipt has a durable approval",
        ),
        next_cohort="expansion-100",
    ),
    PressureCohortPlan(
        name="expansion-100",
        target_size=100,
        description="First broader mixed cohort after baseline-50 remains clean.",
        selection=(
            "Start with baseline-50.",
            "Add ordinary active common stocks.",
            "Add recent rename and split-adjacent cases.",
            "Avoid concentrating the new 50 cases in one exchange or category.",
        ),
        success_factors=(
            "ok_count equals attempted",
            "skipped_count equals 0",
            "error_count equals 0",
            "request-count outliers are reviewed",
            "new failures become typed regression cases or typed evidence gaps",
        ),
        next_cohort="expansion-250",
    ),
    PressureCohortPlan(
        name="expansion-250",
        target_size=250,
        description="Larger mixed cohort before category-specific widening.",
        selection=(
            "Start with expansion-100.",
            "Add more active common stocks across primary exchanges.",
            "Add known historical ticker changes.",
            "Add securities with sparse early coverage.",
        ),
        success_factors=(
            "ok_count equals attempted",
            "skipped_count equals 0",
            "error_count equals 0",
            "DB validation is clean",
            "audit rows exist for every receipt",
            "request-count outliers have a follow-up decision",
        ),
        next_cohort="exchange-category",
    ),
    PressureCohortPlan(
        name="exchange-category",
        target_size=None,
        description="Exchange and security-category cohorts after the mixed 250 run.",
        selection=(
            "Run exchange-specific common-stock cohorts.",
            "Run warrant, unit, preferred, and rights cohorts separately.",
            "Keep each category cohort independently reproducible.",
        ),
        success_factors=(
            "category-specific failures are not hidden inside aggregate counts",
            "identity rules stay category-driven rather than ticker-specific",
            "each cohort has its own manifest and audit review",
        ),
        next_cohort="ugly-historical",
    ),
    PressureCohortPlan(
        name="ugly-historical",
        target_size=None,
        description="Known difficult historical ticker and identity-change cases.",
        selection=(
            "Include historical FIGI rekeys.",
            "Include same-ticker reuse cases.",
            "Include long gaps and sparse coverage cases.",
            "Promote confirmed failures into typed regression cases.",
        ),
        success_factors=(
            "hard cases resolve through typed evidence, not exceptions",
            "unresolved cases produce precise EvidenceNeeded results",
            "new planner behavior has typed regression coverage before broad reuse",
        ),
        next_cohort="maintain-and-repeat",
    ),
)


def build_pressure_run_manifest(
    *,
    report_path: str | Path,
    cohort: str,
    command: Iterable[str] | str,
    db_path: str | Path | None = None,
    generated_at_utc: str | None = None,
    repo_root: str | Path | None = None,
    git_head: str | None = None,
    git_dirty: bool | None = None,
) -> dict[str, Any]:
    """Build a compact manifest for a pressure-run report."""
    root = (
        Path(repo_root)
        if repo_root is not None
        else Path(__file__).resolve().parents[2]
    )
    report = Path(report_path)
    report_payload = _read_json(report)
    return {
        "schema_version": "stock_universe.pressure_run_manifest.v1",
        "generated_at_utc": generated_at_utc or dt.datetime.now(dt.UTC).isoformat(),
        "repo": {
            "root": str(root),
            "git_head": git_head
            if git_head is not None
            else _git_output(root, "rev-parse", "HEAD"),
            "git_dirty": git_dirty
            if git_dirty is not None
            else bool(_git_output(root, "status", "--short")),
        },
        "cohort": cohort,
        "cohort_plan": pressure_cohort_plan(cohort),
        "command": command if isinstance(command, str) else list(command),
        "artifacts": {
            "report_path": str(report),
            "report_sha256": _sha256_file(report),
            "db_path": str(db_path)
            if db_path is not None
            else str(report_payload.get("db") or ""),
        },
        "summary": _report_summary(report_payload),
        "request_efficiency": _request_efficiency(report_payload),
        "validation": report_payload.get("validation") or {},
    }


def pressure_cohort_plan(cohort: str) -> dict[str, Any]:
    """Return machine-readable plan metadata for a known pressure cohort."""
    normalized = str(cohort).strip()
    for plan in PRESSURE_COHORT_PLANS:
        if plan.name == normalized:
            return plan.to_payload()
    return {
        "known": False,
        "name": normalized,
        "target_size": None,
        "description": "Custom pressure cohort.",
        "selection": [],
        "success_factors": (
            [
                "attempted equals expected cohort size",
                "ok_count equals attempted",
                "skipped_count equals 0",
                "error_count equals 0",
                "SQLite validation is clean",
                "audit rows exist for every receipt",
            ]
        ),
        "next_cohort": "",
    }


def list_pressure_cohort_plans() -> list[dict[str, Any]]:
    """Return known pressure cohort plans in recommended execution order."""
    return [plan.to_payload() for plan in PRESSURE_COHORT_PLANS]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _report_summary(report: dict[str, Any]) -> dict[str, Any]:
    results = report.get("results") or []
    status_counts = Counter(
        str(item.get("status") or "unknown")
        for item in results
        if isinstance(item, dict)
    )
    summary = {
        "attempted": _int_value(report.get("attempted"), default=len(results)),
        "ok_count": _int_value(
            report.get("ok_count"), default=status_counts.get("ok", 0)
        ),
        "skipped_count": _int_value(
            report.get("skipped_count"), default=status_counts.get("skipped", 0)
        ),
        "error_count": _int_value(
            report.get("error_count"), default=status_counts.get("error", 0)
        ),
        "status_counts": dict(sorted(status_counts.items())),
        "counts": report.get("counts") or {},
    }
    for key in ("plans", "evidence_facts", "execution_receipts", "bars_inserted"):
        if key in report:
            summary[key] = report[key]
    return summary


def _request_efficiency(report: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for index, item in enumerate(report.get("results") or (), 1):
        if not isinstance(item, dict):
            continue
        observed = _first_int(item, "request_count", "planning_request_count")
        if observed is None:
            continue
        rows.append(
            {
                "index": index,
                "input": _result_label(item, index),
                "status": str(item.get("status") or "unknown"),
                "observed_request_count": observed,
                "planning_request_count": _optional_int(
                    item.get("planning_request_count")
                ),
                "receipt_request_count": _optional_int(item.get("request_count")),
                "ohlcv_series_id": item.get("ohlcv_series_id", item.get("series_id")),
                "latest_ticker": item.get("latest_ticker") or item.get("ticker") or "",
            }
        )
    if not rows:
        return {
            "instrumented_result_count": 0,
            "total_observed_request_count": 0,
            "max_observed_request_count": 0,
            "average_observed_request_count": 0,
            "top_observed_request_counts": [],
        }
    counts = [int(row["observed_request_count"]) for row in rows]
    return {
        "instrumented_result_count": len(rows),
        "total_observed_request_count": sum(counts),
        "max_observed_request_count": max(counts),
        "average_observed_request_count": round(sum(counts) / len(counts), 2),
        "top_observed_request_counts": sorted(
            rows,
            key=lambda row: (-int(row["observed_request_count"]), row["input"]),
        )[:10],
    }


def _first_int(item: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _optional_int(item.get(key))
        if value is not None:
            return value
    return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _result_label(item: dict[str, Any], index: int) -> str:
    for key in ("input", "ticker", "latest_ticker"):
        if item.get(key):
            return str(item[key])
    ohlcv_series_id = item.get("ohlcv_series_id", item.get("series_id"))
    if ohlcv_series_id is not None:
        return f"ohlcv_series:{ohlcv_series_id}"
    return f"result:{index}"


def _int_value(value: Any, *, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_output(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()
