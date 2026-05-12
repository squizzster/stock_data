"""Executor contract validation.

This is not the live bar downloader. It is the pre-effect contract that a real
executor must satisfy before it is allowed to fetch or write anything.
"""

from __future__ import annotations

from dataclasses import dataclass

from stock_universe.domain import BackfillPlan, PlannedSegment


@dataclass(frozen=True)
class ExecutionApproval:
    request_hash: str
    approved_by: str = ""
    allow_caution: bool = False


@dataclass(frozen=True)
class ExecutionContractReport:
    ok: bool
    checks: tuple[str, ...]


class ExecutionContractError(ValueError):
    def __init__(self, checks: tuple[str, ...]):
        super().__init__("Backfill plan failed executor contract: " + "; ".join(checks))
        self.checks = checks


def validate_approved_plan(
    plan: BackfillPlan, approval: ExecutionApproval
) -> ExecutionContractReport:
    failures: list[str] = []

    if plan.status == "blocked":
        failures.append("blocked plans cannot execute")
    if plan.status == "caution" and not approval.allow_caution:
        failures.append("caution plans require explicit caution approval")
    if approval.request_hash != plan.request.request_hash:
        failures.append("approval request hash does not match plan request")
    if not plan.evidence_ledger_hash or plan.evidence_ledger_hash == "unknown":
        failures.append("plan must carry a non-empty evidence ledger hash")

    failures.extend(_segment_contract_failures(plan))

    if failures:
        raise ExecutionContractError(tuple(failures))
    return ExecutionContractReport(
        ok=True,
        checks=(
            "status executable",
            "request hash matched",
            "evidence ledger hash present",
            "segments ordered",
            "segments non-overlapping",
            "segments inside request bounds",
            "segments valid",
        ),
    )


def _segment_contract_failures(plan: BackfillPlan) -> list[str]:
    failures: list[str] = []
    previous: PlannedSegment | None = None
    for expected_index, segment in enumerate(plan.segments, 1):
        if segment.segment_index != expected_index:
            failures.append(
                f"segment {segment.segment_index} is not in sequential order"
            )
        if segment.from_date > segment.to_date:
            failures.append(f"segment {segment.segment_index} starts after it ends")
        if (
            segment.from_date < plan.request.from_date
            or segment.to_date > plan.request.to_date
        ):
            failures.append(
                f"segment {segment.segment_index} falls outside request bounds"
            )
        if previous and segment.from_date <= previous.to_date:
            failures.append(
                f"segment {segment.segment_index} overlaps segment {previous.segment_index}"
            )
        if not segment.valid:
            failures.append(f"segment {segment.segment_index} is marked invalid")
        previous = segment
    if not plan.segments:
        failures.append("plan has no segments")
    return failures
