"""Small executable-context result envelope helpers."""

from __future__ import annotations

from typing import Any

from stock_universe.domain import BackfillPlan, EvidenceNeeded
from stock_universe.xctx.protocol import (
    PROTOCOL_VERSION,
    ActionKind,
    CommandSpec,
    EffectSpec,
    InvalidAction,
    NextAction,
    RepairAction,
)


def result_envelope(
    command: str,
    result: BackfillPlan | EvidenceNeeded,
    *,
    execution_approved: bool = False,
    approval_argv: list[str] | tuple[str, ...] | None = None,
    approval_source_checkout_argv: list[str] | tuple[str, ...] | None = None,
    execution_argv: list[str] | tuple[str, ...] | None = None,
    execution_source_checkout_argv: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    if isinstance(result, BackfillPlan):
        envelope = {
            "protocol_version": PROTOCOL_VERSION,
            "ok": result.status != "blocked",
            "command": command,
            "result_type": "BackfillPlan",
            "status": result.status,
            "evidence_ledger_hash": result.evidence_ledger_hash,
            "decisions": [decision.to_legacy_dict() for decision in result.decisions],
            "next_actions": [
                action.to_dict()
                for action in _plan_next_actions(
                    result,
                    execution_approved=execution_approved,
                    approval_argv=tuple(approval_argv or ()),
                    approval_source_checkout_argv=tuple(
                        approval_source_checkout_argv or approval_argv or ()
                    ),
                    execution_argv=tuple(execution_argv or ()),
                    execution_source_checkout_argv=tuple(
                        execution_source_checkout_argv or execution_argv or ()
                    ),
                )
            ],
        }
        invalid_actions = _plan_invalid_next_actions(
            result, execution_approved=execution_approved
        )
        if invalid_actions:
            envelope["invalid_next_actions"] = [
                action.to_dict() for action in invalid_actions
            ]
        return envelope
    envelope = {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": command,
        "result_type": "EvidenceNeeded",
        "requests": [request.to_legacy_dict() for request in result.requests],
        "decisions": [decision.to_legacy_dict() for decision in result.decisions],
        "next_actions": [
            _next_action(
                "collect-missing-evidence",
                "command",
                "xctx dry-run",
                "Collect additional requested evidence and rerun planning.",
                effects=(
                    EffectSpec(
                        "read",
                        "evidence-source",
                        "Read provider or repository evidence.",
                    ),
                ),
            ).to_dict()
        ],
        "invalid_next_actions": [
            _invalid_action(
                "execute-approved-plan", "planner has not emitted a BackfillPlan"
            ).to_dict(),
            _invalid_action(
                "write-final-plan", "required evidence is still missing"
            ).to_dict(),
        ],
    }
    repairs = _evidence_repairs(result)
    if repairs:
        envelope["repairs"] = [repair.to_dict() for repair in repairs]
        envelope["next_actions"] = envelope["next_actions"] + [
            _next_action(
                "apply-repair",
                "repair",
                "xctx repair",
                "Emit a repair action payload for missing evidence.",
                effects=(
                    EffectSpec(
                        "none", "stdout", "Describe a repair; no mutation is performed."
                    ),
                ),
            ).to_dict()
        ]
    return envelope


def _plan_next_actions(
    plan: BackfillPlan,
    *,
    execution_approved: bool,
    approval_argv: tuple[str, ...] = (),
    approval_source_checkout_argv: tuple[str, ...] = (),
    execution_argv: tuple[str, ...] = (),
    execution_source_checkout_argv: tuple[str, ...] = (),
) -> list[NextAction]:
    if plan.status == "blocked":
        return [
            _next_action(
                "inspect-decision",
                "inspection",
                "xctx next",
                "Inspect blocking planner decisions.",
                effects=(EffectSpec("none", "stdout", "Render decision context."),),
            ),
            _next_action(
                "collect-missing-evidence",
                "command",
                "xctx dry-run",
                "Collect additional requested evidence and rerun planning.",
                effects=(
                    EffectSpec(
                        "read",
                        "evidence-source",
                        "Read provider or repository evidence.",
                    ),
                ),
            ),
        ]
    if plan.status == "caution":
        if execution_approved:
            return [
                _next_action(
                    "inspect-warnings",
                    "inspection",
                    "xctx next",
                    "Inspect warning decisions before execution.",
                    effects=(EffectSpec("none", "stdout", "Render warning context."),),
                ),
                _execute_action(
                    argv=execution_argv,
                    source_checkout_argv=execution_source_checkout_argv,
                ),
            ]
        return [
            _next_action(
                "inspect-warnings",
                "inspection",
                "xctx next",
                "Inspect warning decisions before approval.",
                effects=(EffectSpec("none", "stdout", "Render warning context."),),
            ),
            _next_action(
                "approve-provisional-backfill",
                "approval",
                "xctx validate",
                "Approve a caution plan before execution can be offered.",
                requires_approval=True,
                effects=(
                    EffectSpec(
                        "none",
                        "approval-record",
                        "No approval is persisted by xctx yet.",
                    ),
                ),
                command_reads=("fixture",),
                argv=approval_argv,
                source_checkout_argv=approval_source_checkout_argv,
            ),
        ]
    if execution_approved:
        return [
            _next_action(
                "review-plan",
                "inspection",
                "xctx next",
                "Review the final backfill plan.",
                effects=(EffectSpec("none", "stdout", "Render plan context."),),
            ),
            _execute_action(
                argv=execution_argv, source_checkout_argv=execution_source_checkout_argv
            ),
        ]
    return [
        _next_action(
            "review-plan",
            "inspection",
            "xctx next",
            "Review the final backfill plan.",
            effects=(EffectSpec("none", "stdout", "Render plan context."),),
        ),
        _next_action(
            "approve-plan",
            "approval",
            "xctx validate",
            "Approve a safe plan before execution can be offered.",
            requires_approval=True,
            effects=(
                EffectSpec(
                    "none", "approval-record", "No approval is persisted by xctx yet."
                ),
            ),
            command_reads=("fixture",),
            argv=approval_argv,
            source_checkout_argv=approval_source_checkout_argv,
        ),
    ]


def _plan_invalid_next_actions(
    plan: BackfillPlan, *, execution_approved: bool
) -> list[InvalidAction]:
    if plan.status == "blocked":
        return [
            _invalid_action("execute-approved-plan", "blocked plans cannot execute")
        ]
    if execution_approved:
        return []
    return [
        _invalid_action(
            "execute-approved-plan",
            "plan review and explicit execution approval are required first",
        )
    ]


def _evidence_repairs(result: EvidenceNeeded) -> list[RepairAction]:
    repairs: list[RepairAction] = []
    for request in result.requests:
        if request.kind == "alias_history":
            repairs.append(
                _repair_action(
                    "provide-alias-history",
                    "alias_history",
                    request.to_legacy_dict(),
                    (
                        "No live alias-history collector is available; provide an explicit AliasHistoryFact "
                        "covering the requested pre-event interval."
                    ),
                )
            )
        elif request.kind == "coverage_gap":
            repairs.append(
                _repair_action(
                    "resolve-coverage-gap",
                    "coverage_gap",
                    request.to_legacy_dict(),
                    (
                        "Append ticker-replacement, handoff, omitted-segment, or terminal-coverage evidence "
                        "that fully accounts for the requested uncovered interval."
                    ),
                )
            )
        elif request.kind == "terminal_coverage":
            repairs.append(
                _repair_action(
                    "provide-terminal-coverage",
                    "terminal_coverage",
                    request.to_legacy_dict(),
                    (
                        "Append an explicit TerminalCoverageFact proving the final transformed ticker "
                        "validly covers the requested terminal interval."
                    ),
                )
            )
    return repairs


def _next_action(
    name: str,
    kind: ActionKind,
    command_name: str,
    description: str,
    *,
    effects: tuple[EffectSpec, ...],
    requires_approval: bool = False,
    command_reads: tuple[str, ...] = (),
    command_writes: tuple[str, ...] = (),
    argv: tuple[str, ...] = (),
    source_checkout_argv: tuple[str, ...] = (),
) -> NextAction:
    return NextAction(
        name=name,
        kind=kind,
        command=CommandSpec(
            command_name, description, reads=command_reads, writes=command_writes
        ),
        effects=effects,
        requires_approval=requires_approval,
        argv=argv,
        source_checkout_argv=source_checkout_argv,
    )


def _execute_action(
    *, argv: tuple[str, ...] = (), source_checkout_argv: tuple[str, ...] = ()
) -> NextAction:
    return _next_action(
        "execute-approved-plan",
        "execution",
        "stock-universe backfill",
        "Execute an approved BackfillPlan through the production CLI.",
        effects=(
            EffectSpec(
                "read", "Massive API", "Collect live bar data for the approved plan."
            ),
            EffectSpec(
                "write", "SQLite DB", "Persist plans, approvals, bars, and receipts."
            ),
        ),
        requires_approval=True,
        command_reads=("Massive API",),
        command_writes=("SQLite DB",),
        argv=argv,
        source_checkout_argv=source_checkout_argv,
    )


def _invalid_action(name: str, reason: str) -> InvalidAction:
    return InvalidAction(
        name=name,
        command=CommandSpec(
            name="stock-universe backfill",
            description="Rejected execution action.",
            reads=("Massive API",),
            writes=("SQLite DB",),
        ),
        reason=reason,
    )


def _repair_action(
    name: str, evidence_kind: str, request: dict[str, Any], reason: str
) -> RepairAction:
    return RepairAction(
        name=name,
        evidence_kind=evidence_kind,
        request=request,
        effect=EffectSpec(
            "append-evidence-fact",
            evidence_kind,
            "Append a typed evidence fact outside xctx; xctx only emits the protocol payload.",
        ),
        reason=reason,
        command=CommandSpec(
            "xctx repair", "Emit repair guidance for unresolved evidence."
        ),
    )
