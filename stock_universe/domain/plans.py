"""Backfill plan result records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common import PlanStatus, freeze_json, unfreeze_json
from .decisions import RuleDecision
from .evidence import EvidenceRequest
from .identity import KnownAlias, TargetIdentity
from .requests import BackfillRequest
from .segments import PlannedSegment


@dataclass(frozen=True)
class BackfillPlan:
    request: BackfillRequest
    status: PlanStatus
    target: TargetIdentity
    segments: tuple[PlannedSegment, ...]
    decisions: tuple[RuleDecision, ...]
    evidence_ledger_hash: str
    planner_version: str
    created_at_utc: str
    event_lookup: Any = field(default_factory=tuple)
    known_aliases: tuple[KnownAlias, ...] = ()
    raw_dir: str = ""
    api_requests: int = 0
    identity_discovery: Any = field(default_factory=tuple)
    plan_files: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "segments", tuple(self.segments))
        object.__setattr__(self, "decisions", tuple(self.decisions))
        object.__setattr__(self, "event_lookup", freeze_json(self.event_lookup))
        object.__setattr__(self, "known_aliases", tuple(self.known_aliases))
        object.__setattr__(
            self, "identity_discovery", freeze_json(self.identity_discovery)
        )
        object.__setattr__(self, "plan_files", freeze_json(self.plan_files))

    @property
    def warnings(self) -> tuple[str, ...]:
        return tuple(
            decision.reason for decision in self.decisions if decision.outcome == "warn"
        )

    @property
    def errors(self) -> tuple[str, ...]:
        return tuple(
            decision.reason
            for decision in self.decisions
            if decision.outcome == "block"
        )

    def to_payload(self) -> dict[str, Any]:
        result = {
            "api_requests": self.api_requests,
            "errors": list(self.errors),
            "event_lookup": unfreeze_json(self.event_lookup),
            "generated_at_utc": self.created_at_utc,
            "identity_discovery": unfreeze_json(self.identity_discovery),
            "known_aliases_from_db": [
                alias.to_payload() for alias in self.known_aliases
            ],
            "range": self.request.to_payload(),
            "raw_dir": self.raw_dir,
            "segments": [segment.to_payload() for segment in self.segments],
            "status": self.status,
            "target": self.target.to_payload(),
            "warnings": list(self.warnings),
        }
        plan_files = unfreeze_json(self.plan_files)
        if plan_files:
            result["plan_files"] = plan_files
        return result


@dataclass(frozen=True)
class EvidenceNeeded:
    requests: tuple[EvidenceRequest, ...]
    decisions: tuple[RuleDecision, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "requests", tuple(self.requests))
        object.__setattr__(self, "decisions", tuple(self.decisions))
