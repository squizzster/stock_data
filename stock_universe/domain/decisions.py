"""Planner decision records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .common import DecisionOutcome


@dataclass(frozen=True)
class RuleDecision:
    rule_name: str
    outcome: DecisionOutcome
    segment_id: str | None
    reason: str
    evidence_refs: tuple[str, ...] = ()
    decision_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evidence_refs", tuple(str(item) for item in self.evidence_refs)
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "rule_name": self.rule_name,
            "outcome": self.outcome,
            "segment_id": self.segment_id,
            "reason": self.reason,
            "evidence_refs": list(self.evidence_refs),
        }
