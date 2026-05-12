"""Required-evidence checks for the pure backfill planner."""

from __future__ import annotations

from stock_universe.domain import EvidenceSnapshot


def _missing_required_evidence(evidence: EvidenceSnapshot) -> tuple[str, ...]:
    required = ("backfill_request", "target_identity", "known_aliases", "plan_metadata")
    missing = [kind for kind in required if evidence.get_one(kind) is None]
    if (
        evidence.get_one("candidate_segments") is None
        and evidence.get_one("ticker_events") is None
    ):
        missing.append("ticker_events")
    return tuple(missing)
