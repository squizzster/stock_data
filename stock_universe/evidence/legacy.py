"""Adapters from legacy plan JSON into typed evidence facts."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from stock_universe.domain import (
    AliasHistoryFact,
    EvidenceFact,
    EvidenceLedger,
    HandoffSegmentFact,
    OmittedSegmentFact,
    ReferenceBoundaryFact,
    TerminalCoverageFact,
    TickerEventFact,
    TickerReplacementFact,
)
from stock_universe.domain.records import parse_date

OMITTED_SEGMENT_RE = re.compile(
    r"^Segment \d+ (?P<ticker>[A-Z0-9.\-]+) (?P<from_date>\d{4}-\d{2}-\d{2}) "
    r"to (?P<to_date>\d{4}-\d{2}-\d{2}) was omitted: (?P<reason>.+)$"
)


def facts_from_legacy_plan(
    plan: dict[str, Any],
    source: str = "legacy_plan_json",
    *,
    include_candidate_segments: bool = True,
) -> tuple[EvidenceFact, ...]:
    """Capture a legacy plan as evidence for parity tests.

    This intentionally treats old warnings/errors as evidence-backed rule
    decisions. Future planner rules can replace those compatibility decisions
    one by one while the rendered legacy output remains stable.
    """
    target = dict(plan["target"])
    series_id = str(target["ohlcv_series_id"])
    facts: list[EvidenceFact] = [
        EvidenceFact("backfill_request", (series_id,), plan["range"], source),
        EvidenceFact("target_identity", (series_id,), target, source),
        EvidenceFact(
            "known_aliases", (series_id,), plan.get("known_aliases_from_db", []), source
        ),
        EvidenceFact(
            "plan_metadata",
            (series_id,),
            {
                "generated_at_utc": plan.get("generated_at_utc", ""),
                "raw_dir": plan.get("raw_dir", ""),
                "api_requests": plan.get("api_requests", 0),
                "identity_discovery": plan.get("identity_discovery", {}),
                "plan_files": plan.get("plan_files", {}),
            },
            source,
        ),
    ]
    if include_candidate_segments:
        facts.append(
            EvidenceFact(
                "candidate_segments", (series_id,), plan.get("segments", []), source
            )
        )
    event_lookup = plan.get("event_lookup", {})
    facts.append(EvidenceFact("event_lookup", (series_id,), event_lookup, source))
    if event_lookup:
        facts.append(
            TickerEventFact.from_legacy_event_lookup(
                event_lookup, source
            ).to_evidence_fact(series_id)
        )
    alias_history = AliasHistoryFact.from_legacy_segments(
        plan.get("segments", []), source
    )
    if alias_history.to_legacy_dict()["spans"]:
        facts.append(alias_history.to_evidence_fact(series_id))
    for segment in plan.get("segments", []):
        if segment.get("ticker_replacement"):
            facts.append(
                TickerReplacementFact.from_legacy_segment(
                    segment, source
                ).to_evidence_fact(series_id)
            )
        if segment.get("event_ticker_handoff"):
            facts.append(
                HandoffSegmentFact.from_legacy_segment(
                    segment, source
                ).to_evidence_fact(series_id)
            )
        for row in segment.get("validation") or []:
            if row.get("date") and row.get("requested_ticker"):
                facts.append(
                    ReferenceBoundaryFact(
                        ticker=row["requested_ticker"],
                        as_of_date=row["date"],
                        api_status=str(row.get("api_status") or ""),
                        matched=bool(row.get("matched", False)),
                        match_reason=str(row.get("match_reason") or ""),
                        payload=row,
                        source=source,
                    ).to_evidence_fact(series_id)
                )
    terminal_fact = _terminal_coverage_from_legacy_plan(plan, source)
    if terminal_fact:
        facts.append(terminal_fact.to_evidence_fact(series_id))

    for index, warning in enumerate(plan.get("warnings", []), 1):
        omitted = OMITTED_SEGMENT_RE.match(str(warning))
        if omitted:
            facts.append(
                OmittedSegmentFact(
                    ticker=omitted.group("ticker"),
                    from_date=omitted.group("from_date"),
                    to_date=omitted.group("to_date"),
                    reason=omitted.group("reason"),
                    source=source,
                ).to_evidence_fact(series_id)
            )
        facts.append(
            EvidenceFact(
                "legacy_decision",
                (series_id, "warning", str(index)),
                {
                    "rule_name": "legacy.warning",
                    "outcome": "warn",
                    "reason": warning,
                },
                source,
            )
        )
    for index, error in enumerate(plan.get("errors", []), 1):
        facts.append(
            EvidenceFact(
                "legacy_decision",
                (series_id, "error", str(index)),
                {
                    "rule_name": "legacy.error",
                    "outcome": "block",
                    "reason": error,
                },
                source,
            )
        )

    return tuple(facts)


def _terminal_coverage_from_legacy_plan(
    plan: dict[str, Any], source: str
) -> TerminalCoverageFact | None:
    segments = plan.get("segments") or []
    events = (plan.get("event_lookup") or {}).get("events") or []
    if not segments or not events:
        return None
    request_to_date = parse_date(plan["range"]["to_date"])
    last_segment_to_date = max(parse_date(segment["to_date"]) for segment in segments)
    if last_segment_to_date >= request_to_date:
        return None
    active_events = [
        event
        for event in events
        if event.get("date")
        and event.get("ticker")
        and parse_date(event["date"]) <= request_to_date
    ]
    if not active_events:
        return None
    active_events.sort(key=lambda item: parse_date(item["date"]))
    ticker = str(active_events[-1]["ticker"])
    from_date = last_segment_to_date + dt.timedelta(days=1)
    reason = "legacy plan ended before requested range; terminal valid-window evidence accounts for the tail."
    return TerminalCoverageFact(
        ticker=ticker,
        from_date=from_date,
        to_date=request_to_date,
        reason=reason,
        source=source,
    )


def ledger_from_legacy_plan(
    plan: dict[str, Any], source: str = "legacy_plan_json"
) -> EvidenceLedger:
    return EvidenceLedger(facts_from_legacy_plan(plan, source))
