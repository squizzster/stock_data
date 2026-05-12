"""Segment transform helpers for ticker replacements and handoffs."""

from __future__ import annotations

from typing import Any

from stock_universe.domain import EvidenceSnapshot, PlannedSegment
from stock_universe.domain.records import parse_date
from stock_universe.planner.segments_utils import _reindex_segments


def _apply_ticker_replacements(
    evidence: EvidenceSnapshot, segments: list[PlannedSegment]
) -> list[PlannedSegment]:
    replacements = [
        fact.payload_value() for fact in evidence.get_all("ticker_replacement")
    ]
    if not replacements:
        return segments

    adjusted: list[PlannedSegment] = []
    for segment in segments:
        segment_replacements = _matching_ticker_replacements(segment, replacements)
        if not segment_replacements:
            adjusted.append(segment)
            continue
        for replacement in segment_replacements:
            metadata = {
                key: value
                for key, value in replacement.items()
                if key
                not in {
                    "event_date",
                    "from_date",
                    "new_ticker",
                    "old_ticker",
                    "replacement_reason",
                    "source",
                    "to_date",
                    "validation",
                }
            }
            adjusted.append(
                PlannedSegment(
                    segment_index=segment.segment_index,
                    ticker=str(replacement["new_ticker"]),
                    from_date=max(
                        segment.from_date, parse_date(replacement["from_date"])
                    ),
                    to_date=min(segment.to_date, parse_date(replacement["to_date"])),
                    source=str(
                        replacement.get("source")
                        or f"{segment.source}+{replacement.get('replacement_reason') or 'ticker_replacement'}"
                    ),
                    valid=segment.valid,
                    validation=replacement.get("validation")
                    or segment.to_legacy_dict().get("validation", ()),
                    event_date=segment.event_date,
                    request_symbol=str(replacement["new_ticker"]),
                    extra=metadata,
                )
            )
    return _reindex_segments(
        [segment for segment in adjusted if segment.from_date <= segment.to_date]
    )


def _matching_ticker_replacements(
    segment: PlannedSegment,
    replacements: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches = [
        replacement
        for replacement in replacements
        if replacement.get("old_ticker") == segment.ticker
        and parse_date(replacement["from_date"]) <= segment.to_date
        and parse_date(replacement["to_date"]) >= segment.from_date
    ]
    return sorted(
        matches,
        key=lambda item: (item["from_date"], item["to_date"], item["new_ticker"]),
    )


def _apply_handoff_segments(
    evidence: EvidenceSnapshot, segments: list[PlannedSegment]
) -> list[PlannedSegment]:
    handoffs = [fact.payload_value() for fact in evidence.get_all("handoff_segment")]
    if not handoffs:
        return segments

    adjusted = list(segments)
    for handoff in handoffs:
        from_date = parse_date(handoff["from_date"])
        to_date = parse_date(handoff["to_date"])
        ticker = str(handoff["ticker"])
        already_present = any(
            segment.ticker == ticker
            and segment.from_date == from_date
            and segment.to_date == to_date
            for segment in adjusted
        )
        if already_present:
            continue
        metadata = {
            key: value
            for key, value in handoff.items()
            if key
            not in {
                "event_date",
                "event_ticker_handoff",
                "from_date",
                "source",
                "ticker",
                "to_date",
                "validation",
            }
        }
        metadata["event_ticker_handoff"] = handoff.get("event_ticker_handoff") or {}
        adjusted.append(
            PlannedSegment(
                segment_index=len(adjusted) + 1,
                ticker=ticker,
                from_date=from_date,
                to_date=to_date,
                source=str(handoff.get("source") or "event_ticker_handoff"),
                valid=True,
                validation=handoff.get("validation") or (),
                event_date=handoff.get("event_date"),
                extra=metadata,
            )
        )
    return _reindex_segments(
        sorted(adjusted, key=lambda item: (item.from_date, item.to_date, item.ticker))
    )
