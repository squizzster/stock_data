"""Segment utility helpers for planner stages."""

from __future__ import annotations

from stock_universe.domain import PlannedSegment


def _reindex_segments(segments: list[PlannedSegment]) -> list[PlannedSegment]:
    return [
        PlannedSegment(
            segment_index=index,
            ticker=segment.ticker,
            from_date=segment.from_date,
            to_date=segment.to_date,
            source=segment.source,
            valid=segment.valid,
            validation=segment.validation,
            event_date=segment.event_date,
            request_symbol=segment.request_symbol,
            extra=segment.extra,
        )
        for index, segment in enumerate(segments, 1)
    ]
