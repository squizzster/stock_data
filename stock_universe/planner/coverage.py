"""Coverage-accounting stage helpers for the pure planner."""

from __future__ import annotations

import datetime as dt

from stock_universe.domain import EvidenceSnapshot, PlannedSegment
from stock_universe.domain.records import parse_date
from stock_universe.market_calendar import (
    first_us_equity_trading_date_on_or_after,
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
)
from stock_universe.planner.segments_utils import _reindex_segments


def _drop_omitted_segments(
    evidence: EvidenceSnapshot, segments: list[PlannedSegment]
) -> list[PlannedSegment]:
    omitted = [fact.payload_value() for fact in evidence.get_all("omitted_segment")]
    kept: list[PlannedSegment] = []
    for segment in segments:
        should_drop = (
            any(
                row.get("ticker") == segment.ticker
                and parse_date(row["from_date"]) <= segment.from_date
                and parse_date(row["to_date"]) >= segment.to_date
                for row in omitted
            )
            and not segment.validation
        )
        if not should_drop:
            kept.append(segment)
    return _reindex_segments(kept)


def _coverage_gap_reason(
    kind: str, ticker: str, from_date: dt.date, to_date: dt.date
) -> str:
    if kind == "terminal_coverage":
        return (
            f"Replacement or handoff evidence ended before the request end; terminal valid-window evidence is "
            f"required for {ticker} {from_date.isoformat()} to {to_date.isoformat()}."
        )
    return (
        f"Replacement or handoff evidence left {ticker} {from_date.isoformat()} "
        f"to {to_date.isoformat()} uncovered."
    )


def _uncovered_ranges(
    evidence: EvidenceSnapshot,
    original_segments: list[PlannedSegment],
    transformed_segments: list[PlannedSegment],
) -> tuple[tuple[str, str, dt.date, dt.date], ...]:
    gaps: list[tuple[str, str, dt.date, dt.date]] = []
    ordered_originals = sorted(
        original_segments, key=lambda item: (item.from_date, item.to_date, item.ticker)
    )
    for index, original in enumerate(ordered_originals):
        next_original = (
            ordered_originals[index + 1] if index + 1 < len(ordered_originals) else None
        )
        covered = sorted(
            (
                max(original.from_date, segment.from_date),
                min(original.to_date, segment.to_date),
            )
            for segment in transformed_segments
            if segment.from_date <= original.to_date
            and segment.to_date >= original.from_date
        )
        cursor = _first_session_on_or_after(original.from_date)
        if cursor > original.to_date:
            continue
        for start, end in covered:
            if end < cursor:
                continue
            if start > cursor:
                gap_end = _previous_session_before(start)
                if cursor <= gap_end and not _has_omitted_coverage(
                    evidence, original.ticker, cursor, gap_end
                ):
                    gaps.append(("coverage_gap", original.ticker, cursor, gap_end))
            cursor = max(cursor, _next_session_after(end))
            if cursor > original.to_date:
                break
        if cursor <= original.to_date:
            if next_original is not None:
                if not _has_omitted_coverage(
                    evidence, original.ticker, cursor, original.to_date
                ):
                    gaps.append(
                        ("coverage_gap", original.ticker, cursor, original.to_date)
                    )
            elif not _has_terminal_coverage(
                evidence, original.ticker, cursor, original.to_date
            ):
                if not _has_omitted_coverage(
                    evidence, original.ticker, cursor, original.to_date
                ):
                    gaps.append(
                        ("terminal_coverage", original.ticker, cursor, original.to_date)
                    )
    return tuple(gaps)


def _first_session_on_or_after(value: dt.date) -> dt.date:
    return parse_date(first_us_equity_trading_date_on_or_after(value))


def _next_session_after(value: dt.date) -> dt.date:
    return parse_date(next_us_equity_trading_date(value))


def _previous_session_before(value: dt.date) -> dt.date:
    return parse_date(previous_us_equity_trading_date(value))


def _has_omitted_coverage(
    evidence: EvidenceSnapshot, ticker: str, from_date: dt.date, to_date: dt.date
) -> bool:
    for fact in evidence.get_all("omitted_segment"):
        payload = fact.payload_value()
        if (
            payload.get("ticker") == ticker
            and parse_date(payload["from_date"]) <= from_date
            and parse_date(payload["to_date"]) >= to_date
        ):
            return True
    return False


def _has_terminal_coverage(
    evidence: EvidenceSnapshot, ticker: str, from_date: dt.date, to_date: dt.date
) -> bool:
    for fact in evidence.get_all("terminal_coverage"):
        payload = fact.payload_value()
        if (
            payload.get("ticker") == ticker
            and parse_date(payload["from_date"]) <= from_date
            and parse_date(payload["to_date"]) >= to_date
        ):
            return True
    return False
