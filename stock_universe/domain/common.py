"""Common helpers and type aliases for immutable domain records."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Literal

DecisionOutcome = Literal["accept", "warn", "block", "skip", "needs_evidence"]
PlanStatus = Literal["safe", "caution", "blocked"]
Timespan = Literal["day", "minute"]
BarGrain = Literal["1d", "1m", "30m"]
DEFAULT_BAR_GRAIN: BarGrain = "1d"


@dataclass(frozen=True)
class BarGrainSpec:
    bar_grain: BarGrain
    multiplier: int
    timespan: Timespan


_BAR_GRAIN_ALIASES: dict[str, BarGrain] = {
    "1d": "1d",
    "1day": "1d",
    "day": "1d",
    "daily": "1d",
    "1/day": "1d",
    "1m": "1m",
    "1min": "1m",
    "1minute": "1m",
    "minute": "1m",
    "1/minute": "1m",
    "30m": "30m",
    "30min": "30m",
    "30minute": "30m",
    "30/minute": "30m",
}

_BAR_GRAIN_SPECS: dict[BarGrain, tuple[int, Timespan]] = {
    "1d": (1, "day"),
    "1m": (1, "minute"),
    "30m": (30, "minute"),
}


def normalize_bar_grain(
    bar_grain: str | None = None,
    *,
    multiplier: int | None = None,
    timespan: str | None = None,
) -> BarGrainSpec:
    """Return the canonical storage/API representation for a supported bar grain."""
    if bar_grain:
        normalized = _BAR_GRAIN_ALIASES.get(str(bar_grain).strip().lower())
        if normalized is None:
            raise ValueError("bar_grain must be one of: 1d, 1m, 30m")
        spec_multiplier, spec_timespan = _BAR_GRAIN_SPECS[normalized]
        if multiplier is not None and int(multiplier) != spec_multiplier:
            raise ValueError("bar_grain conflicts with multiplier")
        if timespan is not None and _normalized_timespan(timespan) != spec_timespan:
            raise ValueError("bar_grain conflicts with timespan")
        return BarGrainSpec(normalized, spec_multiplier, spec_timespan)

    if multiplier is None and timespan is None:
        multiplier, timespan = _BAR_GRAIN_SPECS[DEFAULT_BAR_GRAIN]
    if multiplier is None or timespan is None:
        raise ValueError("multiplier and timespan must be provided together")
    resolved_multiplier = int(multiplier)
    resolved_timespan = _normalized_timespan(timespan)
    for candidate, (
        candidate_multiplier,
        candidate_timespan,
    ) in _BAR_GRAIN_SPECS.items():
        if (
            resolved_multiplier == candidate_multiplier
            and resolved_timespan == candidate_timespan
        ):
            return BarGrainSpec(candidate, resolved_multiplier, resolved_timespan)
    raise ValueError("supported bar grains are 1d, 1m, and 30m")


def bar_grain_from_parts(multiplier: int, timespan: str) -> BarGrain:
    return normalize_bar_grain(multiplier=multiplier, timespan=timespan).bar_grain


def _normalized_timespan(timespan: str) -> Timespan:
    normalized = str(timespan).strip().lower()
    if normalized in {"day", "daily"}:
        return "day"
    if normalized in {"minute", "min", "m"}:
        return "minute"
    raise ValueError("timespan must be 'day' or 'minute'")


def parse_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value)


def date_text(value: str | dt.date | None) -> str | None:
    if value is None:
        return None
    return parse_date(value).isoformat()


def date_range_days(from_date: str | dt.date, to_date: str | dt.date) -> int:
    return (parse_date(to_date) - parse_date(from_date)).days + 1


def freeze_json(value: Any) -> Any:
    """Return a tuple-backed immutable representation of JSON-like data."""
    if isinstance(value, dict):
        return tuple(
            (str(key), freeze_json(item)) for key, item in sorted(value.items())
        )
    if isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    return value


def unfreeze_json(value: Any) -> Any:
    if isinstance(value, tuple):
        if all(
            isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
            for item in value
        ):
            return {key: unfreeze_json(item) for key, item in value}
        return [unfreeze_json(item) for item in value]
    return value


def stable_json_hash(value: Any) -> str:
    encoded = json.dumps(
        unfreeze_json(freeze_json(value)), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
