"""Backfill request records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .common import (
    Timespan,
    date_range_days,
    normalize_bar_grain,
    parse_date,
    stable_json_hash,
)


@dataclass(frozen=True)
class BackfillRequest:
    series_id: int
    from_date: dt.date
    to_date: dt.date
    multiplier: int = 1
    timespan: Timespan = "day"
    adjusted: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.from_date > self.to_date:
            raise ValueError("from_date must be on or before to_date")
        if self.multiplier <= 0:
            raise ValueError("multiplier must be positive")
        spec = normalize_bar_grain(multiplier=self.multiplier, timespan=self.timespan)
        object.__setattr__(self, "multiplier", spec.multiplier)
        object.__setattr__(self, "timespan", spec.timespan)

    @classmethod
    def from_legacy_dict(
        cls, series_id: int, payload: dict[str, Any]
    ) -> "BackfillRequest":
        if payload.get("bar_grain"):
            grain = normalize_bar_grain(
                payload.get("bar_grain"),
                multiplier=int(payload["multiplier"])
                if "multiplier" in payload
                else None,
                timespan=payload.get("timespan"),
            )
        else:
            grain = normalize_bar_grain(
                multiplier=int(payload.get("multiplier", 1)),
                timespan=payload.get("timespan", "day"),
            )
        return cls(
            series_id=series_id,
            from_date=payload["from_date"],
            to_date=payload["to_date"],
            multiplier=grain.multiplier,
            timespan=grain.timespan,
            adjusted=bool(payload.get("adjusted", True)),
        )

    def to_legacy_dict(self) -> dict[str, Any]:
        return {
            "from_date": self.from_date.isoformat(),
            "to_date": self.to_date.isoformat(),
            "multiplier": self.multiplier,
            "timespan": self.timespan,
            "adjusted": self.adjusted,
            "day_count": date_range_days(self.from_date, self.to_date),
        }

    @property
    def request_hash(self) -> str:
        payload = {"ohlcv_series_id": self.series_id, **self.to_legacy_dict()}
        return stable_json_hash(payload)
