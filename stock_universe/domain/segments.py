"""Planned segment records."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from .common import freeze_json, parse_date, unfreeze_json


@dataclass(frozen=True)
class PlannedSegment:
    segment_index: int
    ticker: str
    from_date: dt.date
    to_date: dt.date
    source: str
    valid: bool = True
    validation: Any = field(default_factory=tuple)
    event_date: dt.date | None = None
    request_symbol: str | None = None
    extra: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.event_date is not None:
            object.__setattr__(self, "event_date", parse_date(self.event_date))
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "validation", freeze_json(self.validation))
        object.__setattr__(self, "extra", freeze_json(self.extra))
        if self.request_symbol is None:
            object.__setattr__(self, "request_symbol", self.ticker)

    @property
    def segment_id(self) -> str:
        return f"segment:{self.segment_index}"

    @classmethod
    def from_legacy_dict(cls, payload: dict[str, Any]) -> "PlannedSegment":
        known_fields = {
            "segment_index",
            "ticker",
            "from_date",
            "to_date",
            "source",
            "valid",
            "validation",
            "event_date",
            "request_symbol",
        }
        return cls(
            segment_index=int(payload["segment_index"]),
            ticker=str(payload["ticker"]),
            from_date=payload["from_date"],
            to_date=payload["to_date"],
            source=str(payload.get("source") or ""),
            valid=bool(payload.get("valid", True)),
            validation=payload.get("validation") or (),
            event_date=payload.get("event_date"),
            request_symbol=payload.get("request_symbol") or payload.get("ticker"),
            extra={
                key: value for key, value in payload.items() if key not in known_fields
            },
        )

    def to_legacy_dict(self) -> dict[str, Any]:
        result = {
            "from_date": self.from_date.isoformat(),
            "segment_index": self.segment_index,
            "source": self.source,
            "ticker": self.ticker,
            "to_date": self.to_date.isoformat(),
            "valid": self.valid,
            "validation": unfreeze_json(self.validation),
        }
        if self.event_date is not None:
            result["event_date"] = self.event_date.isoformat()
        if self.request_symbol and self.request_symbol != self.ticker:
            result["request_symbol"] = self.request_symbol
        result.update(unfreeze_json(self.extra))
        return result
