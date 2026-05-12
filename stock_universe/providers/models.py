"""Provider read models before evidence normalization."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from stock_universe.domain.records import freeze_json, parse_date, unfreeze_json


@dataclass(frozen=True)
class ReferenceSnapshot:
    ticker: str
    as_of_date: dt.date
    api_status: str
    response_ticker: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    cik: str = ""
    primary_exchange: str = ""
    security_type: str = ""
    active: bool | None = None
    raw: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "raw", freeze_json(self.raw))

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "active": self.active,
            "api_status": self.api_status,
            "cik": self.cik,
            "composite_figi": self.composite_figi,
            "date": self.as_of_date.isoformat(),
            "primary_exchange": self.primary_exchange,
            "requested_ticker": self.ticker,
            "response_ticker": self.response_ticker or self.ticker,
            "share_class_figi": self.share_class_figi,
            "type": self.security_type,
        }
        raw = unfreeze_json(self.raw)
        if raw:
            payload["raw"] = raw
        return payload


@dataclass(frozen=True)
class ReferenceBoundaryProbe:
    ticker: str
    as_of_date: dt.date
    point: str
    snapshot: ReferenceSnapshot

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "point", str(self.point))
        if self.point not in {"start", "end"}:
            raise ValueError("point must be 'start' or 'end'")
        if self.snapshot.ticker != self.ticker:
            raise ValueError("snapshot ticker must match probe ticker")
        if self.snapshot.as_of_date != self.as_of_date:
            raise ValueError("snapshot date must match probe date")


@dataclass(frozen=True)
class BarProbeResult:
    ticker: str
    from_date: dt.date
    to_date: dt.date
    bar_count: int
    api_status: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.from_date > self.to_date:
            raise ValueError("from_date must be on or before to_date")
        if self.bar_count < 0:
            raise ValueError("bar_count must be non-negative")


@dataclass(frozen=True)
class IdentityScanResult:
    query: str
    as_of_date: dt.date
    matches: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", str(self.query))
        object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "matches", freeze_json(self.matches))


@dataclass(frozen=True)
class OmittedSegmentProbe:
    ticker: str
    from_date: dt.date
    to_date: dt.date

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))


@dataclass(frozen=True)
class TickerReplacementWindow:
    old_ticker: str
    new_ticker: str
    from_date: dt.date
    to_date: dt.date
    event_date: dt.date | None = None
    replacement_reason: str = "known_alias_boundary_validation"

    def __post_init__(self) -> None:
        object.__setattr__(self, "old_ticker", str(self.old_ticker))
        object.__setattr__(self, "new_ticker", str(self.new_ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.event_date is not None:
            object.__setattr__(self, "event_date", parse_date(self.event_date))


@dataclass(frozen=True)
class HandoffWindow:
    event_ticker: str
    candidate_ticker: str
    from_date: dt.date
    to_date: dt.date
    event_date: dt.date | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_ticker", str(self.event_ticker))
        object.__setattr__(self, "candidate_ticker", str(self.candidate_ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.event_date is not None:
            object.__setattr__(self, "event_date", parse_date(self.event_date))
