"""Evidence request, fact, and ledger records."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from .common import date_text, freeze_json, parse_date, stable_json_hash, unfreeze_json


@dataclass(frozen=True)
class AliasHistoryFact:
    spans: Any = field(default_factory=tuple)
    source: str = "alias_history"

    def __post_init__(self) -> None:
        normalized = []
        for index, span in enumerate(self.spans, 1):
            normalized.append(
                {
                    "event_date": date_text(span.get("event_date")),
                    "from_date": date_text(span["from_date"]),
                    "segment_index": int(span.get("segment_index") or index),
                    "source": str(span.get("source") or "alias_history"),
                    "ticker": str(span["ticker"]),
                    "to_date": date_text(span["to_date"]),
                    "valid": bool(span.get("valid", True)),
                    "validation": span.get("validation") or [],
                }
            )
        normalized.sort(
            key=lambda item: (item["from_date"] or "", item["segment_index"])
        )
        object.__setattr__(self, "spans", freeze_json(normalized))

    @classmethod
    def from_segments_payload(
        cls, segments: list[dict[str, Any]], source: str = "plan_payload"
    ) -> "AliasHistoryFact":
        alias_spans = [
            segment
            for segment in segments
            if str(segment.get("source") or "").startswith("known_alias_")
            or str(segment.get("source") or "").startswith("identity_scan_")
        ]
        return cls(alias_spans, source)

    def to_payload(self) -> dict[str, Any]:
        return {"spans": unfreeze_json(self.spans)}

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "alias_history", (str(series_id),), self.to_payload(), self.source
        )


@dataclass(frozen=True)
class EvidenceRequest:
    kind: str
    key: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", tuple(str(item) for item in self.key))

    def to_payload(self) -> dict[str, Any]:
        return {"kind": self.kind, "key": list(self.key)}


@dataclass(frozen=True)
class EvidenceFact:
    kind: str
    key: tuple[str, ...]
    payload: Any = field(default_factory=tuple)
    source: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", tuple(str(item) for item in self.key))
        object.__setattr__(self, "payload", freeze_json(self.payload))

    def payload_value(self) -> Any:
        payload = unfreeze_json(self.payload)
        if (
            self.kind == "identity_scan"
            and isinstance(payload, dict)
            and payload.get("matches") == {}
        ):
            payload = dict(payload)
            payload["matches"] = []
        return payload

    def to_payload(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "key": list(self.key),
            "payload": self.payload_value(),
            "source": self.source,
        }


@dataclass(frozen=True)
class TickerEventFact:
    identifier: str
    identifier_type: str
    api_status: str
    events: Any = field(default_factory=tuple)
    event_cik: str = ""
    event_composite_figi: str = ""
    event_name: str = ""
    source: str = "ticker_events"

    def __post_init__(self) -> None:
        normalized = []
        for event in self.events:
            ticker = event.get("ticker")
            if ticker is None and isinstance(event.get("ticker_change"), dict):
                ticker = event["ticker_change"].get("ticker")
            normalized.append(
                {
                    "date": date_text(event.get("date")),
                    "ticker": str(ticker or ""),
                    "type": str(event.get("type") or "ticker_change"),
                }
            )
        normalized.sort(key=lambda item: (item["date"] or "", item["ticker"]))
        object.__setattr__(self, "events", freeze_json(normalized))

    @classmethod
    def from_provider_payload(
        cls,
        identifier: str,
        identifier_type: str,
        payload: dict[str, Any],
        source: str = "massive.ticker_events",
    ) -> "TickerEventFact":
        results = payload.get("results") or {}
        return cls(
            identifier=identifier,
            identifier_type=identifier_type,
            api_status=str(payload.get("status") or ""),
            event_cik=str(results.get("cik") or ""),
            event_composite_figi=str(results.get("composite_figi") or ""),
            event_name=str(results.get("name") or ""),
            events=results.get("events") or (),
            source=source,
        )

    @classmethod
    def from_event_lookup_payload(
        cls, payload: dict[str, Any], source: str = "plan_payload"
    ) -> "TickerEventFact":
        return cls(
            identifier=str(payload.get("identifier") or ""),
            identifier_type=str(payload.get("identifier_type") or ""),
            api_status=str(payload.get("api_status") or ""),
            event_cik=str(payload.get("event_cik") or ""),
            event_composite_figi=str(payload.get("event_composite_figi") or ""),
            event_name=str(payload.get("event_name") or ""),
            events=payload.get("events") or (),
            source=source,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "api_status": self.api_status,
            "event_cik": self.event_cik,
            "event_composite_figi": self.event_composite_figi,
            "event_name": self.event_name,
            "events": unfreeze_json(self.events),
            "identifier": self.identifier,
            "identifier_type": self.identifier_type,
        }

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "ticker_events",
            (str(series_id), self.identifier),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class ReferenceBoundaryFact:
    ticker: str
    as_of_date: dt.date
    api_status: str
    matched: bool
    match_reason: str = ""
    payload: Any = field(default_factory=tuple)
    source: str = "reference_boundary"

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "payload", freeze_json(self.payload))

    def to_payload(self) -> dict[str, Any]:
        return {
            "api_status": self.api_status,
            "as_of_date": self.as_of_date.isoformat(),
            "matched": self.matched,
            "match_reason": self.match_reason,
            "payload": unfreeze_json(self.payload),
            "ticker": self.ticker,
        }

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "reference_boundary",
            (str(series_id), self.ticker, self.as_of_date.isoformat()),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class BarProbeFact:
    ticker: str
    from_date: dt.date
    to_date: dt.date
    bar_count: int
    api_status: str = ""
    source: str = "bar_probe"

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))

    def to_payload(self) -> dict[str, Any]:
        return {
            "api_status": self.api_status,
            "bar_count": self.bar_count,
            "from_date": self.from_date.isoformat(),
            "ticker": self.ticker,
            "to_date": self.to_date.isoformat(),
        }

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "bar_probe",
            (
                str(series_id),
                self.ticker,
                self.from_date.isoformat(),
                self.to_date.isoformat(),
            ),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class OmittedSegmentFact:
    ticker: str
    from_date: dt.date
    to_date: dt.date
    reason: str
    source: str = "omitted_segment"
    proof: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        object.__setattr__(self, "proof", freeze_json(self.proof))

    def to_payload(self) -> dict[str, Any]:
        result = {
            "from_date": self.from_date.isoformat(),
            "reason": self.reason,
            "ticker": self.ticker,
            "to_date": self.to_date.isoformat(),
        }
        proof = unfreeze_json(self.proof)
        if proof:
            result["proof"] = proof
        return result

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "omitted_segment",
            (
                str(series_id),
                self.ticker,
                self.from_date.isoformat(),
                self.to_date.isoformat(),
            ),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class TerminalCoverageFact:
    ticker: str
    from_date: dt.date
    to_date: dt.date
    reason: str
    source: str = "terminal_coverage"

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))

    def to_payload(self) -> dict[str, Any]:
        return {
            "from_date": self.from_date.isoformat(),
            "reason": self.reason,
            "ticker": self.ticker,
            "to_date": self.to_date.isoformat(),
        }

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "terminal_coverage",
            (
                str(series_id),
                self.ticker,
                self.from_date.isoformat(),
                self.to_date.isoformat(),
            ),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class TickerReplacementFact:
    old_ticker: str
    new_ticker: str
    from_date: dt.date
    to_date: dt.date
    replacement_reason: str
    source: str
    event_date: dt.date | None = None
    validation: Any = field(default_factory=tuple)
    metadata: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "old_ticker", str(self.old_ticker))
        object.__setattr__(self, "new_ticker", str(self.new_ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.event_date is not None:
            object.__setattr__(self, "event_date", parse_date(self.event_date))
        object.__setattr__(self, "validation", freeze_json(self.validation))
        object.__setattr__(self, "metadata", freeze_json(self.metadata))

    @classmethod
    def from_segment_payload(
        cls, segment: dict[str, Any], source: str = "plan_payload"
    ) -> "TickerReplacementFact":
        replacement = segment["ticker_replacement"]
        return cls(
            old_ticker=str(replacement["old_ticker"]),
            new_ticker=str(replacement["new_ticker"]),
            from_date=segment["from_date"],
            to_date=segment["to_date"],
            replacement_reason=str(replacement.get("replacement_reason") or ""),
            source=str(segment.get("source") or ""),
            event_date=segment.get("event_date"),
            validation=segment.get("validation") or (),
            metadata={
                "ticker_replacement": replacement,
                **{
                    key: value
                    for key, value in segment.items()
                    if key
                    in {
                        "identity_scan_replacement",
                        "start_alias_identity_bridge",
                        "start_validation_override",
                        "end_validation_override",
                        "leading_identity_trim",
                        "first_bar_replacement",
                        "event_ticker_handoff",
                    }
                },
            },
        )

    def to_payload(self) -> dict[str, Any]:
        result = {
            "event_date": date_text(self.event_date),
            "from_date": self.from_date.isoformat(),
            "new_ticker": self.new_ticker,
            "old_ticker": self.old_ticker,
            "replacement_reason": self.replacement_reason,
            "source": self.source,
            "to_date": self.to_date.isoformat(),
            "validation": unfreeze_json(self.validation),
        }
        result.update(unfreeze_json(self.metadata))
        return result

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "ticker_replacement",
            (
                str(series_id),
                self.old_ticker,
                self.from_date.isoformat(),
                self.to_date.isoformat(),
            ),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class HandoffSegmentFact:
    ticker: str
    from_date: dt.date
    to_date: dt.date
    source: str
    event_ticker_handoff: Any = field(default_factory=tuple)
    validation: Any = field(default_factory=tuple)
    event_date: dt.date | None = None
    extra: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        object.__setattr__(self, "from_date", parse_date(self.from_date))
        object.__setattr__(self, "to_date", parse_date(self.to_date))
        if self.event_date is not None:
            object.__setattr__(self, "event_date", parse_date(self.event_date))
        object.__setattr__(
            self, "event_ticker_handoff", freeze_json(self.event_ticker_handoff)
        )
        object.__setattr__(self, "validation", freeze_json(self.validation))
        object.__setattr__(self, "extra", freeze_json(self.extra))

    @classmethod
    def from_segment_payload(
        cls, segment: dict[str, Any], source: str = "plan_payload"
    ) -> "HandoffSegmentFact":
        known_fields = {
            "event_date",
            "event_ticker_handoff",
            "from_date",
            "segment_index",
            "source",
            "ticker",
            "to_date",
            "valid",
            "validation",
        }
        return cls(
            ticker=str(segment["ticker"]),
            from_date=segment["from_date"],
            to_date=segment["to_date"],
            source=str(segment.get("source") or ""),
            event_ticker_handoff=segment.get("event_ticker_handoff") or {},
            validation=segment.get("validation") or (),
            event_date=segment.get("event_date"),
            extra={
                key: value for key, value in segment.items() if key not in known_fields
            },
        )

    def to_payload(self) -> dict[str, Any]:
        result = {
            "event_date": date_text(self.event_date),
            "event_ticker_handoff": unfreeze_json(self.event_ticker_handoff),
            "from_date": self.from_date.isoformat(),
            "source": self.source,
            "ticker": self.ticker,
            "to_date": self.to_date.isoformat(),
            "validation": unfreeze_json(self.validation),
        }
        result.update(unfreeze_json(self.extra))
        return result

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "handoff_segment",
            (
                str(series_id),
                self.ticker,
                self.from_date.isoformat(),
                self.to_date.isoformat(),
            ),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class IdentityScanFact:
    as_of_date: dt.date
    query: str
    matches: Any = field(default_factory=tuple)
    source: str = "identity_scan"

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "matches", freeze_json(self.matches))

    def to_payload(self) -> dict[str, Any]:
        matches = [] if self.matches == () else unfreeze_json(self.matches)
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "matches": matches,
            "query": self.query,
        }

    def to_evidence_fact(self, series_id: int | str) -> EvidenceFact:
        return EvidenceFact(
            "identity_scan",
            (str(series_id), self.query, self.as_of_date.isoformat()),
            self.to_payload(),
            self.source,
        )


@dataclass(frozen=True)
class EvidenceSnapshot:
    facts: tuple[EvidenceFact, ...]
    ledger_hash: str

    def get_all(self, kind: str) -> tuple[EvidenceFact, ...]:
        return tuple(fact for fact in self.facts if fact.kind == kind)

    def get_one(self, kind: str) -> EvidenceFact | None:
        matches = self.get_all(kind)
        return matches[-1] if matches else None


@dataclass(frozen=True)
class EvidenceLedger:
    facts: tuple[EvidenceFact, ...] = ()

    def append(self, fact: EvidenceFact) -> "EvidenceLedger":
        return EvidenceLedger(self.facts + (fact,))

    def merge(
        self, facts: "EvidenceLedger | tuple[EvidenceFact, ...]"
    ) -> "EvidenceLedger":
        if isinstance(facts, EvidenceLedger):
            return EvidenceLedger(self.facts + facts.facts)
        return EvidenceLedger(self.facts + tuple(facts))

    @property
    def ledger_hash(self) -> str:
        return stable_json_hash([fact.to_payload() for fact in self.facts])

    def snapshot(self) -> EvidenceSnapshot:
        return EvidenceSnapshot(facts=self.facts, ledger_hash=self.ledger_hash)
