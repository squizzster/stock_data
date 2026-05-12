"""Target identity and alias records."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from .common import date_text, freeze_json, parse_date, unfreeze_json


@dataclass(frozen=True)
class ProviderIdentitySeed:
    natural_key: str
    company_id: int | None = None
    security_id: int | None = None
    company_name: str = ""
    current_company_name: str = ""
    cik: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    identity_status: str = "unknown"
    latest_ticker: str = ""
    latest_primary_exchange: str = ""
    locale: str = ""
    market: str = ""
    provisional_key: str | None = None
    security_type: str | None = None
    known_alias_tickers: tuple[str, ...] = ()
    same_ticker_other_permanent_identities: Any = field(default_factory=tuple)
    extra: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        key = str(self.natural_key or "").strip()
        if not key:
            raise ValueError("ProviderIdentitySeed.natural_key is required")
        object.__setattr__(self, "natural_key", key)
        object.__setattr__(
            self,
            "known_alias_tickers",
            tuple(str(item) for item in self.known_alias_tickers),
        )
        object.__setattr__(
            self,
            "same_ticker_other_permanent_identities",
            freeze_json(self.same_ticker_other_permanent_identities),
        )
        object.__setattr__(self, "extra", freeze_json(self.extra))

    def to_target_identity(self, ohlcv_series_id: int) -> "TargetIdentity":
        return TargetIdentity(
            ohlcv_series_id=ohlcv_series_id,
            company_id=self.company_id,
            security_id=self.security_id,
            company_name=self.company_name,
            current_company_name=self.current_company_name,
            cik=self.cik,
            composite_figi=self.composite_figi,
            share_class_figi=self.share_class_figi,
            identity_status=self.identity_status,
            latest_ticker=self.latest_ticker,
            latest_primary_exchange=self.latest_primary_exchange,
            locale=self.locale,
            market=self.market,
            natural_key=self.natural_key,
            provisional_key=self.provisional_key,
            security_type=self.security_type,
            known_alias_tickers=self.known_alias_tickers,
            same_ticker_other_permanent_identities=unfreeze_json(
                self.same_ticker_other_permanent_identities
            ),
            extra=unfreeze_json(self.extra),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "cik": self.cik,
            "company_id": self.company_id,
            "company_name": self.company_name,
            "composite_figi": self.composite_figi,
            "current_company_name": self.current_company_name,
            "identity_status": self.identity_status,
            "known_alias_tickers": list(self.known_alias_tickers),
            "latest_primary_exchange": self.latest_primary_exchange,
            "latest_ticker": self.latest_ticker,
            "locale": self.locale,
            "market": self.market,
            "natural_key": self.natural_key,
            "provisional_key": self.provisional_key,
            "security_id": self.security_id,
            "security_type": self.security_type,
            "share_class_figi": self.share_class_figi,
        }
        others = unfreeze_json(self.same_ticker_other_permanent_identities)
        if others:
            result["same_ticker_other_permanent_identities"] = others
        result.update(unfreeze_json(self.extra))
        return result


@dataclass(frozen=True)
class TargetIdentity:
    ohlcv_series_id: int
    company_id: int | None = None
    security_id: int | None = None
    company_name: str = ""
    current_company_name: str = ""
    cik: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    identity_status: str = "unknown"
    latest_ticker: str = ""
    latest_primary_exchange: str = ""
    locale: str = ""
    market: str = ""
    natural_key: str | None = None
    provisional_key: str | None = None
    security_type: str | None = None
    known_alias_tickers: tuple[str, ...] = ()
    same_ticker_other_permanent_identities: Any = field(default_factory=tuple)
    extra: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "known_alias_tickers",
            tuple(str(item) for item in self.known_alias_tickers),
        )
        object.__setattr__(
            self,
            "same_ticker_other_permanent_identities",
            freeze_json(self.same_ticker_other_permanent_identities),
        )
        object.__setattr__(self, "extra", freeze_json(self.extra))

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TargetIdentity":
        known_fields = {
            "ohlcv_series_id",
            "company_id",
            "security_id",
            "company_name",
            "current_company_name",
            "cik",
            "composite_figi",
            "share_class_figi",
            "identity_status",
            "latest_ticker",
            "latest_primary_exchange",
            "locale",
            "market",
            "natural_key",
            "provisional_key",
            "security_type",
            "known_alias_tickers",
            "same_ticker_other_permanent_identities",
        }
        extra = {
            key: value for key, value in payload.items() if key not in known_fields
        }
        return cls(
            ohlcv_series_id=int(payload["ohlcv_series_id"]),
            company_id=payload.get("company_id"),
            security_id=payload.get("security_id"),
            company_name=str(payload.get("company_name") or ""),
            current_company_name=str(payload.get("current_company_name") or ""),
            cik=str(payload.get("cik") or ""),
            composite_figi=str(payload.get("composite_figi") or ""),
            share_class_figi=str(payload.get("share_class_figi") or ""),
            identity_status=str(payload.get("identity_status") or "unknown"),
            latest_ticker=str(payload.get("latest_ticker") or ""),
            latest_primary_exchange=str(payload.get("latest_primary_exchange") or ""),
            locale=str(payload.get("locale") or ""),
            market=str(payload.get("market") or ""),
            natural_key=payload.get("natural_key"),
            provisional_key=payload.get("provisional_key"),
            security_type=payload.get("security_type"),
            known_alias_tickers=tuple(payload.get("known_alias_tickers") or ()),
            same_ticker_other_permanent_identities=payload.get(
                "same_ticker_other_permanent_identities"
            )
            or (),
            extra=extra,
        )

    def to_payload(self) -> dict[str, Any]:
        result = {
            "cik": self.cik,
            "company_id": self.company_id,
            "company_name": self.company_name,
            "composite_figi": self.composite_figi,
            "current_company_name": self.current_company_name,
            "identity_status": self.identity_status,
            "known_alias_tickers": list(self.known_alias_tickers),
            "latest_primary_exchange": self.latest_primary_exchange,
            "latest_ticker": self.latest_ticker,
            "locale": self.locale,
            "market": self.market,
            "natural_key": self.natural_key,
            "ohlcv_series_id": self.ohlcv_series_id,
            "provisional_key": self.provisional_key,
            "security_id": self.security_id,
            "security_type": self.security_type,
            "share_class_figi": self.share_class_figi,
        }
        others = unfreeze_json(self.same_ticker_other_permanent_identities)
        if others:
            result["same_ticker_other_permanent_identities"] = others
        result.update(unfreeze_json(self.extra))
        return result


@dataclass(frozen=True)
class KnownAlias:
    ticker: str
    active: int | bool | None = None
    as_of_date: dt.date | None = None
    company_name: str = ""
    primary_exchange: str = ""
    symbol_id: int | None = None
    extra: Any = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", str(self.ticker))
        if self.as_of_date is not None:
            object.__setattr__(self, "as_of_date", parse_date(self.as_of_date))
        object.__setattr__(self, "extra", freeze_json(self.extra))

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "KnownAlias":
        known_fields = {
            "symbol_text",
            "ticker",
            "active",
            "as_of_date",
            "company_name",
            "primary_exchange",
            "symbol_id",
        }
        return cls(
            ticker=str(payload.get("symbol_text") or payload.get("ticker") or ""),
            active=payload.get("active"),
            as_of_date=payload.get("as_of_date"),
            company_name=str(payload.get("company_name") or ""),
            primary_exchange=str(payload.get("primary_exchange") or ""),
            symbol_id=payload.get("symbol_id"),
            extra={
                key: value for key, value in payload.items() if key not in known_fields
            },
        )

    def to_payload(self) -> dict[str, Any]:
        result = {
            "active": self.active,
            "as_of_date": date_text(self.as_of_date),
            "company_name": self.company_name,
            "primary_exchange": self.primary_exchange,
            "symbol_id": self.symbol_id,
            "symbol_text": self.ticker,
        }
        result.update(unfreeze_json(self.extra))
        return result
