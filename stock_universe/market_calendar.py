"""US equity trading calendar helpers."""

from __future__ import annotations

import bisect
import datetime as dt
import json
import os
from dataclasses import dataclass
from importlib import resources
from functools import lru_cache
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

MARKET_CALENDAR_ENV = "STOCK_UNIVERSE_MARKET_CALENDAR"
DEFAULT_MARKET_CALENDAR_FILE = "us_market_hours.json"
DEFAULT_MARKET_CALENDAR_RESOURCE = DEFAULT_MARKET_CALENDAR_FILE
DEFAULT_US_EQUITY_CALENDAR_ID = "US_EQUITY"
DEFAULT_US_EQUITY_TIMEZONE = "America/New_York"
DEFAULT_US_EQUITY_HISTORY_YEARS = 5


@dataclass(frozen=True)
class MarketSession:
    calendar_id: str
    session_date: str
    timezone_name: str
    regular_open_time: str
    regular_close_time: str
    session_open_time: str
    session_close_time: str
    regular_open_utc_ts: int
    regular_close_utc_ts: int
    session_open_utc_ts: int
    session_close_utc_ts: int
    settlement_date: str


def next_us_equity_trading_date(after_date: str | dt.date) -> str:
    """Return the first known US equity session after ``after_date``."""
    value = _parse_date(after_date)
    candidate = value + dt.timedelta(days=1)
    return first_us_equity_trading_date_on_or_after(candidate)


def previous_us_equity_trading_date(before_date: str | dt.date) -> str:
    """Return the last known US equity session before ``before_date``."""
    value = _parse_date(before_date)
    candidate = value - dt.timedelta(days=1)
    return last_us_equity_trading_date_on_or_before(candidate)


def first_us_equity_trading_date_on_or_after(value: str | dt.date) -> str:
    """Return the first known US equity session on or after ``value``."""
    candidate = _parse_date(value)
    sessions = _calendar_dates(_calendar_source())
    if sessions and sessions[0] <= candidate <= sessions[-1]:
        index = bisect.bisect_left(sessions, candidate)
        if index < len(sessions):
            return sessions[index].isoformat()
    return _next_weekday_on_or_after(candidate).isoformat()


def last_us_equity_trading_date_on_or_before(value: str | dt.date) -> str:
    """Return the last known US equity session on or before ``value``."""
    candidate = _parse_date(value)
    sessions = _calendar_dates(_calendar_source())
    if sessions and sessions[0] <= candidate <= sessions[-1]:
        index = bisect.bisect_right(sessions, candidate) - 1
        if index >= 0:
            return sessions[index].isoformat()
    return _previous_weekday_on_or_before(candidate).isoformat()


def is_us_equity_trading_date(value: str | dt.date) -> bool:
    date = _parse_date(value)
    sessions = _calendar_dates(_calendar_source())
    if sessions and sessions[0] <= date <= sessions[-1]:
        return date in set(sessions)
    return date.weekday() < 5


def classify_us_equity_session(value: str | dt.date) -> str:
    date = _parse_date(value)
    if is_us_equity_trading_date(date):
        return "trading_session"
    if date.weekday() >= 5:
        return "this_is_a_weekend"
    return "this_is_a_market_holiday"


def default_us_equity_history_start_date(
    to_date: str | dt.date | None = None,
    *,
    years: int = DEFAULT_US_EQUITY_HISTORY_YEARS,
) -> str:
    """Return the first session after the rolling lookback boundary."""
    if years < 1:
        raise ValueError("years must be positive")
    if to_date is None:
        end = _parse_date(
            last_us_equity_trading_date_on_or_before(dt.datetime.now(dt.UTC).date())
        )
    else:
        end = _parse_date(to_date)
    return next_us_equity_trading_date(_subtract_years(end, years))


def us_equity_session_for_date(
    value: str | dt.date, *, calendar_id: str = DEFAULT_US_EQUITY_CALENDAR_ID
) -> MarketSession | None:
    date = _parse_date(value)
    sessions = _calendar_session_map(_calendar_source())
    session = sessions.get(date)
    if session is not None:
        if calendar_id == DEFAULT_US_EQUITY_CALENDAR_ID:
            return session
        return _with_calendar_id(session, calendar_id)
    if date.weekday() >= 5:
        return None
    return _fallback_weekday_session(date, calendar_id=calendar_id)


def us_equity_session_for_utc_ts(
    utc_ts_ms: int, *, calendar_id: str = DEFAULT_US_EQUITY_CALENDAR_ID
) -> MarketSession | None:
    sessions = _calendar_sessions(_calendar_source())
    if sessions:
        ts = int(utc_ts_ms)
        containing = [
            session
            for session in sessions
            if session.session_open_utc_ts <= ts <= session.session_close_utc_ts
        ]
        if containing:
            session = containing[0]
            return session if calendar_id == DEFAULT_US_EQUITY_CALENDAR_ID else _with_calendar_id(session, calendar_id)
    utc_dt = dt.datetime.fromtimestamp(int(utc_ts_ms) / 1000, dt.UTC)
    session_date = utc_dt.astimezone(ZoneInfo(DEFAULT_US_EQUITY_TIMEZONE)).date()
    return us_equity_session_for_date(session_date, calendar_id=calendar_id)


def iter_us_equity_sessions(
    *, calendar_id: str = DEFAULT_US_EQUITY_CALENDAR_ID
) -> tuple[MarketSession, ...]:
    sessions = _calendar_sessions(_calendar_source())
    if calendar_id == DEFAULT_US_EQUITY_CALENDAR_ID:
        return sessions
    return tuple(_with_calendar_id(session, calendar_id) for session in sessions)


def _calendar_source() -> str:
    override = os.environ.get(MARKET_CALENDAR_ENV, "").strip()
    if override:
        return override
    if Path(DEFAULT_MARKET_CALENDAR_FILE).exists():
        return DEFAULT_MARKET_CALENDAR_FILE
    return f"resource:{DEFAULT_MARKET_CALENDAR_RESOURCE}"


@lru_cache(maxsize=8)
def _calendar_sessions(source: str) -> tuple[MarketSession, ...]:
    rows = _load_calendar_rows(source)
    sessions = sorted(
        (
            session
            for session in (_session_from_row(row) for row in _iter_rows(rows))
            if session is not None
        ),
        key=lambda item: item.session_date,
    )
    return tuple(sessions)


@lru_cache(maxsize=8)
def _calendar_session_map(source: str) -> dict[dt.date, MarketSession]:
    return {
        dt.date.fromisoformat(session.session_date): session
        for session in _calendar_sessions(source)
    }


@lru_cache(maxsize=8)
def _calendar_dates(source: str) -> tuple[dt.date, ...]:
    try:
        rows = _load_calendar_rows(source)
    except (FileNotFoundError, ModuleNotFoundError, OSError, json.JSONDecodeError):
        return ()
    dates = sorted(
        {
            parsed
            for parsed in (_date_from_row(row) for row in _iter_rows(rows))
            if parsed is not None
        }
    )
    return tuple(dates)


def _load_calendar_rows(source: str) -> object:
    if source.startswith("resource:"):
        resource_name = source.removeprefix("resource:")
        text = (
            resources.files("stock_universe")
            .joinpath(resource_name)
            .read_text(encoding="utf-8")
        )
    else:
        path = Path(source)
        if not path.exists():
            return ()
        text = path.read_text(encoding="utf-8")
    return json.loads(text)


def _iter_rows(value: object) -> Iterable[object]:
    if isinstance(value, list):
        return value
    return ()


def _date_from_row(row: object) -> dt.date | None:
    if not isinstance(row, dict):
        return None
    value = str(row.get("date") or "")
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def _session_from_row(row: object) -> MarketSession | None:
    if not isinstance(row, dict):
        return None
    date = _date_from_row(row)
    if date is None:
        return None
    regular_open = _normalize_time_text(row.get("open") or "09:30")
    regular_close = _normalize_time_text(row.get("close") or "16:00")
    session_open = _normalize_time_text(row.get("session_open") or regular_open)
    session_close = _normalize_time_text(row.get("session_close") or regular_close)
    settlement_date = str(row.get("settlement_date") or date.isoformat())
    return _build_session(
        date,
        regular_open=regular_open,
        regular_close=regular_close,
        session_open=session_open,
        session_close=session_close,
        settlement_date=settlement_date,
        calendar_id=DEFAULT_US_EQUITY_CALENDAR_ID,
    )


def _fallback_weekday_session(
    date: dt.date, *, calendar_id: str
) -> MarketSession:
    return _build_session(
        date,
        regular_open="09:30:00",
        regular_close="16:00:00",
        session_open="04:00:00",
        session_close="20:00:00",
        settlement_date=date.isoformat(),
        calendar_id=calendar_id,
    )


def _build_session(
    date: dt.date,
    *,
    regular_open: str,
    regular_close: str,
    session_open: str,
    session_close: str,
    settlement_date: str,
    calendar_id: str,
) -> MarketSession:
    return MarketSession(
        calendar_id=calendar_id,
        session_date=date.isoformat(),
        timezone_name=DEFAULT_US_EQUITY_TIMEZONE,
        regular_open_time=regular_open,
        regular_close_time=regular_close,
        session_open_time=session_open,
        session_close_time=session_close,
        regular_open_utc_ts=_session_utc_ts(date, regular_open),
        regular_close_utc_ts=_session_utc_ts(date, regular_close),
        session_open_utc_ts=_session_utc_ts(date, session_open),
        session_close_utc_ts=_session_utc_ts(date, session_close),
        settlement_date=settlement_date,
    )


def _with_calendar_id(session: MarketSession, calendar_id: str) -> MarketSession:
    return MarketSession(
        calendar_id=calendar_id,
        session_date=session.session_date,
        timezone_name=session.timezone_name,
        regular_open_time=session.regular_open_time,
        regular_close_time=session.regular_close_time,
        session_open_time=session.session_open_time,
        session_close_time=session.session_close_time,
        regular_open_utc_ts=session.regular_open_utc_ts,
        regular_close_utc_ts=session.regular_close_utc_ts,
        session_open_utc_ts=session.session_open_utc_ts,
        session_close_utc_ts=session.session_close_utc_ts,
        settlement_date=session.settlement_date,
    )


def _session_utc_ts(date: dt.date, time_text: str) -> int:
    hour, minute, second = (int(part) for part in time_text.split(":"))
    local_dt = dt.datetime(
        date.year,
        date.month,
        date.day,
        hour,
        minute,
        second,
        tzinfo=ZoneInfo(DEFAULT_US_EQUITY_TIMEZONE),
    )
    return int(local_dt.astimezone(dt.UTC).timestamp() * 1000)


def _normalize_time_text(value: object) -> str:
    text = str(value or "").strip()
    if len(text) == 4 and text.isdigit():
        text = f"{text[:2]}:{text[2:]}"
    parts = text.split(":")
    if len(parts) == 2:
        parts.append("00")
    if len(parts) != 3:
        raise ValueError(f"invalid session time: {value!r}")
    hour, minute, second = (int(part) for part in parts)
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def _next_weekday(value: dt.date) -> dt.date:
    candidate = value + dt.timedelta(days=1)
    return _next_weekday_on_or_after(candidate)


def _next_weekday_on_or_after(value: dt.date) -> dt.date:
    candidate = value
    while candidate.weekday() >= 5:
        candidate += dt.timedelta(days=1)
    return candidate


def _previous_weekday_on_or_before(value: dt.date) -> dt.date:
    candidate = value
    while candidate.weekday() >= 5:
        candidate -= dt.timedelta(days=1)
    return candidate


def _subtract_years(value: dt.date, years: int) -> dt.date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _parse_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))
