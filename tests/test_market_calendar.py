from __future__ import annotations

import json
from pathlib import Path

from stock_universe.market_calendar import (
    MARKET_CALENDAR_ENV,
    default_us_equity_history_start_date,
    first_us_equity_trading_date_on_or_after,
    is_us_equity_trading_date,
    last_us_equity_trading_date_on_or_before,
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
    us_equity_session_for_date,
    us_equity_session_for_utc_ts,
)


def test_market_calendar_uses_root_session_file_by_default(monkeypatch) -> None:
    monkeypatch.delenv(MARKET_CALENDAR_ENV, raising=False)

    assert Path("us_market_hours.json").is_file()
    assert last_us_equity_trading_date_on_or_before("2026-01-01") == "2025-12-31"
    assert first_us_equity_trading_date_on_or_after("2026-01-01") == "2026-01-02"
    assert is_us_equity_trading_date("2026-01-01") is False


def test_default_history_start_is_first_session_after_five_year_boundary() -> None:
    assert default_us_equity_history_start_date("2026-05-12") == "2021-05-13"


def test_market_calendar_falls_back_outside_packaged_date_range(monkeypatch) -> None:
    monkeypatch.delenv(MARKET_CALENDAR_ENV, raising=False)

    assert next_us_equity_trading_date("2010-01-05") == "2010-01-06"
    assert previous_us_equity_trading_date("2010-01-05") == "2010-01-04"
    assert first_us_equity_trading_date_on_or_after("2035-01-06") == "2035-01-08"
    assert last_us_equity_trading_date_on_or_before("2035-01-06") == "2035-01-05"
    assert is_us_equity_trading_date("2035-01-05") is True
    assert is_us_equity_trading_date("2035-01-06") is False


def test_market_calendar_uses_session_file_for_next_and_previous_dates(
    tmp_path: Path, monkeypatch
) -> None:
    calendar = tmp_path / "sessions.json"
    calendar.write_text(
        json.dumps(
            [
                {"date": "2026-03-06", "open": "09:30", "close": "16:00"},
                {"date": "2026-03-10", "open": "09:30", "close": "16:00"},
                {"date": "2026-03-11", "open": "09:30", "close": "16:00"},
            ]
        )
    )
    monkeypatch.setenv(MARKET_CALENDAR_ENV, str(calendar))

    assert next_us_equity_trading_date("2026-03-06") == "2026-03-10"
    assert first_us_equity_trading_date_on_or_after("2026-03-08") == "2026-03-10"
    assert last_us_equity_trading_date_on_or_before("2026-03-09") == "2026-03-06"
    assert previous_us_equity_trading_date("2026-03-10") == "2026-03-06"
    assert is_us_equity_trading_date("2026-03-10") is True
    assert is_us_equity_trading_date("2026-03-09") is False


def test_market_calendar_exposes_session_boundaries_from_file(
    tmp_path: Path, monkeypatch
) -> None:
    calendar = tmp_path / "sessions.json"
    calendar.write_text(
        json.dumps(
            [
                {
                    "date": "2026-07-02",
                    "open": "09:30",
                    "close": "16:00",
                    "session_open": "0400",
                    "session_close": "2000",
                    "settlement_date": "2026-07-06",
                },
                {
                    "date": "2026-07-03",
                    "open": "09:30",
                    "close": "13:00",
                    "session_open": "0400",
                    "session_close": "1700",
                    "settlement_date": "2026-07-07",
                },
            ]
        )
    )
    monkeypatch.setenv(MARKET_CALENDAR_ENV, str(calendar))

    session = us_equity_session_for_date("2026-07-03", calendar_id="XNAS")

    assert session is not None
    assert session.calendar_id == "XNAS"
    assert session.session_date == "2026-07-03"
    assert session.regular_close_time == "13:00:00"
    assert session.session_close_time == "17:00:00"
    assert session.settlement_date == "2026-07-07"
    assert session.session_open_utc_ts < session.regular_open_utc_ts
    assert session.regular_close_utc_ts < session.session_close_utc_ts


def test_market_calendar_maps_utc_timestamp_to_exchange_session(
    tmp_path: Path, monkeypatch
) -> None:
    calendar = tmp_path / "sessions.json"
    calendar.write_text(
        json.dumps(
            [
                {
                    "date": "2026-05-11",
                    "open": "09:30",
                    "close": "16:00",
                    "session_open": "0400",
                    "session_close": "2000",
                    "settlement_date": "2026-05-13",
                }
            ]
        )
    )
    monkeypatch.setenv(MARKET_CALENDAR_ENV, str(calendar))

    session = us_equity_session_for_utc_ts(1778486400000, calendar_id="XNAS")

    assert session is not None
    assert session.calendar_id == "XNAS"
    assert session.session_date == "2026-05-11"
