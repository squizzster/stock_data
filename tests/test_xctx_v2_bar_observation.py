from __future__ import annotations

import json

from stock_universe.storage.sqlite_repo import (
    SQLiteStockUniverseRepository,
    StoredOhlcvBar,
    StoredReferenceSnapshot,
)
from stock_universe.xctx.cli import main as xctx_main


def _seed_nvda_bar(db):
    repository = SQLiteStockUniverseRepository(db)
    natural_key = "massive:composite_figi:BBG000BBJQV0"
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="NVDA",
                active=True,
                company_name="NVIDIA Corporation",
                cik="0001045810",
                composite_figi="BBG000BBJQV0",
                share_class_figi="BBG001S5TZJ6",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key=natural_key,
                raw={"ticker": "NVDA"},
                source_request={"test": "xctx v2 bars"},
            )
        ]
    )
    series_id = repository.lookup_ohlcv_series_id(natural_key)
    assert series_id is not None
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="NVDA",
                bar_date="2026-01-09",
                bar_start_ts=1767916800000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=185.08,
                high=186.34,
                low=183.6701,
                close=184.86,
                volume=131327534,
                vwap=185.109,
                transaction_count=1024,
                request_hash="request-hash",
                ledger_hash="ledger-hash",
                segment_index=0,
                bar_quality_status="VALIDATED",
                raw_bar_json={
                    "o": 185.08,
                    "h": 186.34,
                    "l": 183.6701,
                    "c": 184.86,
                    "v": 131327534,
                },
            )
        ]
    )
    return series_id


def test_xctx_v2_single_bar_simple_returns_only_requested_observation(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-09",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload == {
        "succeeded": True,
        "actual_result": "bar_found",
        "ohlcv_series_id": series_id,
        "ticker": "NVDA",
        "date": "2026-01-09",
        "bar": {
            "open": 185.08,
            "high": 186.34,
            "low": 183.6701,
            "close": 184.86,
            "volume": 131327534.0,
            "vwap": 185.109,
        },
    }


def test_xctx_v2_bars_default_daily_and_can_select_minute_grain(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)
    repository = SQLiteStockUniverseRepository(db)
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="NVDA",
                bar_date="2026-01-09",
                bar_start_ts=1767969000000,
                multiplier=1,
                timespan="minute",
                adjusted=True,
                open=184.01,
                high=184.30,
                low=183.99,
                close=184.20,
                volume=5000,
                vwap=184.15,
                transaction_count=12,
                request_hash="minute-request",
                ledger_hash="minute-ledger",
                segment_index=0,
                bar_quality_status="VALIDATED",
            )
        ]
    )

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-09",
            ]
        )
        == 0
    )
    daily_payload = json.loads(capsys.readouterr().out)
    assert daily_payload["bar"]["open"] == 185.08
    assert "bar_grain" not in daily_payload

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-09",
                "--bar-grain",
                "1m",
            ]
        )
        == 0
    )
    minute_payload = json.loads(capsys.readouterr().out)
    assert minute_payload["bar_grain"] == "1m"
    assert minute_payload["bar"]["open"] == 184.01


def test_xctx_v2_bars_classifies_weekend_without_repair_actions(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-10",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["succeeded"] is False
    assert payload["actual_result"] == "this_is_a_weekend"
    assert payload["bar"] is None
    assert "next_actions" not in payload
    assert "effects" not in payload


def test_xctx_v2_bars_classifies_market_holiday_without_repair_actions(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-01",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["succeeded"] is False
    assert payload["actual_result"] == "this_is_a_market_holiday"
    assert payload["bar"] is None
    assert "next_actions" not in payload


def test_xctx_v2_bars_classifies_expected_missing_bar_as_repairable(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-12",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["succeeded"] is False
    assert payload["actual_result"] == "bar_expected_but_missing"
    assert payload["bar"] is None
    assert any(
        action["name"] == "plan-series-catch-up" for action in payload["next_actions"]
    )


def test_xctx_v2_bars_detail_adds_identity_and_calendar_context(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-10",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["view"] == "detail"
    assert payload["actual_result"] == "this_is_a_weekend"
    assert payload["calendar"]["session"] == "this_is_a_weekend"
    assert payload["calendar"]["is_trading_day"] is False
    assert payload["calendar"]["previous_trading_date"] == "2026-01-09"
    assert payload["calendar"]["next_trading_date"] == "2026-01-12"
    assert payload["identity"]["ohlcv_series_id"] == series_id


def test_xctx_v2_bars_extra_detail_adds_lineage_and_raw_payload(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    series_id = _seed_nvda_bar(db)

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2026-01-09",
                "--view",
                "extra_detail",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["view"] == "extra_detail"
    assert payload["bar"]["canonical"]["bar_start_ts"] == 1767969000000
    assert payload["bar"]["raw_provider_bar"]["h"] == 186.34
    assert payload["bar"]["raw_provider_ohlcv"]["high"] == 186.34
    assert payload["bar"]["lineage"]["request_hash"] == "request-hash"
    assert "effects" in payload
