from __future__ import annotations

import json

from stock_universe.storage.sqlite_repo import (
    SQLiteStockUniverseRepository,
    StoredOhlcvBar,
    StoredReferenceSnapshot,
)
from stock_universe.xctx.cli import _select_bar_identity_candidate, main as xctx_main


def test_bar_identity_selection_dedupes_numeric_query_by_ohlcv_series_id() -> None:
    candidates = [
        {
            "ohlcv_series_id": 7964,
            "ticker": "NVDA",
            "security_type": "CS",
            "match_reason": "ohlcv_series_id_exact",
        },
        {
            "ohlcv_series_id": 7964,
            "ticker": "NVDA",
            "security_type": "CS",
            "match_reason": "ohlcv_series_id_exact",
        },
    ]

    selected = _select_bar_identity_candidate("7964", candidates)

    assert selected is not None
    assert selected["ohlcv_series_id"] == 7964


def test_bar_identity_selection_dedupes_exact_ticker_before_declaring_ambiguity() -> (
    None
):
    candidates = [
        {
            "ohlcv_series_id": 7964,
            "ticker": "NVDA",
            "security_type": "CS",
            "match_reason": "ticker_exact_case",
        },
        {
            "ohlcv_series_id": 7964,
            "ticker": "NVDA",
            "security_type": "CS",
            "match_reason": "ticker_exact_case",
        },
        {
            "ohlcv_series_id": 7963,
            "ticker": "NVD",
            "security_type": "ETF",
            "match_reason": "company_name_word",
        },
    ]

    selected = _select_bar_identity_candidate("NVDA", candidates)

    assert selected is not None
    assert selected["ohlcv_series_id"] == 7964


def test_xctx_bars_simple_and_detail_views_separate_canonical_from_raw(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
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
                source_request={"test": "xctx bars"},
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
                bar_date="2024-06-10",
                bar_start_ts=1717977600000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=120.37,
                high=123.10,
                low=117.01,
                close=121.79,
                volume=314157461,
                vwap=121.1155,
                transaction_count=1024,
                request_hash="request-hash",
                ledger_hash="ledger-hash",
                segment_index=0,
                bar_quality_status="VALIDATED_REPAIRED",
                repair_rule="provider-raw-split-anomaly",
                raw_bar_json={
                    "o": 120.37,
                    "h": 195.95,
                    "l": 117.01,
                    "c": 121.79,
                    "v": 314157461,
                },
                repair_evidence_json={"canonical_high": 123.10, "raw_high": 195.95},
            )
        ]
    )

    assert (
        xctx_main(["bars", "--db", str(db), "--query", "NVDA", "--date", "2024-06-10"])
        == 0
    )
    simple = json.loads(capsys.readouterr().out)

    assert simple["succeeded"] is True
    assert simple["actual_result"] == "bar_found"
    assert simple["bar"]["high"] == 123.10
    assert "quality_status" not in simple["bar"]
    assert "raw_provider_bar" not in simple["bar"]

    assert (
        xctx_main(
            ["bars", "--db", str(db), "--query", str(series_id), "--date", "2024-06-10"]
        )
        == 0
    )
    by_id_query = json.loads(capsys.readouterr().out)

    assert by_id_query["bar"]["close"] == 121.79
    assert by_id_query["ohlcv_series_id"] == series_id

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2024-06-10",
                "--view",
                "detail",
            ]
        )
        == 0
    )
    detail = json.loads(capsys.readouterr().out)

    assert detail["view"] == "detail"
    assert detail["bar"]["high"] == 123.10
    assert detail["bar"]["quality_status"] == "VALIDATED_REPAIRED"
    assert "raw_provider_ohlcv" not in detail["bar"]

    assert (
        xctx_main(
            [
                "bars",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--date",
                "2024-06-10",
                "--view",
                "extra_detail",
            ]
        )
        == 0
    )
    extra_detail = json.loads(capsys.readouterr().out)

    assert extra_detail["view"] == "extra_detail"
    assert extra_detail["bar"]["canonical"]["bar_start_ts"] == 1718026200000
    assert extra_detail["bar"]["canonical"]["source"] == "massive.aggregate_bars"
    assert extra_detail["bar"]["raw_provider_bar"]["h"] == 195.95
    assert extra_detail["bar"]["raw_provider_ohlcv"]["high"] == 195.95
    assert extra_detail["bar"]["lineage"]["repair_rule"] == "provider-raw-split-anomaly"


def test_xctx_bars_rejects_invalid_ohlcv_series_id(tmp_path, capsys) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert (
        xctx_main(
            ["bars", "--db", str(db), "--ohlcv-series-id", "0", "--date", "2024-06-10"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "invalid_ohlcv_series_id"


def test_xctx_bars_requires_explicit_ohlcv_series_id_for_ambiguous_query(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="GOOG",
                active=True,
                company_name="Alphabet Inc. Class C Capital Stock",
                cik="0001652044",
                composite_figi="BBG009S3NB30",
                share_class_figi="BBG009S3NB21",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBG009S3NB30",
                raw={"ticker": "GOOG"},
                source_request={"test": "xctx bars"},
            ),
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="GOOGL",
                active=True,
                company_name="Alphabet Inc. Class A Common Stock",
                cik="0001652044",
                composite_figi="BBG009S39JX6",
                share_class_figi="BBG009S39JY5",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBG009S39JX6",
                raw={"ticker": "GOOGL"},
                source_request={"test": "xctx bars"},
            ),
        ]
    )

    assert (
        xctx_main(
            ["bars", "--db", str(db), "--query", "0001652044", "--date", "2024-06-10"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["errors"][0]["code"] == "ambiguous_ohlcv_identity"
    assert (
        payload["next_actions"][0]["command"]["args"]["ohlcv_series_id"]
        == "{selected_candidate.ohlcv_series_id}"
    )
