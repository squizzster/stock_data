from __future__ import annotations

import json
from pathlib import Path

from stock_universe.ops import (
    build_pressure_run_manifest,
    list_pressure_cohort_plans,
    pressure_cohort_plan,
)


def test_pressure_manifest_records_report_summary(tmp_path: Path) -> None:
    report_path = tmp_path / "pressure_report.json"
    report_path.write_text(
        json.dumps(
            {
                "attempted": 3,
                "ok_count": 2,
                "skipped_count": 1,
                "error_count": 0,
                "counts": {"ohlcv_bars": 12, "execution_receipts": 2},
                "validation": {"ok": True, "checks": ["foreign keys valid"]},
                "results": [
                    {
                        "status": "ok",
                        "fixture": "a.json",
                        "request_count": 2,
                        "series_id": 1,
                    },
                    {
                        "status": "ok",
                        "fixture": "b.json",
                        "request_count": 7,
                        "series_id": 2,
                    },
                    {
                        "status": "skipped",
                        "fixture": "c.json",
                        "planning_request_count": 4,
                        "series_id": 3,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    manifest = build_pressure_run_manifest(
        report_path=report_path,
        cohort="unit-3",
        command=["python", "scripts/live_sqlite_backfill.py", "--strict"],
        db_path=tmp_path / "stock.sqlite",
        generated_at_utc="2026-05-07T00:00:00+00:00",
        repo_root=tmp_path,
        git_head="abc123",
        git_dirty=False,
    )

    assert manifest["schema_version"] == "stock_universe.pressure_run_manifest.v1"
    assert manifest["repo"] == {
        "root": str(tmp_path),
        "git_head": "abc123",
        "git_dirty": False,
    }
    assert manifest["cohort"] == "unit-3"
    assert manifest["cohort_plan"]["known"] is False
    assert manifest["cohort_plan"]["name"] == "unit-3"
    assert manifest["summary"]["attempted"] == 3
    assert manifest["summary"]["status_counts"] == {"ok": 2, "skipped": 1}
    assert manifest["summary"]["counts"]["ohlcv_bars"] == 12
    assert manifest["validation"]["ok"] is True
    assert manifest["artifacts"]["report_sha256"]
    assert manifest["request_efficiency"]["instrumented_result_count"] == 3
    assert manifest["request_efficiency"]["total_observed_request_count"] == 13
    assert manifest["request_efficiency"]["max_observed_request_count"] == 7
    assert (
        manifest["request_efficiency"]["top_observed_request_counts"][0]["input"]
        == "b.json"
    )
    assert (
        manifest["request_efficiency"]["top_observed_request_counts"][0][
            "ohlcv_series_id"
        ]
        == 2
    )
    assert (
        "series_id"
        not in manifest["request_efficiency"]["top_observed_request_counts"][0]
    )
    assert (
        manifest["request_efficiency"]["top_observed_request_counts"][0][
            "receipt_request_count"
        ]
        == 7
    )


def test_known_pressure_cohort_plan_defines_scale_out_success_factors() -> None:
    baseline = pressure_cohort_plan("baseline-50")
    expansion = pressure_cohort_plan("expansion-100")

    assert baseline["known"] is True
    assert baseline["target_size"] == 50
    assert baseline["next_cohort"] == "expansion-100"
    assert "each receipt has a durable approval" in baseline["success_factors"]
    assert expansion["target_size"] == 100
    assert (
        "new failures become fixtures or typed evidence gaps"
        in expansion["success_factors"]
    )


def test_list_pressure_cohort_plans_returns_recommended_order() -> None:
    plans = list_pressure_cohort_plans()

    assert [plan["name"] for plan in plans] == [
        "baseline-50",
        "expansion-100",
        "expansion-250",
        "exchange-category",
        "ugly-historical",
    ]
    assert plans[-1]["next_cohort"] == "maintain-and-repeat"
