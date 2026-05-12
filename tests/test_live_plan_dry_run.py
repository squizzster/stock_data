from __future__ import annotations

import argparse

from scripts.live_plan_dry_run import _planned_reads


def test_planned_reads_disclose_ticker_replacement_provider() -> None:
    reads = _planned_reads(
        argparse.Namespace(
            fixture="tests/fixtures/legacy_plans/ceg_invalid_event_ticker_replacement.json",
            api_key="secret",
        )
    )

    assert "massive.ticker_replacement" in reads
