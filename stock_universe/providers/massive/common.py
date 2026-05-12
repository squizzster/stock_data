"""Compatibility re-exports for Massive provider helper functions."""

from __future__ import annotations

from stock_universe.providers.massive.coverage_helpers import (
    _known_alias_replacement_for_gap,
    _omitted_fact_for_absent_ticker_interval,
    _omitted_fact_from_intrabar_non_target_interval,
    _omitted_fact_from_non_downloadable_interval,
    _omitted_proof,
    _probe_dates_align,
    _scan_aliases_have_no_bars,
    _target_identity_scan_aliases,
    _terminal_coverage_fact,
)
from stock_universe.providers.massive.payloads import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _bar_probe_result_from_payload,
    _identity_scan_result_from_payload,
    _reference_snapshot_from_payload,
)
from stock_universe.providers.massive.reference_helpers import (
    START_GAP_BAR_SCAN_LIMIT,
    _first_bar_boundary_fact_after_start_gap,
    _first_matching_suffix_boundary_fact,
    _historical_figi_rekey_reason,
    _reference_boundary_fact_with_historical_rekey,
    _reference_is_conclusive_non_target,
    _reference_is_target_match,
    _reference_missing_durable_ids_without_contradiction,
    _reference_name,
    _retag_reference_boundary_fact,
    _segment_validation_row,
)
from stock_universe.providers.massive.replacement_helpers import (
    _event_ticker_is_absent_and_has_no_bars,
    _historical_bar_alias_current_end_reason,
    _historical_figi_rekey_bar_alias_replacement_fact,
    _missing_durable_start_replacement_fact,
)

__all__ = [
    "START_GAP_BAR_SCAN_LIMIT",
    "_aggregate_bars_payload",
    "_bar_dates_from_payload",
    "_bar_probe_result_from_payload",
    "_event_ticker_is_absent_and_has_no_bars",
    "_first_bar_boundary_fact_after_start_gap",
    "_first_matching_suffix_boundary_fact",
    "_historical_bar_alias_current_end_reason",
    "_historical_figi_rekey_bar_alias_replacement_fact",
    "_historical_figi_rekey_reason",
    "_identity_scan_result_from_payload",
    "_known_alias_replacement_for_gap",
    "_missing_durable_start_replacement_fact",
    "_omitted_fact_for_absent_ticker_interval",
    "_omitted_fact_from_intrabar_non_target_interval",
    "_omitted_fact_from_non_downloadable_interval",
    "_omitted_proof",
    "_probe_dates_align",
    "_reference_boundary_fact_with_historical_rekey",
    "_reference_is_conclusive_non_target",
    "_reference_is_target_match",
    "_reference_missing_durable_ids_without_contradiction",
    "_reference_name",
    "_reference_snapshot_from_payload",
    "_retag_reference_boundary_fact",
    "_scan_aliases_have_no_bars",
    "_segment_validation_row",
    "_target_identity_scan_aliases",
    "_terminal_coverage_fact",
]
