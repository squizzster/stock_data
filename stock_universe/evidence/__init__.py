"""Evidence collection and provider adapters."""

from .collectors import (
    BackfillEvidenceSource,
    EvidenceCollectionError,
    ProviderBackfillEvidenceSource,
    collect_initial_backfill_evidence,
    collect_requested_evidence,
)
from .contracts import EvidenceContractIssue, validate_collected_backfill_facts
from .normalizers import (
    bar_probe_fact_from_result,
    handoff_segment_fact_from_target_valid_event_window,
    identity_scan_fact_from_result,
    omitted_segment_fact_from_absent_reference_and_bars,
    reference_boundary_fact_from_snapshot,
    ticker_replacement_fact_from_target_valid_alias_window,
)

__all__ = [
    "BackfillEvidenceSource",
    "EvidenceCollectionError",
    "EvidenceContractIssue",
    "ProviderBackfillEvidenceSource",
    "collect_initial_backfill_evidence",
    "collect_requested_evidence",
    "bar_probe_fact_from_result",
    "handoff_segment_fact_from_target_valid_event_window",
    "identity_scan_fact_from_result",
    "omitted_segment_fact_from_absent_reference_and_bars",
    "reference_boundary_fact_from_snapshot",
    "ticker_replacement_fact_from_target_valid_alias_window",
    "validate_collected_backfill_facts",
]
