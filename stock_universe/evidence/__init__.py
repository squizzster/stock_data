"""Evidence collection and compatibility adapters."""

from .collectors import (
    BackfillEvidenceSource,
    EvidenceCollectionError,
    ProviderBackfillEvidenceSource,
    StaticBackfillEvidenceSource,
    collect_initial_backfill_evidence,
    collect_requested_evidence,
)
from .contracts import EvidenceContractIssue, validate_collected_backfill_facts
from .legacy import facts_from_legacy_plan, ledger_from_legacy_plan
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
    "StaticBackfillEvidenceSource",
    "collect_initial_backfill_evidence",
    "collect_requested_evidence",
    "facts_from_legacy_plan",
    "ledger_from_legacy_plan",
    "bar_probe_fact_from_result",
    "handoff_segment_fact_from_target_valid_event_window",
    "identity_scan_fact_from_result",
    "omitted_segment_fact_from_absent_reference_and_bars",
    "reference_boundary_fact_from_snapshot",
    "ticker_replacement_fact_from_target_valid_alias_window",
    "validate_collected_backfill_facts",
]
