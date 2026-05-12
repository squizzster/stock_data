"""Live read-only dry-run helpers."""

from __future__ import annotations

from pathlib import Path

from stock_universe.domain import EvidenceFact
from stock_universe.evidence import (
    ProviderBackfillEvidenceSource,
    facts_from_legacy_plan,
)
from stock_universe.providers import (
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    massive_read_only_provider_set,
)


BASE_FACT_KINDS = frozenset(
    {
        "backfill_request",
        "known_aliases",
        "plan_metadata",
        "target_identity",
    }
)


def live_dry_run_base_facts_from_legacy_plan(plan: dict) -> tuple[EvidenceFact, ...]:
    """Extract non-decisional seed facts from a legacy plan fixture/input."""
    facts = facts_from_legacy_plan(plan, include_candidate_segments=False)
    return tuple(fact for fact in facts if fact.kind in BASE_FACT_KINDS)


def massive_live_dry_run_source_from_legacy_plan(
    plan: dict,
    *,
    api_key: str | None = None,
    base_url: str = "https://api.massive.com",
    capture_dir: Path | None = None,
    client: MassiveReadOnlyClient | None = None,
) -> tuple[ProviderBackfillEvidenceSource, MassiveReadOnlyClient]:
    """Build a read-only evidence source and expose its client request log."""
    if client is None:
        if not api_key:
            raise ValueError("api_key is required when client is not provided")
        client = MassiveReadOnlyClient(
            MassiveProviderConfig(api_key=api_key, base_url=base_url),
            raw_capture_dir=capture_dir,
        )
    source = ProviderBackfillEvidenceSource(
        live_dry_run_base_facts_from_legacy_plan(plan),
        massive_read_only_provider_set(client),
    )
    return source, client
