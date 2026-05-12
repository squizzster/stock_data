"""Massive read-only provider package."""

from __future__ import annotations

from stock_universe.providers.massive.alias_history import MassiveAliasHistoryProvider
from stock_universe.providers.massive.bar_probe import MassiveBarProbeProvider
from stock_universe.providers.massive.client import (
    HttpJsonResponse,
    HttpJsonTransport,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    MassiveRequestRecord,
    UrllibJsonTransport,
)
from stock_universe.providers.massive.coverage_accounting import (
    MassiveCoverageAccountingProvider,
)
from stock_universe.providers.massive.identity_scan import MassiveIdentityScanProvider
from stock_universe.providers.massive.reference_boundary import (
    MassiveReferenceBoundaryProvider,
)
from stock_universe.providers.massive.ticker_events import MassiveTickerEventsProvider
from stock_universe.providers.massive.ticker_replacement import (
    MassiveTickerReplacementProvider,
)
from stock_universe.providers.ports import BackfillProviderSet


def massive_read_only_provider_set(
    client: MassiveReadOnlyClient,
) -> BackfillProviderSet:
    """Return live read-only providers for adaptive planning dry-runs."""
    return BackfillProviderSet(
        (
            MassiveTickerEventsProvider(client),
            MassiveReferenceBoundaryProvider(client),
            MassiveAliasHistoryProvider(client),
            MassiveTickerReplacementProvider(client),
            MassiveCoverageAccountingProvider(client),
            MassiveBarProbeProvider(client),
            MassiveIdentityScanProvider(client),
        )
    )


__all__ = [
    "HttpJsonResponse",
    "HttpJsonTransport",
    "MassiveAliasHistoryProvider",
    "MassiveBarProbeProvider",
    "MassiveCoverageAccountingProvider",
    "MassiveIdentityScanProvider",
    "MassiveProviderConfig",
    "MassiveReadOnlyClient",
    "MassiveReferenceBoundaryProvider",
    "MassiveRequestRecord",
    "MassiveTickerEventsProvider",
    "MassiveTickerReplacementProvider",
    "UrllibJsonTransport",
    "massive_read_only_provider_set",
]
