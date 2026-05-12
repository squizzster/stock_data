"""Read-only provider ports and offline fakes."""

from .ports import BackfillFactProvider, BackfillProviderSet
from .models import (
    BarProbeResult,
    HandoffWindow,
    IdentityScanResult,
    OmittedSegmentProbe,
    ReferenceBoundaryProbe,
    ReferenceSnapshot,
    TickerReplacementWindow,
)
from .fake import StaticBackfillFactProvider, StaticProviderReadFactProvider
from .live import (
    HttpJsonResponse,
    MassiveAliasHistoryProvider,
    MassiveBarProbeProvider,
    MassiveCoverageAccountingProvider,
    MassiveIdentityScanProvider,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    MassiveReferenceBoundaryProvider,
    MassiveRequestRecord,
    MassiveTickerEventsProvider,
    MassiveTickerReplacementProvider,
    UrllibJsonTransport,
    massive_read_only_provider_set,
)

__all__ = [
    "BackfillFactProvider",
    "BackfillProviderSet",
    "BarProbeResult",
    "HandoffWindow",
    "HttpJsonResponse",
    "IdentityScanResult",
    "MassiveProviderConfig",
    "MassiveAliasHistoryProvider",
    "MassiveBarProbeProvider",
    "MassiveCoverageAccountingProvider",
    "MassiveIdentityScanProvider",
    "MassiveReadOnlyClient",
    "MassiveReferenceBoundaryProvider",
    "MassiveRequestRecord",
    "MassiveTickerEventsProvider",
    "MassiveTickerReplacementProvider",
    "OmittedSegmentProbe",
    "ReferenceBoundaryProbe",
    "ReferenceSnapshot",
    "StaticBackfillFactProvider",
    "StaticProviderReadFactProvider",
    "TickerReplacementWindow",
    "UrllibJsonTransport",
    "massive_read_only_provider_set",
]
