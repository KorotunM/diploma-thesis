from .adapter import AGGREGATOR_ADAPTER_VERSION, AggregatorAdapter
from .payload_extractor import AggregatorPayloadExtractor
from .tabiturient_sitemap import (
    DiscoveredUniversityPage,
    TabiturientSitemapDiscovery,
)

__all__ = [
    "AGGREGATOR_ADAPTER_VERSION",
    "AggregatorAdapter",
    "AggregatorPayloadExtractor",
    "DiscoveredUniversityPage",
    "TabiturientSitemapDiscovery",
]
