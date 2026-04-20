from .consumer import (
    PARSER_BULK_QUEUE,
    PARSER_CRAWL_REQUEST_QUEUES,
    PARSER_HIGH_QUEUE,
    CrawlRequestConsumer,
    build_crawl_request_consumer,
    build_crawl_request_consumers,
)
from .models import CrawlRequestProcessingResult
from .service import CrawlRequestProcessingService

__all__ = [
    "CrawlRequestConsumer",
    "CrawlRequestProcessingResult",
    "CrawlRequestProcessingService",
    "PARSER_BULK_QUEUE",
    "PARSER_CRAWL_REQUEST_QUEUES",
    "PARSER_HIGH_QUEUE",
    "build_crawl_request_consumer",
    "build_crawl_request_consumers",
]
