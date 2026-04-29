from __future__ import annotations

from functools import cache
from typing import Protocol

try:
    from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram
except ModuleNotFoundError:  # pragma: no cover - fallback for stripped environments
    REGISTRY = None
    CollectorRegistry = object
    Counter = None
    Histogram = None


class DomainMetricsCollector(Protocol):
    def record_crawl_job(
        self,
        *,
        status: str,
        trigger_type: str,
        priority: str,
        parser_profile: str,
    ) -> None:
        """Record crawl job publication lifecycle."""

    def record_parse_run(
        self,
        *,
        status: str,
        parser_profile: str,
        parser_version: str,
        fragment_count: int,
        duration_seconds: float,
        parse_completed_emitted: bool,
    ) -> None:
        """Record parser domain execution lifecycle."""

    def record_normalize_run(
        self,
        *,
        status: str,
        parser_version: str,
        normalizer_version: str,
        claim_count: int,
        evidence_count: int,
        resolved_fact_count: int,
        source_count: int,
        rating_fact_count: int,
        duration_seconds: float,
    ) -> None:
        """Record normalizer domain execution lifecycle."""

    def record_card_build(
        self,
        *,
        status: str,
        normalizer_version: str,
        resolved_fact_count: int,
        rating_count: int,
        duration_seconds: float,
        search_doc_refreshed: bool,
    ) -> None:
        """Record card projection build lifecycle."""


class NoopDomainMetricsCollector:
    def record_crawl_job(
        self,
        *,
        status: str,
        trigger_type: str,
        priority: str,
        parser_profile: str,
    ) -> None:
        return None

    def record_parse_run(
        self,
        *,
        status: str,
        parser_profile: str,
        parser_version: str,
        fragment_count: int,
        duration_seconds: float,
        parse_completed_emitted: bool,
    ) -> None:
        return None

    def record_normalize_run(
        self,
        *,
        status: str,
        parser_version: str,
        normalizer_version: str,
        claim_count: int,
        evidence_count: int,
        resolved_fact_count: int,
        source_count: int,
        rating_fact_count: int,
        duration_seconds: float,
    ) -> None:
        return None

    def record_card_build(
        self,
        *,
        status: str,
        normalizer_version: str,
        resolved_fact_count: int,
        rating_count: int,
        duration_seconds: float,
        search_doc_refreshed: bool,
    ) -> None:
        return None


class PrometheusDomainMetricsCollector:
    def __init__(self, *, registry: CollectorRegistry | None = None) -> None:
        if Counter is None or Histogram is None or REGISTRY is None:
            raise RuntimeError(
                "prometheus_client is required to create Prometheus domain metrics."
            )
        resolved_registry = registry or REGISTRY
        self._crawl_jobs_total = Counter(
            "pipeline_crawl_jobs_total",
            "Count of crawl jobs planned and published by the scheduler.",
            labelnames=("status", "trigger_type", "priority", "parser_profile"),
            registry=resolved_registry,
        )
        self._parse_runs_total = Counter(
            "pipeline_parse_runs_total",
            "Count of parser runs that reached raw-only parsed or failed states.",
            labelnames=("status", "parser_profile", "parser_version", "parse_completed_emitted"),
            registry=resolved_registry,
        )
        self._parse_duration_seconds = Histogram(
            "pipeline_parse_duration_seconds",
            "Parser run duration in seconds.",
            labelnames=("parser_profile", "parser_version"),
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
            registry=resolved_registry,
        )
        self._parse_fragments_per_run = Histogram(
            "pipeline_parse_fragments_per_run",
            "Extracted fragment count per parser run.",
            labelnames=("parser_profile", "parser_version"),
            buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128),
            registry=resolved_registry,
        )
        self._normalize_runs_total = Counter(
            "pipeline_normalize_runs_total",
            "Count of normalization runs executed from parsed documents.",
            labelnames=("status", "parser_version", "normalizer_version", "source_count_bucket"),
            registry=resolved_registry,
        )
        self._normalize_duration_seconds = Histogram(
            "pipeline_normalize_duration_seconds",
            "Normalization duration in seconds.",
            labelnames=("parser_version", "normalizer_version"),
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
            registry=resolved_registry,
        )
        self._normalize_claims_per_run = Histogram(
            "pipeline_normalize_claims_per_run",
            "Claim count seen by a normalization run.",
            labelnames=("normalizer_version",),
            buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128, 256),
            registry=resolved_registry,
        )
        self._normalize_evidence_per_run = Histogram(
            "pipeline_normalize_evidence_per_run",
            "Evidence count seen by a normalization run.",
            labelnames=("normalizer_version",),
            buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128, 256),
            registry=resolved_registry,
        )
        self._normalize_resolved_facts_per_run = Histogram(
            "pipeline_normalize_resolved_facts_per_run",
            "Resolved fact count produced by a normalization run.",
            labelnames=("normalizer_version",),
            buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128),
            registry=resolved_registry,
        )
        self._card_builds_total = Counter(
            "pipeline_card_builds_total",
            "Count of delivery card projection builds.",
            labelnames=("status", "normalizer_version", "search_doc_refreshed"),
            registry=resolved_registry,
        )
        self._card_build_duration_seconds = Histogram(
            "pipeline_card_build_duration_seconds",
            "Card build duration in seconds.",
            labelnames=("normalizer_version",),
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
            registry=resolved_registry,
        )
        self._card_build_resolved_facts_per_run = Histogram(
            "pipeline_card_build_resolved_facts_per_run",
            "Resolved fact count projected into one card build.",
            labelnames=("normalizer_version",),
            buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128),
            registry=resolved_registry,
        )
        self._card_build_ratings_per_run = Histogram(
            "pipeline_card_build_ratings_per_run",
            "Rating item count projected into one card build.",
            labelnames=("normalizer_version",),
            buckets=(0, 1, 2, 4, 8, 16, 32),
            registry=resolved_registry,
        )

    def record_crawl_job(
        self,
        *,
        status: str,
        trigger_type: str,
        priority: str,
        parser_profile: str,
    ) -> None:
        self._crawl_jobs_total.labels(
            status=_label(status),
            trigger_type=_label(trigger_type),
            priority=_label(priority),
            parser_profile=_label(parser_profile),
        ).inc()

    def record_parse_run(
        self,
        *,
        status: str,
        parser_profile: str,
        parser_version: str,
        fragment_count: int,
        duration_seconds: float,
        parse_completed_emitted: bool,
    ) -> None:
        labels = {
            "status": _label(status),
            "parser_profile": _label(parser_profile),
            "parser_version": _label(parser_version),
            "parse_completed_emitted": "true" if parse_completed_emitted else "false",
        }
        self._parse_runs_total.labels(**labels).inc()
        self._parse_duration_seconds.labels(
            parser_profile=labels["parser_profile"],
            parser_version=labels["parser_version"],
        ).observe(max(0.0, duration_seconds))
        self._parse_fragments_per_run.labels(
            parser_profile=labels["parser_profile"],
            parser_version=labels["parser_version"],
        ).observe(max(0, fragment_count))

    def record_normalize_run(
        self,
        *,
        status: str,
        parser_version: str,
        normalizer_version: str,
        claim_count: int,
        evidence_count: int,
        resolved_fact_count: int,
        source_count: int,
        rating_fact_count: int,
        duration_seconds: float,
    ) -> None:
        normalized_normalizer_version = _label(normalizer_version)
        labels = {
            "status": _label(status),
            "parser_version": _label(parser_version),
            "normalizer_version": normalized_normalizer_version,
            "source_count_bucket": _source_count_bucket(source_count),
        }
        self._normalize_runs_total.labels(**labels).inc()
        self._normalize_duration_seconds.labels(
            parser_version=labels["parser_version"],
            normalizer_version=normalized_normalizer_version,
        ).observe(max(0.0, duration_seconds))
        self._normalize_claims_per_run.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0, claim_count))
        self._normalize_evidence_per_run.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0, evidence_count))
        self._normalize_resolved_facts_per_run.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0, resolved_fact_count + rating_fact_count))

    def record_card_build(
        self,
        *,
        status: str,
        normalizer_version: str,
        resolved_fact_count: int,
        rating_count: int,
        duration_seconds: float,
        search_doc_refreshed: bool,
    ) -> None:
        normalized_normalizer_version = _label(normalizer_version)
        self._card_builds_total.labels(
            status=_label(status),
            normalizer_version=normalized_normalizer_version,
            search_doc_refreshed="true" if search_doc_refreshed else "false",
        ).inc()
        self._card_build_duration_seconds.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0.0, duration_seconds))
        self._card_build_resolved_facts_per_run.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0, resolved_fact_count))
        self._card_build_ratings_per_run.labels(
            normalizer_version=normalized_normalizer_version
        ).observe(max(0, rating_count))


@cache
def get_domain_metrics() -> DomainMetricsCollector:
    if Counter is None or Histogram is None or REGISTRY is None:
        return NoopDomainMetricsCollector()
    return PrometheusDomainMetricsCollector()


def _label(value: str | None) -> str:
    normalized = (value or "").strip()
    return normalized or "unknown"


def _source_count_bucket(source_count: int) -> str:
    if source_count <= 1:
        return "1"
    if source_count == 2:
        return "2"
    if source_count <= 4:
        return "3_4"
    return "5_plus"
