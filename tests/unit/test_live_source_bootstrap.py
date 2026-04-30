from __future__ import annotations

import json
from uuid import uuid4

from apps.scheduler.app.sources.endpoint_repository import SourceEndpointRepository
from apps.scheduler.app.sources.models import SourceTrustTier, SourceType
from apps.scheduler.app.sources.repository import SourceRepository
from scripts.source_bootstrap.workflow import (
    SchedulerSourceRegistryGateway,
    build_live_mvp_source_seed_specs,
)


class FakeMappingResult:
    def __init__(self, *, row=None, rows=None, scalar=None) -> None:
        self._row = row
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self) -> FakeMappingResult:
        return self

    def one(self):
        return self._row

    def one_or_none(self):
        return self._row

    def all(self):
        return self._rows

    def scalar_one(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._scalar


class FakeBootstrapSession:
    def __init__(self) -> None:
        self.sources: dict[str, dict] = {}
        self.endpoints: dict[object, dict] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict):
        normalized = " ".join(statement.split()).lower()

        if normalized.startswith("select source_id from ingestion.source"):
            source = self.sources.get(params["source_key"])
            return FakeMappingResult(scalar=None if source is None else source["source_id"])

        if normalized.startswith("select") and "from ingestion.source_endpoint" in normalized:
            return FakeMappingResult(row=self._select_endpoint_row(params))

        if normalized.startswith("select") and "from ingestion.source " in normalized:
            return FakeMappingResult(row=self.sources.get(params["source_key"]))

        if normalized.startswith("insert into ingestion.source_endpoint"):
            row = {
                "endpoint_id": params["endpoint_id"],
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "endpoint_url": params["endpoint_url"],
                "parser_profile": params["parser_profile"],
                "crawl_policy": params["crawl_policy"],
            }
            self.endpoints[params["endpoint_id"]] = row
            return FakeMappingResult(row=row)

        if normalized.startswith("update ingestion.source_endpoint"):
            row = self.endpoints.get(params["endpoint_id"])
            if row is None or row["source_key"] != params["source_key"]:
                return FakeMappingResult(row=None)
            if params["endpoint_url"] is not None:
                row["endpoint_url"] = params["endpoint_url"]
            if params["parser_profile"] is not None:
                row["parser_profile"] = params["parser_profile"]
            if params["crawl_policy_is_set"]:
                row["crawl_policy"] = params["crawl_policy"]
            return FakeMappingResult(row=row)

        if normalized.startswith("insert into ingestion.source "):
            row = {
                "source_id": params["source_id"],
                "source_key": params["source_key"],
                "source_type": params["source_type"],
                "trust_tier": params["trust_tier"],
                "is_active": params["is_active"],
                "metadata": params["metadata"],
            }
            self.sources[params["source_key"]] = row
            return FakeMappingResult(row=row)

        if normalized.startswith("update ingestion.source "):
            row = self.sources.get(params["source_key"])
            if row is None:
                return FakeMappingResult(row=None)
            if params["source_type"] is not None:
                row["source_type"] = params["source_type"]
            if params["trust_tier"] is not None:
                row["trust_tier"] = params["trust_tier"]
            if params["is_active"] is not None:
                row["is_active"] = params["is_active"]
            if params["metadata_is_set"]:
                row["metadata"] = params["metadata"]
            return FakeMappingResult(row=row)

        raise AssertionError(f"Unexpected statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _select_endpoint_row(self, params: dict) -> dict | None:
        for row in self.endpoints.values():
            if row["source_key"] != params["source_key"]:
                continue
            if "endpoint_url" in params and row["endpoint_url"] == params["endpoint_url"]:
                return row
            if "endpoint_id" in params and row["endpoint_id"] == params["endpoint_id"]:
                return row
        return None


def json_dict(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def test_live_mvp_source_seed_specs_match_registry_contract() -> None:
    specs = build_live_mvp_source_seed_specs()
    by_key = {spec.source_key: spec for spec in specs}

    assert set(by_key) == {
        "kubsu-official",
        "tabiturient-aggregator",
        "tabiturient-globalrating",
    }

    aggregator = by_key["tabiturient-aggregator"]
    assert aggregator.source_type == SourceType.AGGREGATOR
    assert aggregator.trust_tier == SourceTrustTier.TRUSTED
    assert [endpoint.endpoint_url for endpoint in aggregator.endpoints] == [
        "https://tabiturient.ru/map/sitemap.php"
    ]
    assert aggregator.metadata["seed_kind"] == "live_mvp"
    assert aggregator.metadata["seeded_endpoint_count"] == 1
    assert aggregator.metadata["discovery_rules"][0]["child_parser_profile"] == (
        "aggregator.tabiturient.university_html"
    )

    ranking = by_key["tabiturient-globalrating"]
    assert [endpoint.parser_profile for endpoint in ranking.endpoints] == [
        "ranking.tabiturient.globalrating_html"
    ]
    assert ranking.endpoints[0].crawl_policy.allowed_content_types == ["text/html"]

    official = by_key["kubsu-official"]
    assert official.source_type == SourceType.OFFICIAL_SITE
    assert [endpoint.parser_profile for endpoint in official.endpoints] == [
        "official_site.kubsu.abiturient_html",
        "official_site.kubsu.programs_html",
        "official_site.kubsu.places_pdf",
    ]
    assert official.endpoints[2].crawl_policy.allowed_content_types == ["application/pdf"]


def test_scheduler_source_registry_gateway_upserts_live_sources_and_endpoints() -> None:
    session = FakeBootstrapSession()
    specs = build_live_mvp_source_seed_specs()
    gateway = SchedulerSourceRegistryGateway(
        session=session,
        source_repository=SourceRepository(session, sql_text=lambda value: value),
        endpoint_repository=SourceEndpointRepository(session, sql_text=lambda value: value),
    )

    for spec in specs:
        gateway.ensure_source(spec)
        for endpoint in spec.endpoints:
            gateway.ensure_endpoint(source_key=spec.source_key, spec=endpoint)
    gateway.commit()

    assert session.commit_count == 1
    assert set(session.sources) == {
        "kubsu-official",
        "tabiturient-aggregator",
        "tabiturient-globalrating",
    }
    assert len(session.endpoints) == 5

    kubsu = session.sources["kubsu-official"]
    assert kubsu["source_type"] == "official_site"
    assert kubsu["trust_tier"] == "authoritative"
    assert json_dict(kubsu["metadata"])["seed_kind"] == "live_mvp"

    endpoint_urls = sorted(row["endpoint_url"] for row in session.endpoints.values())
    assert endpoint_urls == [
        "https://tabiturient.ru/globalrating",
        "https://tabiturient.ru/map/sitemap.php",
        "https://www.kubsu.ru/ru/abiturient",
        "https://www.kubsu.ru/ru/node/44875",
        "https://www.kubsu.ru/sites/default/files/insert/page/2026_places_b_b.pdf",
    ]


def test_scheduler_source_registry_gateway_merges_existing_source_metadata_and_updates_endpoint(
) -> None:
    session = FakeBootstrapSession()
    source_id = uuid4()
    endpoint_id = uuid4()
    session.sources["kubsu-official"] = {
        "source_id": source_id,
        "source_key": "kubsu-official",
        "source_type": "aggregator",
        "trust_tier": "experimental",
        "is_active": False,
        "metadata": {"owner": "user", "seed_kind": "stale"},
    }
    session.endpoints[endpoint_id] = {
        "endpoint_id": endpoint_id,
        "source_id": source_id,
        "source_key": "kubsu-official",
        "endpoint_url": "https://www.kubsu.ru/ru/abiturient",
        "parser_profile": "official_site.legacy",
        "crawl_policy": {"timeout_seconds": 30},
    }

    gateway = SchedulerSourceRegistryGateway(
        session=session,
        source_repository=SourceRepository(session, sql_text=lambda value: value),
        endpoint_repository=SourceEndpointRepository(session, sql_text=lambda value: value),
    )
    kubsu_spec = next(
        spec for spec in build_live_mvp_source_seed_specs() if spec.source_key == "kubsu-official"
    )

    gateway.ensure_source(kubsu_spec)
    gateway.ensure_endpoint(source_key=kubsu_spec.source_key, spec=kubsu_spec.endpoints[0])

    updated_source = session.sources["kubsu-official"]
    assert updated_source["source_type"] == "official_site"
    assert updated_source["trust_tier"] == "authoritative"
    assert updated_source["is_active"] is True
    assert json_dict(updated_source["metadata"])["owner"] == "user"
    assert json_dict(updated_source["metadata"])["seed_kind"] == "live_mvp"

    updated_endpoint = session.endpoints[endpoint_id]
    assert updated_endpoint["parser_profile"] == "official_site.kubsu.abiturient_html"
    assert json_dict(updated_endpoint["crawl_policy"])["timeout_seconds"] == 45
