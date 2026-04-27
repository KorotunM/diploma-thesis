from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from apps.normalizer.app.claims import (
    ClaimBuildError,
    ClaimBuildRepository,
    ClaimBuildService,
)
from apps.normalizer.app.claims.repository import (
    deterministic_claim_id,
    deterministic_evidence_id,
)
from apps.normalizer.app.persistence import json_from_db, json_to_db
from libs.contracts.events import NormalizeRequestPayload


class MappingResult:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        assert self._row is not None
        return self._row

    def one_or_none(self) -> dict[str, Any] | None:
        return self._row

    def all(self) -> list[dict[str, Any]]:
        return self._rows or []


class FakeClaimBuildSession:
    def __init__(self) -> None:
        self.parsed_document_id = uuid4()
        self.raw_artifact_id = uuid4()
        self.fragment_ids = [uuid4(), uuid4()]
        self.source_url = "https://example.edu/admissions"
        self.created_at = datetime(2026, 4, 22, 9, 0, tzinfo=UTC)
        self.captured_at = datetime(2026, 4, 22, 8, 55, tzinfo=UTC)
        self.parsed_document = {
            "parsed_document_id": self.parsed_document_id,
            "crawl_run_id": uuid4(),
            "raw_artifact_id": self.raw_artifact_id,
            "source_key": "msu-official",
            "parser_profile": "official_site.default",
            "parser_version": "0.1.0",
            "entity_type": "university",
            "entity_hint": "Example University",
            "parsed_at": datetime(2026, 4, 22, 8, 58, tzinfo=UTC),
            "metadata": json_to_db({"adapter_key": "official_sites:0.1.0"}),
        }
        self.fragments = [
            {
                "fragment_id": self.fragment_ids[0],
                "parsed_document_id": self.parsed_document_id,
                "raw_artifact_id": self.raw_artifact_id,
                "source_key": "msu-official",
                "source_url": self.source_url,
                "captured_at": self.captured_at,
                "field_name": "canonical_name",
                "value": json_to_db({"value": "Example University"}),
                "value_type": "str",
                "locator": "h1",
                "confidence": 0.98,
                "metadata": json_to_db({"adapter_key": "official_sites:0.1.0"}),
            },
            {
                "fragment_id": self.fragment_ids[1],
                "parsed_document_id": self.parsed_document_id,
                "raw_artifact_id": self.raw_artifact_id,
                "source_key": "msu-official",
                "source_url": self.source_url,
                "captured_at": self.captured_at,
                "field_name": "contacts.emails",
                "value": json_to_db({"value": ["admissions@example.edu"]}),
                "value_type": "list",
                "locator": "a[href^='mailto:']",
                "confidence": 0.95,
                "metadata": json_to_db({"adapter_key": "official_sites:0.1.0"}),
            },
        ]
        self.claims: dict[Any, dict[str, Any]] = {}
        self.evidence: dict[Any, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "from parsing.parsed_document" in sql:
            return self._select_parsed_document(params)
        if "from parsing.extracted_fragment" in sql:
            return self._select_fragments(params)
        if "insert into normalize.claim_evidence" in sql:
            return MappingResult(row=self._upsert_evidence(params))
        if "insert into normalize.claim " in sql:
            return MappingResult(row=self._upsert_claim(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _select_parsed_document(self, params: dict[str, Any]) -> MappingResult:
        if params["parsed_document_id"] != self.parsed_document_id:
            return MappingResult(row=None)
        return MappingResult(row=self.parsed_document)

    def _select_fragments(self, params: dict[str, Any]) -> MappingResult:
        if params["parsed_document_id"] != self.parsed_document_id:
            return MappingResult(rows=[])
        return MappingResult(rows=self.fragments)

    def _upsert_claim(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.claims.get(params["claim_id"])
        row = {
            **params,
            "created_at": existing["created_at"] if existing else self.created_at,
        }
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        self.claims[params["claim_id"]] = row
        return row

    def _upsert_evidence(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.evidence.get(params["evidence_id"])
        row = dict(params)
        if existing is not None:
            row["metadata"] = json.dumps(
                {
                    **json_from_db(existing["metadata"]),
                    **json_from_db(params["metadata"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        self.evidence[params["evidence_id"]] = row
        return row


def build_payload(session: FakeClaimBuildSession) -> NormalizeRequestPayload:
    return NormalizeRequestPayload(
        crawl_run_id=session.parsed_document["crawl_run_id"],
        source_key="msu-official",
        parsed_document_id=session.parsed_document_id,
        parser_version="0.1.0",
        normalizer_version="normalizer.0.1.0",
    )


def test_claim_build_repository_maps_extracted_fragments_to_claim_records() -> None:
    session = FakeClaimBuildSession()
    repository = ClaimBuildRepository(session=session, sql_text=lambda value: value)
    parsed_document = repository.get_parsed_document(session.parsed_document_id)

    assert parsed_document is not None
    fragments = repository.list_extracted_fragments(session.parsed_document_id)
    claims = repository.upsert_claims_from_fragments(
        parsed_document=parsed_document,
        fragments=fragments,
        normalizer_version="normalizer.0.1.0",
    )

    assert len(claims) == 2
    by_field = {claim.field_name: claim for claim in claims}
    assert by_field["canonical_name"].value == "Example University"
    assert by_field["canonical_name"].value_type == "str"
    assert by_field["canonical_name"].entity_hint == "Example University"
    assert by_field["canonical_name"].parser_confidence == 0.98
    assert by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert by_field["contacts.emails"].value_type == "list"
    assert by_field["contacts.emails"].metadata["raw_artifact_id"] == str(
        session.raw_artifact_id
    )
    assert set(session.claims) == {
        deterministic_claim_id(
            parsed_document_id=session.parsed_document_id,
            fragment_id=fragment_id,
            normalizer_version="normalizer.0.1.0",
        )
        for fragment_id in session.fragment_ids
    }

    evidence = repository.upsert_claim_evidence(claims=claims, fragments=fragments)

    assert len(evidence) == 2
    by_claim_id = {record.claim_id: record for record in evidence}
    canonical_evidence = by_claim_id[by_field["canonical_name"].claim_id]
    assert canonical_evidence.source_key == "msu-official"
    assert canonical_evidence.source_url == session.source_url
    assert canonical_evidence.raw_artifact_id == session.raw_artifact_id
    assert canonical_evidence.fragment_id == session.fragment_ids[0]
    assert canonical_evidence.captured_at == session.captured_at
    assert canonical_evidence.metadata["field_name"] == "canonical_name"
    assert set(session.evidence) == {
        deterministic_evidence_id(
            claim_id=claim.claim_id,
            raw_artifact_id=session.raw_artifact_id,
            fragment_id=claim.metadata["fragment_id"],
        )
        for claim in claims
    }


def test_claim_build_service_builds_claims_idempotently_for_normalize_request() -> None:
    session = FakeClaimBuildSession()
    service = ClaimBuildService(
        ClaimBuildRepository(session=session, sql_text=lambda value: value)
    )
    payload = build_payload(session)

    first = service.build_claims_from_extracted_fragments(payload)
    second = service.build_claims_from_extracted_fragments(payload)

    assert [claim.claim_id for claim in second.claims] == [
        claim.claim_id for claim in first.claims
    ]
    assert [record.evidence_id for record in second.evidence] == [
        record.evidence_id for record in first.evidence
    ]
    assert len(session.claims) == 2
    assert len(session.evidence) == 2
    assert session.commit_count == 2


def test_claim_build_service_rejects_mismatched_parser_version_or_source() -> None:
    session = FakeClaimBuildSession()
    service = ClaimBuildService(
        ClaimBuildRepository(session=session, sql_text=lambda value: value)
    )

    with pytest.raises(ClaimBuildError):
        service.build_claims_from_extracted_fragments(
            build_payload(session).model_copy(update={"parser_version": "0.2.0"})
        )
    with pytest.raises(ClaimBuildError):
        service.build_claims_from_extracted_fragments(
            build_payload(session).model_copy(update={"source_key": "other-source"})
        )


def test_claim_build_service_requires_existing_parsed_document() -> None:
    session = FakeClaimBuildSession()
    service = ClaimBuildService(
        ClaimBuildRepository(session=session, sql_text=lambda value: value)
    )

    with pytest.raises(ClaimBuildError):
        service.build_claims_from_extracted_fragments(
            build_payload(session).model_copy(update={"parsed_document_id": uuid4()})
        )


def test_claim_build_repository_preserves_rating_claim_metadata_and_value_types() -> None:
    session = FakeClaimBuildSession()
    session.fragments.append(
        {
            "fragment_id": uuid4(),
            "parsed_document_id": session.parsed_document_id,
            "raw_artifact_id": session.raw_artifact_id,
            "source_key": "msu-ranking",
            "source_url": "https://rankings.example.com/universities/example-university",
            "captured_at": session.captured_at,
            "field_name": "ratings.year",
            "value": json_to_db({"value": 2026}),
            "value_type": "int",
            "locator": "$.ranking_entry.year",
            "confidence": 0.97,
            "metadata": json_to_db(
                {
                    "adapter_key": "rankings:0.1.0",
                    "adapter_family": "rankings",
                    "rating_item_key": "qs-world:2026:world_overall:example-university",
                    "provider_key": "qs-world",
                    "provider_name": "QS World University Rankings",
                    "source_field": "ranking.year",
                }
            ),
        }
    )
    repository = ClaimBuildRepository(session=session, sql_text=lambda value: value)
    parsed_document = repository.get_parsed_document(session.parsed_document_id)

    assert parsed_document is not None
    claims = repository.upsert_claims_from_fragments(
        parsed_document=parsed_document,
        fragments=repository.list_extracted_fragments(session.parsed_document_id),
        normalizer_version="normalizer.0.1.0",
    )

    rating_claim = next(claim for claim in claims if claim.field_name == "ratings.year")
    assert rating_claim.value == 2026
    assert rating_claim.value_type == "int"
    assert rating_claim.metadata["fragment_metadata"]["rating_item_key"] == (
        "qs-world:2026:world_overall:example-university"
    )
    assert rating_claim.metadata["fragment_metadata"]["provider_name"] == (
        "QS World University Rankings"
    )
