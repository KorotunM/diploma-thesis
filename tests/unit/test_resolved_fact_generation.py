from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from apps.normalizer.app.claims import ClaimEvidenceRecord, ClaimRecord
from apps.normalizer.app.facts import (
    CANONICAL_FACT_FIELDS,
    PROGRAM_FIELD_PREFIX,
    RATING_FIELD_PREFIX,
    ResolvedFactGenerationService,
    ResolvedFactRepository,
    deterministic_resolved_fact_id,
)
from apps.normalizer.app.persistence import json_from_db
from apps.normalizer.app.resolution import (
    CANONICAL_FIELD_POLICY,
    RATING_FIELD_POLICY,
    SINGLE_SOURCE_AUTHORITATIVE_POLICY,
    SourceTrustTier,
)
from apps.normalizer.app.universities import (
    SourceAuthorityRecord,
    UniversityBootstrapResult,
    UniversityRecord,
    deterministic_university_id,
)


class MappingResult:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def mappings(self) -> MappingResult:
        return self

    def one(self) -> dict[str, Any]:
        return self._row


class FakeResolvedFactSession:
    def __init__(self) -> None:
        self.resolved_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
        self.facts: dict[UUID, dict[str, Any]] = {}
        self.commit_count = 0

    def execute(self, statement: str, params: dict[str, Any]) -> MappingResult:
        sql = " ".join(statement.split()).lower()
        if "insert into core.resolved_fact" in sql:
            return MappingResult(self._upsert_fact(params))
        raise AssertionError(f"Unexpected SQL statement: {statement}")

    def commit(self) -> None:
        self.commit_count += 1

    def _upsert_fact(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self.facts.get(params["resolved_fact_id"])
        row = {
            **params,
            "resolved_at": self.resolved_at,
        }
        if existing is not None:
            metadata = {
                **json_from_db(existing["metadata"]),
                **json_from_db(params["metadata"]),
            }
            row["metadata"] = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        self.facts[params["resolved_fact_id"]] = row
        return row


def claim(
    *,
    field_name: str,
    value,
    confidence: float,
    claim_id: UUID | None = None,
) -> ClaimRecord:
    value_type = "null"
    if isinstance(value, str):
        value_type = "str"
    elif isinstance(value, int):
        value_type = "int"
    elif isinstance(value, float):
        value_type = "float"
    elif isinstance(value, list):
        value_type = "list"
    elif isinstance(value, dict):
        value_type = "dict"
    return ClaimRecord(
        claim_id=claim_id or uuid4(),
        parsed_document_id=uuid4(),
        source_key="msu-official",
        field_name=field_name,
        value=value,
        value_type=value_type,
        entity_hint="Example University",
        parser_version="0.1.0",
        normalizer_version="normalizer.0.1.0",
        parser_confidence=confidence,
        created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
        metadata={"fragment_id": str(uuid4())},
    )


def evidence_for(claim_record: ClaimRecord) -> ClaimEvidenceRecord:
    return ClaimEvidenceRecord(
        evidence_id=uuid4(),
        claim_id=claim_record.claim_id,
        source_key=claim_record.source_key,
        source_url="https://example.edu/admissions",
        raw_artifact_id=uuid4(),
        fragment_id=uuid4(),
        captured_at=datetime(2026, 4, 23, 8, 55, tzinfo=UTC),
        metadata={"field_name": claim_record.field_name},
    )


def build_bootstrap_result() -> UniversityBootstrapResult:
    university_id = deterministic_university_id("msu-official")
    selected_name = claim(
        field_name="canonical_name",
        value="Example University",
        confidence=0.98,
    )
    lower_confidence_name = claim(
        field_name="canonical_name",
        value="Example University Old Name",
        confidence=0.5,
    )
    website = claim(
        field_name="contacts.website",
        value="https://example.edu",
        confidence=0.9,
    )
    city = claim(field_name="location.city", value="Moscow", confidence=0.88)
    country = claim(field_name="location.country_code", value="RU", confidence=0.8)
    email = claim(
        field_name="contacts.emails",
        value=["admissions@example.edu"],
        confidence=0.9,
    )
    phone = claim(
        field_name="contacts.phones",
        value=["+7 495 000-00-00"],
        confidence=0.89,
    )
    null_field = claim(field_name="location.country_code", value=None, confidence=0.99)
    claims = [
        selected_name,
        lower_confidence_name,
        website,
        city,
        country,
        email,
        phone,
        null_field,
    ]
    return UniversityBootstrapResult(
        source=SourceAuthorityRecord(
            source_id=uuid4(),
            source_key="msu-official",
            source_type="official_site",
            trust_tier=SourceTrustTier.AUTHORITATIVE,
            is_active=True,
        ),
        university=UniversityRecord(
            university_id=university_id,
            canonical_name="Example University",
            canonical_domain="example.edu",
            country_code="RU",
            city_name="Moscow",
            created_at=datetime(2026, 4, 23, 9, 5, tzinfo=UTC),
            metadata={"bootstrap_policy": SINGLE_SOURCE_AUTHORITATIVE_POLICY},
        ),
        claims_used=claims,
        evidence_used=[evidence_for(claim_record) for claim_record in claims],
    )


def build_service(session: FakeResolvedFactSession) -> ResolvedFactGenerationService:
    return ResolvedFactGenerationService(
        ResolvedFactRepository(session=session, sql_text=lambda value: value)
    )


def test_resolved_fact_generation_persists_canonical_fields() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()

    result = service.generate_for_bootstrap(bootstrap_result)

    assert result.university == bootstrap_result.university
    assert {fact.field_name for fact in result.facts} == {
        *CANONICAL_FACT_FIELDS,
        "contacts.emails",
        "contacts.phones",
    }
    by_field = {fact.field_name: fact for fact in result.facts}
    assert by_field["canonical_name"].value == "Example University"
    assert by_field["canonical_name"].fact_score == 0.98
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["location.city"].value == "Moscow"
    assert by_field["location.country_code"].value == "RU"
    assert by_field["contacts.emails"].value == ["admissions@example.edu"]
    assert by_field["contacts.phones"].value == ["+7 495 000-00-00"]
    assert by_field["contacts.website"].resolution_policy == CANONICAL_FIELD_POLICY
    assert by_field["canonical_name"].selected_claim_ids == [
        bootstrap_result.claims_used[0].claim_id
    ]
    assert by_field["canonical_name"].selected_evidence_ids == [
        bootstrap_result.evidence_used[0].evidence_id
    ]
    assert by_field["canonical_name"].metadata["source_key"] == "msu-official"
    assert (
        by_field["canonical_name"].metadata["source_trust_tier"]
        == SourceTrustTier.AUTHORITATIVE.value
    )
    assert by_field["canonical_name"].metadata["field_resolution_policy"] == (
        CANONICAL_FIELD_POLICY
    )
    assert by_field["canonical_name"].metadata["source_urls"] == [
        "https://example.edu/admissions"
    ]
    assert len(session.facts) == 6
    assert session.commit_count == 1


def test_resolved_fact_generation_is_idempotent_by_university_field_and_card_version() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()

    first = service.generate_for_bootstrap(bootstrap_result)
    second = service.generate_for_bootstrap(bootstrap_result)

    assert [fact.resolved_fact_id for fact in second.facts] == [
        fact.resolved_fact_id for fact in first.facts
    ]
    assert len(session.facts) == 6
    assert session.commit_count == 2
    assert set(session.facts) == {
        deterministic_resolved_fact_id(
            university_id=bootstrap_result.university.university_id,
            field_name=field_name,
            card_version=1,
        )
        for field_name in (
            *CANONICAL_FACT_FIELDS,
            "contacts.emails",
            "contacts.phones",
        )
    }


def test_resolved_fact_generation_prefers_authoritative_claims_in_dual_source_merge() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()
    aggregator_city = claim(
        field_name="location.city",
        value="Moscow City",
        confidence=0.99,
    ).model_copy(update={"source_key": "msu-aggregator"})
    aggregator_website = claim(
        field_name="contacts.website",
        value="https://directory.example.edu",
        confidence=0.97,
    ).model_copy(update={"source_key": "msu-aggregator"})
    dual_source = bootstrap_result.model_copy(
        update={
            "sources_used": [
                bootstrap_result.source,
                bootstrap_result.source.model_copy(
                    update={
                        "source_key": "msu-aggregator",
                        "source_type": "aggregator",
                        "trust_tier": SourceTrustTier.TRUSTED,
                    }
                ),
            ],
            "claims_used": [
                *bootstrap_result.claims_used,
                aggregator_city,
                aggregator_website,
            ],
            "evidence_used": [
                *bootstrap_result.evidence_used,
                evidence_for(aggregator_city),
                evidence_for(aggregator_website),
            ],
        }
    )

    result = service.generate_for_bootstrap(dual_source)
    by_field = {fact.field_name: fact for fact in result.facts}

    assert by_field["location.city"].value == "Moscow"
    assert by_field["contacts.website"].value == "https://example.edu"
    assert by_field["location.city"].metadata["source_key"] == "msu-official"
    assert by_field["location.city"].metadata["source_trust_tier"] == (
        SourceTrustTier.AUTHORITATIVE.value
    )


def test_resolved_fact_generation_builds_structured_rating_fact() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()
    rating_item_key = "qs-world:2026:world_overall:example-university"
    rating_source = bootstrap_result.source.model_copy(
        update={
            "source_key": "qs-world-ranking",
            "source_type": "ranking",
            "trust_tier": SourceTrustTier.TRUSTED,
        }
    )
    rating_claims = [
        claim(
            field_name="ratings.provider",
            value="QS World University Rankings",
            confidence=0.98,
        ).model_copy(
            update={
                "source_key": "qs-world-ranking",
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "rating_item_key": rating_item_key,
                        "provider_key": "qs-world",
                        "provider_name": "QS World University Rankings",
                    },
                },
            }
        ),
        claim(
            field_name="ratings.year",
            value=2026,
            confidence=0.97,
        ).model_copy(
            update={
                "source_key": "qs-world-ranking",
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "rating_item_key": rating_item_key,
                        "provider_key": "qs-world",
                        "provider_name": "QS World University Rankings",
                    },
                },
            }
        ),
        claim(
            field_name="ratings.metric",
            value="world_overall",
            confidence=0.96,
        ).model_copy(
            update={
                "source_key": "qs-world-ranking",
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "rating_item_key": rating_item_key,
                        "provider_key": "qs-world",
                        "provider_name": "QS World University Rankings",
                        "scale": "global",
                    },
                },
            }
        ),
        claim(
            field_name="ratings.value",
            value="151",
            confidence=0.95,
        ).model_copy(
            update={
                "source_key": "qs-world-ranking",
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "rating_item_key": rating_item_key,
                        "provider_key": "qs-world",
                        "provider_name": "QS World University Rankings",
                        "rank_display": "#151",
                        "scale": "global",
                    },
                },
            }
        ),
    ]
    enriched_bootstrap = bootstrap_result.model_copy(
        update={
            "sources_used": [bootstrap_result.source, rating_source],
            "claims_used": [*bootstrap_result.claims_used, *rating_claims],
            "evidence_used": [
                *bootstrap_result.evidence_used,
                *[evidence_for(claim_record) for claim_record in rating_claims],
            ],
        }
    )

    result = service.generate_for_bootstrap(enriched_bootstrap)
    by_field = {fact.field_name: fact for fact in result.facts}
    rating_field_name = f"{RATING_FIELD_PREFIX}{rating_item_key}"

    assert rating_field_name in by_field
    rating_fact = by_field[rating_field_name]
    assert rating_fact.value == {
        "provider": "QS World University Rankings",
        "year": 2026,
        "metric": "world_overall",
        "value": "151",
    }
    assert rating_fact.value_type == "rating_item"
    assert rating_fact.resolution_policy == RATING_FIELD_POLICY
    assert rating_fact.metadata["provider_key"] == "qs-world"
    assert rating_fact.metadata["rank_display"] == "#151"
    assert rating_fact.metadata["scale"] == "global"
    assert rating_fact.metadata["source_key"] == "qs-world-ranking"
    assert rating_fact.metadata["source_trust_tier"] == SourceTrustTier.TRUSTED.value
    assert len(rating_fact.selected_claim_ids) == 4
    assert len(rating_fact.selected_evidence_ids) == 4


def test_resolved_fact_generation_builds_structured_program_fact() -> None:
    session = FakeResolvedFactSession()
    service = build_service(session)
    bootstrap_result = build_bootstrap_result()
    program_item_key = "science-faculty:05.03.01:0"
    program_claims = [
        claim(
            field_name="programs.faculty",
            value="Faculty of Science",
            confidence=0.99,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
        claim(
            field_name="programs.code",
            value="05.03.01",
            confidence=0.99,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
        claim(
            field_name="programs.name",
            value="Geology",
            confidence=0.98,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
        claim(
            field_name="programs.budget_places",
            value=25,
            confidence=0.99,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
        claim(
            field_name="programs.passing_score",
            value=182,
            confidence=0.99,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
        claim(
            field_name="programs.year",
            value=2025,
            confidence=0.99,
        ).model_copy(
            update={
                "metadata": {
                    "fragment_id": str(uuid4()),
                    "fragment_metadata": {
                        "record_group_key": program_item_key,
                        "faculty": "Faculty of Science",
                        "program_code": "05.03.01",
                        "program_year": 2025,
                    },
                },
            }
        ),
    ]
    enriched_bootstrap = bootstrap_result.model_copy(
        update={
            "claims_used": [*bootstrap_result.claims_used, *program_claims],
            "evidence_used": [
                *bootstrap_result.evidence_used,
                *[
                    evidence_for(claim_record).model_copy(
                        update={"source_url": "https://example.edu/programs"}
                    )
                    for claim_record in program_claims
                ],
            ],
        }
    )

    result = service.generate_for_bootstrap(enriched_bootstrap)
    by_field = {fact.field_name: fact for fact in result.facts}
    program_field_name = f"{PROGRAM_FIELD_PREFIX}{program_item_key}"

    assert program_field_name in by_field
    program_fact = by_field[program_field_name]
    assert program_fact.value == {
        "faculty": "Faculty of Science",
        "code": "05.03.01",
        "name": "Geology",
        "budget_places": 25,
        "passing_score": 182,
        "year": 2025,
    }
    assert program_fact.value_type == "program_item"
    assert program_fact.metadata["program_item_key"] == program_item_key
    assert program_fact.metadata["faculty"] == "Faculty of Science"
    assert program_fact.metadata["program_code"] == "05.03.01"
    assert program_fact.metadata["program_year"] == 2025
    assert program_fact.metadata["source_key"] == "msu-official"
    assert program_fact.metadata["source_urls"] == ["https://example.edu/programs"]
    assert len(program_fact.selected_claim_ids) == 6
    assert len(program_fact.selected_evidence_ids) == 6
