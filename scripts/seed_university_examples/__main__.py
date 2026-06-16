"""Insert official university examples into the local delivery projections."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from apps.backend.app.admin_review import ReviewCaseRepository
from apps.backend.app.persistence import sql_text
from apps.normalizer.app.search_docs import (
    UniversitySearchDocProjectionRepository,
    UniversitySearchDocProjectionService,
)
from apps.scheduler.app.persistence import json_to_db
from libs.domain.university.models import UniversityCard
from libs.storage.postgres.engine import get_postgres_session_factory

from .data import DEMO_UNIVERSITY_CARDS

NORMALIZER_VERSION = "manual-official-examples-v1"
RESOLUTION_POLICY = "manual_seed_authoritative"


def _resolved_fact_id(university_id: UUID, card_version: int, field_name: str) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"https://demo.local/resolved-facts/{university_id}/{card_version}/{field_name}",
    )


def _alias_id(university_id: UUID, alias: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"https://demo.local/aliases/{university_id}/{alias}")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _search_text(card: UniversityCard) -> str:
    terms: list[str] = [
        str(card.canonical_name.value or ""),
        *card.aliases,
        card.location.city or "",
        card.location.region or "",
        card.description or "",
    ]
    for program in card.programs:
        if not isinstance(program, dict):
            continue
        terms.extend(
            str(value)
            for value in (
                program.get("code"),
                program.get("name"),
                program.get("faculty"),
                program.get("description"),
            )
            if value
        )
        terms.extend(str(subject) for subject in program.get("ege_subjects", []) if subject)
    return " ".join(term for term in terms if term)


def _fact_rows(card: UniversityCard) -> Iterable[dict[str, Any]]:
    source = card.sources[0] if card.sources else None
    source_urls = [item.source_url for item in card.sources]
    base_metadata = {
        "source_key": source.source_key if source else None,
        "source_trust_tier": "authoritative",
        "source_urls": source_urls,
        "resolution_strategy": "manual_seed_from_official_sources",
        "selected_claim_ids": [],
        "selected_evidence_ids": [],
    }
    fields: dict[str, Any] = {
        "canonical_name": card.canonical_name.value,
        "description": card.description,
        "history": card.history,
        "location.city": card.location.city,
        "location.region": card.location.region,
        "location.address": card.location.address,
        "contacts.website": card.contacts.website,
        "contacts.emails": card.contacts.emails,
        "contacts.phones": card.contacts.phones,
        "institutional.founded_year": card.institutional.founded_year,
        "stats.avg_passing_score": card.stats.avg_passing_score,
        "stats.budget_places": card.stats.budget_places,
        "dormitory": card.dormitory,
        "military_department": card.military_department,
    }
    for field_name, value in fields.items():
        yield {
            "field_name": field_name,
            "value": value,
            "score": 0.99,
            "metadata": base_metadata,
        }
    for index, program in enumerate(card.programs, start=1):
        if not isinstance(program, dict):
            continue
        field_name = str(program.get("field_name") or f"programs.{index:03d}")
        program_source_urls = [
            str(item.get("source_url"))
            for item in program.get("sources", [])
            if isinstance(item, dict) and item.get("source_url")
        ] or source_urls
        yield {
            "field_name": field_name,
            "value": program,
            "score": float(program.get("confidence") or 0.98),
            "metadata": {
                **base_metadata,
                "source_urls": program_source_urls,
            },
        }


def _upsert_university(session: Any, card: UniversityCard) -> None:
    session.execute(
        sql_text(
            """
            INSERT INTO core.university (
                university_id,
                canonical_name,
                canonical_domain,
                country_code,
                city_name,
                metadata
            )
            VALUES (
                :university_id,
                :canonical_name,
                :canonical_domain,
                :country_code,
                :city_name,
                CAST(:metadata AS jsonb)
            )
            ON CONFLICT (university_id)
            DO UPDATE SET
                canonical_name = EXCLUDED.canonical_name,
                canonical_domain = EXCLUDED.canonical_domain,
                country_code = EXCLUDED.country_code,
                city_name = EXCLUDED.city_name,
                metadata = EXCLUDED.metadata
            """
        ),
        {
            "university_id": card.university_id,
            "canonical_name": str(card.canonical_name.value),
            "canonical_domain": (card.contacts.website or "")
            .removeprefix("https://")
            .removeprefix("www."),
            "country_code": card.location.country,
            "city_name": card.location.city,
            "metadata": _json(
                {
                    "source_type": "official_site",
                    "seed_kind": "official_examples",
                    "source_keys": [source.source_key for source in card.sources],
                }
            ),
        },
    )
    for alias in card.aliases:
        session.execute(
            sql_text(
                """
                INSERT INTO core.university_alias (
                    alias_id,
                    university_id,
                    alias_name,
                    alias_kind
                )
                VALUES (:alias_id, :university_id, :alias_name, 'display')
                ON CONFLICT (alias_id)
                DO UPDATE SET alias_name = EXCLUDED.alias_name
                """
            ),
            {
                "alias_id": _alias_id(card.university_id, alias),
                "university_id": card.university_id,
                "alias_name": alias,
            },
        )


def _upsert_card_version(session: Any, card: UniversityCard) -> None:
    session.execute(
        sql_text(
            """
            INSERT INTO core.card_version (
                university_id,
                card_version,
                normalizer_version,
                generated_at
            )
            VALUES (:university_id, :card_version, :normalizer_version, :generated_at)
            ON CONFLICT (university_id, card_version)
            DO UPDATE SET
                normalizer_version = EXCLUDED.normalizer_version,
                generated_at = EXCLUDED.generated_at
            """
        ),
        {
            "university_id": card.university_id,
            "card_version": card.version.card_version,
            "normalizer_version": NORMALIZER_VERSION,
            "generated_at": card.version.generated_at,
        },
    )


def _upsert_delivery_card(session: Any, card: UniversityCard) -> None:
    session.execute(
        sql_text(
            """
            INSERT INTO delivery.university_card (
                university_id,
                card_version,
                card_json,
                search_text,
                generated_at
            )
            VALUES (
                :university_id,
                :card_version,
                CAST(:card_json AS jsonb),
                to_tsvector('simple', :search_text),
                :generated_at
            )
            ON CONFLICT (university_id, card_version)
            DO UPDATE SET
                card_json = EXCLUDED.card_json,
                search_text = EXCLUDED.search_text,
                generated_at = EXCLUDED.generated_at
            """
        ),
        {
            "university_id": card.university_id,
            "card_version": card.version.card_version,
            "card_json": _json(card.model_dump(mode="json")),
            "search_text": _search_text(card),
            "generated_at": card.version.generated_at,
        },
    )


def _upsert_resolved_facts(session: Any, card: UniversityCard) -> int:
    count = 0
    for row in _fact_rows(card):
        field_name = row["field_name"]
        session.execute(
            sql_text(
                """
                INSERT INTO core.resolved_fact (
                    resolved_fact_id,
                    university_id,
                    field_name,
                    value_json,
                    fact_score,
                    resolution_policy,
                    card_version,
                    resolved_at,
                    metadata
                )
                VALUES (
                    :resolved_fact_id,
                    :university_id,
                    :field_name,
                    CAST(:value_json AS jsonb),
                    :fact_score,
                    :resolution_policy,
                    :card_version,
                    :resolved_at,
                    CAST(:metadata AS jsonb)
                )
                ON CONFLICT (resolved_fact_id)
                DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    fact_score = EXCLUDED.fact_score,
                    resolution_policy = EXCLUDED.resolution_policy,
                    resolved_at = EXCLUDED.resolved_at,
                    metadata = EXCLUDED.metadata
                """
            ),
            {
                "resolved_fact_id": _resolved_fact_id(
                    card.university_id,
                    card.version.card_version,
                    field_name,
                ),
                "university_id": card.university_id,
                "field_name": field_name,
                "value_json": _json(row["value"]),
                "fact_score": row["score"],
                "resolution_policy": RESOLUTION_POLICY,
                "card_version": card.version.card_version,
                "resolved_at": card.version.generated_at,
                "metadata": json_to_db(row["metadata"]),
            },
        )
        count += 1
    return count


def _seed_review_case(session: Any, card: UniversityCard) -> None:
    ReviewCaseRepository(session).upsert_case(
        review_case_id=uuid5(
            NAMESPACE_URL,
            f"https://demo.local/review-cases/{card.university_id}/official-example",
        ),
        reason="demo_official_example_check",
        priority="normal",
        university_id=card.university_id,
        evidence_ids=[],
        metadata={
            "title": "Проверить демо-карточку перед публикацией",
            "summary": (
                "Сгенерированная карточка содержит историю, программы, общежитие "
                "и военную кафедру из официальных источников."
            ),
            "source_keys": [source.source_key for source in card.sources],
            "suggested_action": "accepted",
        },
    )


def seed_examples(
    session: Any, cards: Iterable[UniversityCard] = DEMO_UNIVERSITY_CARDS
) -> dict[str, Any]:
    card_list = list(cards)
    search_service = UniversitySearchDocProjectionService(
        UniversitySearchDocProjectionRepository(session=session)
    )
    seeded: list[dict[str, Any]] = []
    fact_count = 0
    for card in card_list:
        _upsert_university(session, card)
        _upsert_card_version(session, card)
        _upsert_delivery_card(session, card)
        fact_count += _upsert_resolved_facts(session, card)
        search_service.refresh_for_card(card)
        seeded.append(
            {
                "university_id": str(card.university_id),
                "canonical_name": card.canonical_name.value,
                "programs": len(card.programs),
            }
        )
    if card_list:
        _seed_review_case(session, card_list[0])
    return {
        "universities": len(seeded),
        "resolved_facts": fact_count,
        "items": seeded,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.seed_university_examples",
        description="Seed four official demo university cards into Postgres.",
    )
    parser.add_argument(
        "--service-name",
        default="backend",
        help="Service name used for platform settings (default: %(default)s).",
    )
    parser.add_argument(
        "--app-env",
        default=None,
        help="Optional app env override for platform settings.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print validated card JSON and do not write to Postgres.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)

    if args.print_json:
        payload = [card.model_dump(mode="json") for card in DEMO_UNIVERSITY_CARDS]
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    session_factory = get_postgres_session_factory(
        service_name=args.service_name,
        app_env=args.app_env,
    )
    with session_factory() as session:
        summary = seed_examples(session)
        session.commit()
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
