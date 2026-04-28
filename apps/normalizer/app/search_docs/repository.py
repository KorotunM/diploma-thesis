from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from apps.normalizer.app.persistence import json_from_db, sql_text

from .models import UniversitySearchDocRecord


class UniversitySearchDocProjectionRepository:
    def __init__(
        self,
        session: Any,
        *,
        sql_text: Callable[[str], Any] = sql_text,
    ) -> None:
        self._session = session
        self._sql_text = sql_text

    def upsert_search_doc(
        self,
        *,
        search_doc: UniversitySearchDocRecord,
        search_text_source: str,
    ) -> UniversitySearchDocRecord:
        result = self._session.execute(
            self._sql_text(
                """
                INSERT INTO delivery.university_search_doc (
                    university_id,
                    card_version,
                    canonical_name,
                    canonical_name_normalized,
                    website_url,
                    website_domain,
                    country_code,
                    city_name,
                    aliases,
                    search_document,
                    search_text,
                    metadata,
                    generated_at
                )
                VALUES (
                    :university_id,
                    :card_version,
                    :canonical_name,
                    :canonical_name_normalized,
                    :website_url,
                    :website_domain,
                    :country_code,
                    :city_name,
                    :aliases,
                    CAST(:search_document AS jsonb),
                    to_tsvector('simple', :search_text_source),
                    CAST(:metadata AS jsonb),
                    :generated_at
                )
                ON CONFLICT (university_id, card_version)
                DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    canonical_name_normalized = EXCLUDED.canonical_name_normalized,
                    website_url = EXCLUDED.website_url,
                    website_domain = EXCLUDED.website_domain,
                    country_code = EXCLUDED.country_code,
                    city_name = EXCLUDED.city_name,
                    aliases = EXCLUDED.aliases,
                    search_document = EXCLUDED.search_document,
                    search_text = EXCLUDED.search_text,
                    metadata = EXCLUDED.metadata,
                    generated_at = EXCLUDED.generated_at
                RETURNING
                    university_id,
                    card_version,
                    canonical_name,
                    canonical_name_normalized,
                    website_url,
                    website_domain,
                    country_code,
                    city_name,
                    aliases,
                    search_document,
                    metadata,
                    generated_at
                """
            ),
            {
                "university_id": search_doc.university_id,
                "card_version": search_doc.card_version,
                "canonical_name": search_doc.canonical_name,
                "canonical_name_normalized": search_doc.canonical_name_normalized,
                "website_url": search_doc.website_url,
                "website_domain": search_doc.website_domain,
                "country_code": search_doc.country_code,
                "city_name": search_doc.city_name,
                "aliases": list(search_doc.aliases),
                "search_document": json.dumps(
                    search_doc.search_document,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "search_text_source": search_text_source,
                "metadata": json.dumps(
                    search_doc.metadata,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "generated_at": search_doc.generated_at,
            },
        )
        return self._search_doc_from_row(result.mappings().one())

    @staticmethod
    def _search_doc_from_row(row: Any) -> UniversitySearchDocRecord:
        return UniversitySearchDocRecord(
            university_id=row["university_id"],
            card_version=row["card_version"],
            canonical_name=row["canonical_name"],
            canonical_name_normalized=row["canonical_name_normalized"],
            website_url=row["website_url"],
            website_domain=row["website_domain"],
            country_code=row["country_code"],
            city_name=row["city_name"],
            aliases=list(row["aliases"] or []),
            search_document=json_from_db(row["search_document"]),
            generated_at=row["generated_at"],
            metadata=json_from_db(row["metadata"]),
        )
