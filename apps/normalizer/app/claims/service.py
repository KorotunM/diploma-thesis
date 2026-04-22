from __future__ import annotations

from libs.contracts.events import NormalizeRequestPayload

from .models import ClaimBuildResult, ParsedDocumentSnapshot
from .repository import ClaimBuildRepository


class ClaimBuildError(ValueError):
    pass


class ClaimBuildService:
    def __init__(self, repository: ClaimBuildRepository) -> None:
        self._repository = repository

    def build_claims_from_extracted_fragments(
        self,
        payload: NormalizeRequestPayload,
    ) -> ClaimBuildResult:
        parsed_document = self._repository.get_parsed_document(
            payload.parsed_document_id
        )
        if parsed_document is None:
            raise ClaimBuildError(
                f"Parsed document {payload.parsed_document_id} was not found."
            )
        self._validate_request(payload=payload, parsed_document=parsed_document)

        fragments = self._repository.list_extracted_fragments(
            payload.parsed_document_id
        )
        claims = self._repository.upsert_claims_from_fragments(
            parsed_document=parsed_document,
            fragments=fragments,
            normalizer_version=payload.normalizer_version,
        )
        self._repository.commit()
        return ClaimBuildResult(parsed_document=parsed_document, claims=claims)

    @staticmethod
    def _validate_request(
        *,
        payload: NormalizeRequestPayload,
        parsed_document: ParsedDocumentSnapshot,
    ) -> None:
        if payload.source_key != parsed_document.source_key:
            raise ClaimBuildError(
                "Normalize request source_key does not match parsed document source_key."
            )
        if payload.parser_version != parsed_document.parser_version:
            raise ClaimBuildError(
                "Normalize request parser_version does not match parsed document parser_version."
            )
