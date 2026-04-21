from __future__ import annotations

from libs.source_sdk import ParserExecutionResult

from .models import ExtractedFragmentRecord, ParsedDocumentRecord
from .repository import ParsedDocumentRepository


class ParsedDocumentPersistenceService:
    def __init__(self, repository: ParsedDocumentRepository) -> None:
        self._repository = repository

    def persist_successful_execution(
        self,
        *,
        execution_result: ParserExecutionResult,
    ) -> tuple[ParsedDocumentRecord, list[ExtractedFragmentRecord]]:
        document = self._repository.upsert_document(execution_result=execution_result)
        fragments = self._repository.upsert_fragments(
            parsed_document=document,
            fragments=execution_result.fragments,
        )
        self._repository.commit()
        return document, fragments
