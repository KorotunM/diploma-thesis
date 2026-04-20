from __future__ import annotations

from libs.source_sdk import FetchContext, FetchedArtifact, RawArtifactStore

from .models import RawArtifactRecord
from .repository import RawArtifactRepository


class RawArtifactPersistenceService:
    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        repository: RawArtifactRepository,
    ) -> None:
        self._raw_store = raw_store
        self._repository = repository

    async def persist_after_successful_fetch(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> tuple[FetchedArtifact, RawArtifactRecord]:
        stored_artifact = await self._raw_store.store_raw(context, artifact)
        record = self._repository.upsert_from_artifact(
            context=context,
            artifact=stored_artifact,
        )
        self._repository.commit()
        return stored_artifact, record
