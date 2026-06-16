from __future__ import annotations

from uuid import UUID

from .models import ReviewCaseItem, ReviewCaseListResponse, ReviewCaseResolveRequest
from .repository import ReviewCaseRepository


class ReviewCaseNotFoundError(LookupError):
    pass


class ReviewCaseService:
    def __init__(self, repository: ReviewCaseRepository) -> None:
        self._repository = repository

    def list_cases(
        self,
        *,
        status: str | None = "open",
        limit: int = 50,
        offset: int = 0,
    ) -> ReviewCaseListResponse:
        normalized_status = status if status in {"open", "resolved", "dismissed"} else "open"
        total, rows = self._repository.list_cases(
            status=normalized_status,
            limit=max(1, min(limit, 100)),
            offset=max(0, offset),
        )
        return ReviewCaseListResponse(
            total=total,
            items=[ReviewCaseItem.model_validate(row) for row in rows],
        )

    def resolve_case(
        self,
        review_case_id: UUID,
        *,
        user_id: UUID,
        body: ReviewCaseResolveRequest,
    ) -> ReviewCaseItem:
        row = self._repository.resolve_case(
            review_case_id=review_case_id,
            resolved_by=user_id,
            resolution=body.resolution,
            note=body.note,
        )
        if row is None:
            raise ReviewCaseNotFoundError(str(review_case_id))
        return ReviewCaseItem.model_validate(row)
