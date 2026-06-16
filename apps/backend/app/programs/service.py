from __future__ import annotations

from .models import ProgramDirectoryItem, ProgramDirectoryResponse
from .repository import ProgramDirectoryRepository


class ProgramDirectoryService:
    def __init__(self, repository: ProgramDirectoryRepository) -> None:
        self._repository = repository

    def list_programs(self) -> ProgramDirectoryResponse:
        rows = self._repository.list_programs()
        items = [
            ProgramDirectoryItem(
                code=row["code"],
                name=row["name"],
                level=row.get("level"),
                description=row.get("description"),
                university_count=int(row.get("university_count") or 0),
                budget_places=int(row.get("budget_places") or 0),
                paid_places=int(row.get("paid_places") or 0),
                avg_passing_score=(
                    round(float(row["avg_passing_score"]), 1)
                    if row.get("avg_passing_score") is not None
                    else None
                ),
                min_tuition_per_year=(
                    int(row["min_tuition_per_year"])
                    if row.get("min_tuition_per_year") is not None
                    else None
                ),
                ege_subjects=sorted(
                    subject
                    for subject in (row.get("ege_subjects") or [])
                    if isinstance(subject, str) and subject
                ),
            )
            for row in rows
        ]
        return ProgramDirectoryResponse(total=len(items), items=items)
