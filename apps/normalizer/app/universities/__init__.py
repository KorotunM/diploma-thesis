from .models import (
    SourceAuthorityRecord,
    UniversityBootstrapCandidate,
    UniversityBootstrapResult,
    UniversityRecord,
    UniversitySimilarityCandidate,
)
from .repository import (
    UniversityBootstrapRepository,
    deterministic_university_id,
)

__all__ = [
    "SourceAuthorityRecord",
    "UniversityBootstrapCandidate",
    "UniversityBootstrapRepository",
    "UniversityBootstrapResult",
    "UniversityRecord",
    "UniversitySimilarityCandidate",
    "deterministic_university_id",
    "UniversityBootstrapError",
    "UniversityBootstrapService",
]


def __getattr__(name: str):
    if name == "UniversityBootstrapError":
        from .service import UniversityBootstrapError

        return UniversityBootstrapError
    if name == "UniversityBootstrapService":
        from .service import UniversityBootstrapService

        return UniversityBootstrapService
    raise AttributeError(name)
