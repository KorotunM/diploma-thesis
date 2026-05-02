from __future__ import annotations

from dataclasses import dataclass

from apps.normalizer.app.cards import UniversityCardProjectionResult
from apps.normalizer.app.card_updated import CardUpdatedEmission
from apps.normalizer.app.claims import ClaimBuildResult
from apps.normalizer.app.facts import ResolvedFactBuildResult
from apps.normalizer.app.universities import UniversityBootstrapResult
from libs.contracts.events import NormalizeRequestEvent


@dataclass(frozen=True)
class ParseCompletedProcessingResult:
    normalize_request: NormalizeRequestEvent
    claim_result: ClaimBuildResult
    bootstrap_result: UniversityBootstrapResult
    fact_result: ResolvedFactBuildResult
    projection_result: UniversityCardProjectionResult
    card_updated: CardUpdatedEmission | None = None
