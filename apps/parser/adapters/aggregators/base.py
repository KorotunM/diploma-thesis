from __future__ import annotations

from abc import ABC, abstractmethod

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact


class AggregatorFragmentExtractor(ABC):
    supported_parser_profiles: tuple[str, ...] = ()

    def can_handle(self, context: FetchContext) -> bool:
        return context.parser_profile in self.supported_parser_profiles

    @abstractmethod
    def extract(
        self,
        *,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> list[ExtractedFragment]:
        raise NotImplementedError
