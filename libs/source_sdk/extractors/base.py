from abc import ABC, abstractmethod
from collections.abc import Sequence

from libs.source_sdk.base_adapter import ExtractedFragment, FetchContext, FetchedArtifact


class FragmentExtractor(ABC):
    @abstractmethod
    async def extract(
        self,
        context: FetchContext,
        artifact: FetchedArtifact,
    ) -> Sequence[ExtractedFragment]:
        raise NotImplementedError
