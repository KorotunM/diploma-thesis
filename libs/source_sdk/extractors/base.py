from abc import ABC, abstractmethod
from typing import Sequence

from libs.source_sdk.base_adapter import ExtractedFragment, FetchedArtifact


class FragmentExtractor(ABC):
    @abstractmethod
    async def extract(self, artifact: FetchedArtifact) -> Sequence[ExtractedFragment]:
        raise NotImplementedError
