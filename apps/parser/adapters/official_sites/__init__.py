from .adapter import OfficialSiteAdapter
from .base import OfficialSiteFragmentExtractor
from .html_extractor import OfficialSiteHtmlExtractor
from .kubsu_abiturient_html_extractor import KubSUAbiturientHtmlExtractor

__all__ = [
    "OfficialSiteFragmentExtractor",
    "OfficialSiteAdapter",
    "OfficialSiteHtmlExtractor",
    "KubSUAbiturientHtmlExtractor",
]
