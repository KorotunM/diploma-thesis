from .adapter import OfficialSiteAdapter
from .base import OfficialSiteFragmentExtractor
from .html_extractor import OfficialSiteHtmlExtractor
from .kubsu_abiturient_html_extractor import KubSUAbiturientHtmlExtractor
from .kubsu_programs_html_extractor import KubSUProgramsHtmlExtractor

__all__ = [
    "OfficialSiteFragmentExtractor",
    "OfficialSiteAdapter",
    "OfficialSiteHtmlExtractor",
    "KubSUAbiturientHtmlExtractor",
    "KubSUProgramsHtmlExtractor",
]
