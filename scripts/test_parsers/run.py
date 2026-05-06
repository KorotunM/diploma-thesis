"""Offline parser smoke-test script.

Runs a named extractor against a local fixture file and prints every
extracted fragment to stdout.  No database, RabbitMQ or MinIO needed.

Usage:
    python -m scripts.test_parsers.run <adapter> <fixture_path> [--url URL]

Available adapters:
    tabiturient.university_html     primary university page (tabiturient)
    tabiturient.about_html          /about/ page (tabiturient)
    tabiturient.proxodnoi_html      /proxodnoi/ page (tabiturient)
    kubsu.abiturient_html           KubGU abiturient page
    kubsu.programs_html             KubGU programs table
    kubsu.places_pdf                KubGU budget-places PDF

Examples:
    python -m scripts.test_parsers.run tabiturient.about_html \\
        tests/fixtures/parser_ingestion/tabiturient_about_page.html

    python -m scripts.test_parsers.run kubsu.programs_html \\
        tests/fixtures/parser_ingestion/kubsu_programs_page.html \\
        --url https://kubsu.ru/priem/programmy

    python -m scripts.test_parsers.run kubsu.places_pdf \\
        tests/fixtures/parser_ingestion/kubsu_places.pdf
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from libs.source_sdk import ExtractedFragment, FetchContext, FetchedArtifact

# ── Adapter registry ──────────────────────────────────────────────────────────

_ADAPTERS: dict[str, tuple[str, str, str, str]] = {
    # adapter_key: (source_key, parser_profile, content_type, endpoint_url_default)
    "tabiturient.university_html": (
        "tabiturient-aggregator",
        "aggregator.tabiturient.university_html",
        "text/html",
        "https://tabiturient.ru/vuzu/test-university",
    ),
    "tabiturient.about_html": (
        "tabiturient-aggregator",
        "aggregator.tabiturient.about_html",
        "text/html",
        "https://tabiturient.ru/vuzu/test-university/about",
    ),
    "tabiturient.proxodnoi_html": (
        "tabiturient-aggregator",
        "aggregator.tabiturient.proxodnoi_html",
        "text/html",
        "https://tabiturient.ru/vuzu/test-university/proxodnoi",
    ),
    "kubsu.abiturient_html": (
        "kubsu-official",
        "official_site.kubsu.abiturient_html",
        "text/html",
        "https://kubsu.ru/priem",
    ),
    "kubsu.programs_html": (
        "kubsu-official",
        "official_site.kubsu.programs_html",
        "text/html",
        "https://kubsu.ru/priem/programmy",
    ),
    "kubsu.places_pdf": (
        "kubsu-official",
        "official_site.kubsu.places_pdf",
        "application/pdf",
        "https://kubsu.ru/priem/mesta.pdf",
    ),
}


def _build_extractor(parser_profile: str):
    if parser_profile == "aggregator.tabiturient.university_html":
        from apps.parser.adapters.aggregators.tabiturient_html_extractor import (
            TabiturientUniversityHtmlExtractor,
        )
        return TabiturientUniversityHtmlExtractor()

    if parser_profile == "aggregator.tabiturient.about_html":
        from apps.parser.adapters.aggregators.tabiturient_about_html_extractor import (
            TabiturientAboutHtmlExtractor,
        )
        return TabiturientAboutHtmlExtractor()

    if parser_profile == "aggregator.tabiturient.proxodnoi_html":
        from apps.parser.adapters.aggregators.tabiturient_proxodnoi_html_extractor import (
            TabiturientProxodnoiHtmlExtractor,
        )
        return TabiturientProxodnoiHtmlExtractor()

    if parser_profile == "official_site.kubsu.abiturient_html":
        from apps.parser.adapters.official_sites.html_extractor import (
            OfficialSiteHtmlExtractor,
        )
        return OfficialSiteHtmlExtractor()

    if parser_profile == "official_site.kubsu.programs_html":
        from apps.parser.adapters.official_sites.kubsu_programs_html_extractor import (
            KubSUProgramsHtmlExtractor,
        )
        return KubSUProgramsHtmlExtractor()

    if parser_profile == "official_site.kubsu.places_pdf":
        from apps.parser.adapters.official_sites.kubsu_places_pdf_extractor import (
            KubSUPlacesPdfExtractor,
        )
        return KubSUPlacesPdfExtractor()

    raise ValueError(f"Unknown parser_profile: {parser_profile}")


def _render_fragment(frag: ExtractedFragment, index: int) -> None:
    value_repr = json.dumps(frag.value, ensure_ascii=False, default=str)
    if len(value_repr) > 200:
        value_repr = value_repr[:197] + "..."
    print(f"  [{index:>3}] field={frag.field_name!r:<35} conf={frag.confidence:.2f}  val={value_repr}")
    if frag.locator:
        print(f"        locator={frag.locator!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.test_parsers.run",
        description="Run an extractor offline against a local fixture file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(f"  {k}" for k in _ADAPTERS),
    )
    parser.add_argument("adapter", choices=list(_ADAPTERS), metavar="ADAPTER",
                        help="Adapter key (see list below)")
    parser.add_argument("fixture", metavar="FIXTURE_PATH",
                        help="Path to fixture file (HTML or PDF)")
    parser.add_argument("--url", metavar="URL",
                        help="Override endpoint URL (optional)")
    args = parser.parse_args(argv)

    fixture_path = Path(args.fixture)
    if not fixture_path.exists():
        print(f"ERROR: fixture file not found: {fixture_path}", file=sys.stderr)
        return 1

    source_key, parser_profile, content_type, default_url = _ADAPTERS[args.adapter]
    endpoint_url = args.url or default_url

    content = fixture_path.read_bytes()

    context = FetchContext(
        crawl_run_id=uuid4(),
        source_key=source_key,
        endpoint_url=endpoint_url,
        parser_profile=parser_profile,
    )
    artifact = FetchedArtifact(
        raw_artifact_id=uuid4(),
        crawl_run_id=context.crawl_run_id,
        source_key=source_key,
        source_url=endpoint_url,
        final_url=endpoint_url,
        http_status=200,
        content_type=content_type,
        content_length=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        fetched_at=datetime.now(UTC),
        render_mode="http",
        content=content,
    )

    extractor = _build_extractor(parser_profile)

    print(f"\nAdapter  : {args.adapter}")
    print(f"Profile  : {parser_profile}")
    print(f"Fixture  : {fixture_path}")
    print(f"URL      : {endpoint_url}")
    print(f"Size     : {len(content):,} bytes")
    print()

    try:
        fragments = extractor.extract(context=context, artifact=artifact)
    except Exception as exc:
        print(f"ERROR during extraction: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    if not fragments:
        print("No fragments extracted — check that the fixture matches the expected HTML structure.")
        return 0

    # Group by field_name for summary
    by_field: dict[str, list[ExtractedFragment]] = {}
    for frag in fragments:
        by_field.setdefault(frag.field_name, []).append(frag)

    print(f"Extracted {len(fragments)} fragment(s) across {len(by_field)} field(s):\n")
    for idx, frag in enumerate(fragments, 1):
        _render_fragment(frag, idx)

    print(f"\n-- Field summary ({len(by_field)} unique fields) --")
    for field, frags in sorted(by_field.items()):
        first = frags[0]
        print(f"  {field:<40} {len(frags):>2}x  conf={first.confidence:.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
