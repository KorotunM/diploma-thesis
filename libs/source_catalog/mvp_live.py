from __future__ import annotations

from .models import DiscoveryRule, EndpointBlueprint, SatelliteEndpointSuffix, SourceBlueprint


def build_live_mvp_source_catalog() -> tuple[SourceBlueprint, ...]:
    return (
        SourceBlueprint(
            source_key="tabiturient-aggregator",
            source_type="aggregator",
            trust_tier="trusted",
            notes=(
                "Tabiturient is the secondary university source. Crawl starts from the "
                "public sitemap and only canonical /vuzu/<slug> pages are considered "
                "primary university cards for downstream parsing."
            ),
            endpoints=(
                EndpointBlueprint(
                    endpoint_url="https://tabiturient.ru/map/sitemap.php",
                    parser_profile="aggregator.tabiturient.sitemap_xml",
                    role="discovery",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(),
                    notes=(
                        "Use this sitemap only for endpoint discovery. Do not treat it as "
                        "a normal university entity page. The PHP script returns text/html "
                        "Content-Type even though the body is XML — accept html to allow fetch."
                    ),
                ),
                EndpointBlueprint(
                    endpoint_url="https://tabiturient.ru/vuzu/<slug>",
                    parser_profile="aggregator.tabiturient.university_html",
                    role="entity",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(
                        "canonical_name",
                        "aliases",
                        "contacts.website",
                        "reviews.items",
                        "reviews.rating",
                    ),
                    notes="Primary entity page. Extracts name, website, and reviews.",
                ),
                EndpointBlueprint(
                    endpoint_url="https://tabiturient.ru/vuzu/<slug>/about/",
                    parser_profile="aggregator.tabiturient.about_html",
                    role="entity_detail",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(
                        "canonical_name",
                        "aliases",
                        "contacts.logo_url",
                        "description",
                        "institutional.type",
                        "institutional.category",
                        "institutional.is_flagship",
                        "location.city",
                        "ratings.tabiturient_user",
                        "reviews.rating",
                        "reviews.rating_count",
                    ),
                    notes="About page. Extracts logo, description, category, institutional type, rating.",
                ),
                EndpointBlueprint(
                    endpoint_url="https://tabiturient.ru/vuzu/<slug>/proxodnoi/",
                    parser_profile="aggregator.tabiturient.proxodnoi_html",
                    role="admissions_programs",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(
                        "programs.code",
                        "programs.name",
                        "programs.faculty",
                        "programs.passing_score",
                        "programs.budget_places",
                        "programs.study_form",
                        "programs.level",
                    ),
                    notes="Programs page with passing scores and budget places per program.",
                ),
            ),
            discovery_rules=(
                DiscoveryRule(
                    parent_endpoint_url="https://tabiturient.ru/map/sitemap.php",
                    child_parser_profile="aggregator.tabiturient.university_html",
                    include_url_pattern=r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/?$",
                    exclude_url_patterns=(
                        r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/.+$",
                        r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/.+\?.+$",
                    ),
                    allowed_hosts=("tabiturient.ru", "www.tabiturient.ru"),
                    notes=(
                        "Root university cards discovered from sitemap. "
                        "About and proxodnoi pages are registered as satellite endpoints."
                    ),
                    satellite_suffixes=(
                        SatelliteEndpointSuffix(
                            url_suffix="/about/",
                            parser_profile="aggregator.tabiturient.about_html",
                        ),
                        SatelliteEndpointSuffix(
                            url_suffix="/proxodnoi/",
                            parser_profile="aggregator.tabiturient.proxodnoi_html",
                        ),
                    ),
                ),
            ),
        ),
        SourceBlueprint(
            source_key="tabiturient-globalrating",
            source_type="ranking",
            trust_tier="trusted",
            notes="Tabiturient global rating is the first external ranking provider.",
            endpoints=(
                EndpointBlueprint(
                    endpoint_url="https://tabiturient.ru/globalrating/",
                    parser_profile="ranking.tabiturient.globalrating_html",
                    role="ranking_snapshot",
                    content_kind="html",
                    implementation_status="planned",
                    target_fields=(
                        "canonical_name",
                        "contacts.website",
                        "location.country_code",
                        "ratings.provider",
                        "ratings.year",
                        "ratings.metric",
                        "ratings.value",
                    ),
                    notes=(
                        "The page is fetched as HTML. Ranking row extraction and matching to "
                        "universities will be implemented separately."
                    ),
                ),
            ),
        ),
        SourceBlueprint(
            source_key="kubsu-official",
            source_type="official_site",
            trust_tier="authoritative",
            notes=(
                "KubSU is the first authoritative official source. Multiple official "
                "documents are crawled under one university source."
            ),
            endpoints=(
                EndpointBlueprint(
                    endpoint_url="https://www.kubsu.ru/ru/abiturient",
                    parser_profile="official_site.kubsu.abiturient_html",
                    role="landing_page",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(
                        "canonical_name",
                        "contacts.website",
                        "contacts.emails",
                        "contacts.phones",
                    ),
                    notes=(
                        "Generic official-site HTML extraction can already read basic canonical "
                        "and contact fields from this page."
                    ),
                ),
                EndpointBlueprint(
                    endpoint_url="https://www.kubsu.ru/ru/node/44875",
                    parser_profile="official_site.kubsu.programs_html",
                    role="admissions_programs",
                    content_kind="html",
                    implementation_status="implemented",
                    target_fields=(
                        "programs.name",
                        "programs.faculty",
                        "programs.passing_score",
                        "programs.budget_places",
                    ),
                    notes=(
                        "Structured HTML extraction is active for directions, faculties, "
                        "passing scores and baseline budget-place facts."
                    ),
                ),
                EndpointBlueprint(
                    endpoint_url=(
                        "https://www.kubsu.ru/sites/default/files/insert/page/"
                        "2026_places_b_b.pdf"
                    ),
                    parser_profile="official_site.kubsu.places_pdf",
                    role="budget_places_pdf",
                    content_kind="pdf",
                    implementation_status="implemented",
                    target_fields=("programs.budget_places",),
                    notes=(
                        "Binary PDF source augments admissions rows with budget-place quotas and "
                        "merges them into the authoritative program section."
                    ),
                ),
            ),
        ),
    )
