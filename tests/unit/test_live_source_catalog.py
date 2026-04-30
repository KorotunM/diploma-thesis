from libs.source_catalog import build_live_mvp_source_catalog


def test_live_source_catalog_contains_requested_mvp_sources() -> None:
    catalog = build_live_mvp_source_catalog()
    by_key = {source.source_key: source for source in catalog}

    assert set(by_key) == {
        "kubsu-official",
        "tabiturient-aggregator",
        "tabiturient-globalrating",
    }

    aggregator = by_key["tabiturient-aggregator"]
    assert aggregator.source_type == "aggregator"
    assert aggregator.endpoints[0].endpoint_url == "https://tabiturient.ru/map/sitemap.php"
    assert aggregator.endpoints[0].parser_profile == "aggregator.tabiturient.sitemap_xml"
    assert (
        aggregator.discovery_rules[0].include_url_pattern
        == r"^https://tabiturient\.ru/vuzu/[a-z0-9_-]+/?$"
    )
    assert aggregator.discovery_rules[0].child_parser_profile == (
        "aggregator.tabiturient.university_html"
    )

    ranking = by_key["tabiturient-globalrating"]
    assert ranking.source_type == "ranking"
    assert ranking.endpoints[0].endpoint_url == "https://tabiturient.ru/globalrating/"
    assert ranking.endpoints[0].parser_profile == "ranking.tabiturient.globalrating_html"
    assert ranking.endpoints[0].implementation_status == "planned"

    official = by_key["kubsu-official"]
    assert official.source_type == "official_site"
    assert [endpoint.endpoint_url for endpoint in official.endpoints] == [
        "https://www.kubsu.ru/ru/abiturient",
        "https://www.kubsu.ru/ru/node/44875",
        "https://www.kubsu.ru/sites/default/files/insert/page/2026_places_b_b.pdf",
    ]
    assert official.endpoints[0].implementation_status == "implemented"
    assert official.endpoints[2].content_kind == "pdf"
