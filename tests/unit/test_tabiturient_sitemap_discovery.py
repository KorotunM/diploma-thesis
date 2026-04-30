from apps.parser.adapters.aggregators.tabiturient_sitemap import (
    TabiturientSitemapDiscovery,
)


def test_tabiturient_sitemap_discovery_keeps_only_primary_university_cards() -> None:
    sitemap = """
    <?xml version="1.0" encoding="utf-8"?>
    <urlset>
      <url>
        <loc>https://tabiturient.ru/vuzu/altgaki</loc>
        <lastmod>2026-04-30T10:55:32+01:00</lastmod>
      </url>
      <url>
        <loc>https://tabiturient.ru/vuzu/altgaki/about</loc>
      </url>
      <url>
        <loc>https://tabiturient.ru/vuzu/altgaki/proxodnoi</loc>
      </url>
      <url>
        <loc>https://tabiturient.ru/vuzu/altgaki/proxodnoi?1020</loc>
      </url>
      <url>
        <loc>https://tabiturient.ru/vuzu/kubsu/</loc>
      </url>
      <url>
        <loc>https://tabiturient.ru/globalrating/</loc>
      </url>
      <url>
        <loc>https://tabiturient.ru/vuzu/kubsu</loc>
      </url>
    </urlset>
    """

    discovery = TabiturientSitemapDiscovery()

    pages = discovery.discover(sitemap)

    assert [page.url for page in pages] == [
        "https://tabiturient.ru/vuzu/altgaki",
        "https://tabiturient.ru/vuzu/kubsu",
    ]
    assert pages[0].last_modified == "2026-04-30T10:55:32+01:00"


def test_tabiturient_sitemap_discovery_accepts_bytes_payload() -> None:
    discovery = TabiturientSitemapDiscovery()

    pages = discovery.discover(
        b"<urlset><url><loc>https://tabiturient.ru/vuzu/altgaki</loc></url></urlset>"
    )

    assert [page.url for page in pages] == ["https://tabiturient.ru/vuzu/altgaki"]
