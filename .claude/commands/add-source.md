---
description: Scaffold a new parser source — adds a SourceBlueprint, extractor stubs, and a unit test
---

Scaffold a new parser source for the project.

Arguments expected (parse from $ARGUMENTS — usually free-form):
- `<source_key>` — stable identifier like `nsu-official` (kebab-case, lowercase)
- `<base_url>` — e.g. https://www.nsu.ru
- `<source_type>` — one of `official_site`, `aggregator`, `ranking`
- `<trust_tier>` — one of `authoritative`, `trusted`, `auxiliary`, `experimental`

Steps you must execute (do not ask the user, just do):

1. Add a `SourceBlueprint` to [libs/source_catalog/mvp_live.py](libs/source_catalog/mvp_live.py) with two endpoints — homepage and abiturient page (or sitemap+detail for aggregators).
2. Pick `parser_profile` strings following the `<family>.<short_key>.<page>_html` convention.
3. Create extractor stubs in `apps/parser/adapters/<family>/<short_key>_<page>_html_extractor.py` subclassing `OfficialSiteFragmentExtractor` (or family equivalent). Use `EMAIL_PATTERN` / `PHONE_PATTERN` from `html_extractor.py` for contacts.
4. Register the extractors in `apps/parser/adapters/<family>/adapter.py`:
   - Import the new extractor classes
   - Add the profile strings to `supported_parser_profiles`
   - Add the instances to the default tuple in `_build_extractors`
5. Create a unit test at `tests/unit/adapters/test_<short_key>_<page>_html_extractor.py` mirroring `test_kubsu_abiturient_html_extractor.py`.
6. Run `pytest tests/unit/adapters/test_<short_key>_*.py -q` to verify.
7. Run `python -m scripts.source_bootstrap` to register the source in the DB.

If the new source is an aggregator with a sitemap, follow the `tabiturient_sitemap.py` pattern instead (XML discovery → URL list → per-URL extraction).

After scaffolding, output a summary listing the files you created and the next step (which is filling in real CSS selectors / regex patterns based on the actual site HTML).
