# E2E Tests

Здесь лежат пользовательские сценарии для финальной проверки MVP.

## Что покрыто

- `test_search_to_card_to_evidence_happy_path.py`
  - in-memory read path `normalize -> backend -> provenance`
- `test_compose_demo_smoke.py`
  - compose-up smoke для frontend shell и backend API
- `test_live_mvp_bootstrap_to_provenance_flow.py`
  - live MVP path:
    - source bootstrap
    - Tabiturient discovery
    - manual crawl publish
    - parser consume
    - normalize consume
    - backend search/card/provenance

## Запуск

Точечный live-flow тест:

```powershell
py -3 -m pytest tests/e2e/test_live_mvp_bootstrap_to_provenance_flow.py -q
```

Локальный smoke runner:

```powershell
py -3 -m tests.e2e.compose_demo_smoke
```

## Замечание

`compose_demo_smoke` по-прежнему проверяет в первую очередь already-running stack и read surfaces.

Новый commit 19 e2e тест нужен именно для того, чтобы отдельно зафиксировать live runtime chain от source bootstrap и discovery до provenance без ручного replay.
