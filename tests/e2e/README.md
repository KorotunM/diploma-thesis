# E2E Tests

Здесь лежат пользовательские сценарии для поиска, карточки вуза и просмотра provenance.

Локальный compose-up smoke script:

```powershell
py -3 -m tests.e2e.compose_demo_smoke
```

Smoke script ожидает, что:

- локальный stack уже поднят через `docker compose`;
- MVP bundle уже импортирован через `scripts.backfill`;
- backend search возвращает хотя бы один seeded университет.
