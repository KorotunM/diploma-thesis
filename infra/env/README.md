# Environment Layout

`infra/env` хранит воспроизводимые конфигурации окружений для локального запуска и Docker Compose.

Правила:

- `app.env` содержит общие настройки платформы и подключения к общим инфраструктурным сервисам;
- `<service>.env` опционален и нужен только для service-specific override;
- значения в реальных переменных окружения имеют приоритет над файлами;
- `PlatformSettings.load(service_name=...)` сначала читает `infra/env/<APP_ENV>/app.env`, затем `infra/env/<APP_ENV>/<service>.env`.

Коммитить сюда можно только не-секретные bootstrap-значения для локального development-окружения.
