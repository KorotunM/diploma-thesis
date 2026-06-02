# Diploma Thesis

Веб-приложение и набор backend-сервисов для агрегации данных о вузах.

Проект собирает данные из внешних источников, сохраняет исходные артефакты, парсит их, нормализует факты и показывает пользователю карточки вузов с поиском и трассировкой источников.

Основной принцип проекта:

```text
raw artifact -> parsed document -> claims -> resolved facts -> delivery projection
```

То есть пользовательская карточка вуза строится не напрямую из HTML/PDF/JSON, а из проверяемой цепочки данных. Для фактов можно посмотреть происхождение: источник, время загрузки, результат парсинга и нормализации.

## Что входит в проект

- `frontend` - пользовательский интерфейс на React/Vite.
- `backend` - публичный API для фронтенда: поиск, карточки вузов, избранное, сравнение, рейтинги.
- `scheduler` - сервис управления источниками и запуском задач сбора данных.
- `parser` - сервис загрузки и разбора страниц/документов.
- `normalizer` - сервис нормализации и сборки итоговых карточек вузов.
- `postgres` - база данных.
- `rabbitmq` - очередь сообщений между сервисами.
- `minio` - S3-совместимое хранилище исходных артефактов.
- `prometheus` и `grafana` - мониторинг.

## Быстрый запуск

Если у вас уже установлены Docker Desktop и Git, выполните из папки проекта:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

После запуска откройте:

- приложение: http://localhost:5173
- backend API: http://localhost:8004/docs
- scheduler API: http://localhost:8001/docs
- RabbitMQ UI: http://localhost:15672
- MinIO Console: http://localhost:9001
- Grafana: http://localhost:3000

Логины и пароли для локального запуска:

| Сервис | Логин | Пароль |
| --- | --- | --- |
| RabbitMQ | `aggregator` | `aggregator` |
| MinIO | `aggregator` | `aggregator-secret` |
| Grafana | `admin` | `admin` |

## Запуск на чистой Windows

Эта инструкция рассчитана на человека, у которого система ещё не настроена для разработки.

### 1. Установите Docker Desktop

1. Откройте https://www.docker.com/products/docker-desktop/.
2. Скачайте Docker Desktop for Windows.
3. Установите программу с настройками по умолчанию.
4. Перезагрузите компьютер, если установщик попросит.
5. Запустите Docker Desktop.
6. Дождитесь статуса `Docker Desktop is running`.

Если Docker Desktop попросит включить WSL 2, согласитесь. Если установка WSL 2 не прошла автоматически, установите его командой в PowerShell от имени администратора:

```powershell
wsl --install
```

После этого перезагрузите компьютер и снова откройте Docker Desktop.

### 2. Установите Git

1. Откройте https://git-scm.com/download/win.
2. Скачайте Git for Windows.
3. Установите с настройками по умолчанию.
4. Откройте PowerShell и проверьте:

```powershell
git --version
```

Если появилась версия Git, всё готово.

### 3. Получите проект

Если проект уже лежит на компьютере, просто перейдите в его папку. Например:

```powershell
cd "C:\Users\miha\Desktop\Vuz\Дипломная работа\diploma-thesis"
```

Если проект нужно скачать из репозитория, используйте:

```powershell
git clone <URL_РЕПОЗИТОРИЯ>
cd diploma-thesis
```

`<URL_РЕПОЗИТОРИЯ>` замените на реальную ссылку на GitHub/GitLab/другой Git-репозиторий.

### 4. Запустите проект

В PowerShell из корневой папки проекта выполните:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

Первый запуск может занять 5-15 минут: Docker скачивает образы, собирает сервисы и устанавливает зависимости.

Команду не закрывайте. Пока это окно PowerShell открыто, проект работает.

### 5. Проверьте, что сервисы запустились

Откройте второе окно PowerShell в той же папке проекта и выполните:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml ps
```

В списке должны быть контейнеры `postgres`, `rabbitmq`, `minio`, `scheduler`, `parser`, `normalizer`, `backend`, `frontend`.

Также можно проверить API:

```powershell
Invoke-RestMethod http://localhost:8001/healthz
Invoke-RestMethod http://localhost:8002/healthz
Invoke-RestMethod http://localhost:8003/healthz
Invoke-RestMethod http://localhost:8004/healthz
```

Если команды возвращают JSON с названием сервиса, значит backend-часть работает.

### 6. Откройте приложение

В браузере откройте:

```text
http://localhost:5173
```

Если страница открылась, фронтенд работает.

### 7. Наполните демо-данными

В `docker-compose.yml` уже есть сервис `auto-seed`, который при запуске пытается зарегистрировать источники и запустить демо-сбор данных. Если в интерфейсе пока нет данных или нужно повторить наполнение вручную, выполните:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml run --rm auto-seed
```

После этого подождите 1-3 минуты: worker-сервисы должны обработать задачи. Затем обновите страницу http://localhost:5173.

## Как остановить проект

В окне, где запущен `docker compose up`, нажмите:

```text
Ctrl + C
```

После остановки можно выполнить:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml down
```

Это остановит контейнеры, но сохранит данные в Docker volumes.

Если нужно полностью очистить локальную базу, очередь, MinIO и Grafana:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml down -v
```

После `down -v` данные будут удалены. Следующий запуск начнётся с пустого состояния.

## Адреса сервисов

| Назначение | Адрес |
| --- | --- |
| Frontend | http://localhost:5173 |
| Backend API | http://localhost:8004 |
| Backend Swagger | http://localhost:8004/docs |
| Scheduler API | http://localhost:8001 |
| Scheduler Swagger | http://localhost:8001/docs |
| Parser API | http://localhost:8002 |
| Normalizer API | http://localhost:8003 |
| RabbitMQ UI | http://localhost:15672 |
| MinIO Console | http://localhost:9001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## Важные локальные настройки

Основной файл окружения:

```text
infra/env/local/app.env
```

Для локального запуска там уже заданы параметры:

| Переменная | Значение |
| --- | --- |
| `POSTGRES_DB` | `aggregator` |
| `POSTGRES_USER` | `aggregator` |
| `POSTGRES_PASSWORD` | `aggregator` |
| `RABBITMQ_DEFAULT_USER` | `aggregator` |
| `RABBITMQ_DEFAULT_PASS` | `aggregator` |
| `MINIO_ROOT_USER` | `aggregator` |
| `MINIO_ROOT_PASSWORD` | `aggregator-secret` |
| `PLATFORM_ADMIN_API_KEY` | `local-dev-admin-key-change-me` |

`PLATFORM_ADMIN_API_KEY` нужен для защищённых admin-endpoint'ов scheduler.

Пример запроса к admin API из PowerShell:

```powershell
$headers = @{
  Authorization = "Bearer local-dev-admin-key-change-me"
}

Invoke-RestMethod `
  -Headers $headers `
  "http://localhost:8001/admin/v1/sources?limit=20&offset=0" |
  ConvertTo-Json -Depth 8
```

## Полезные команды Docker

Посмотреть состояние контейнеров:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml ps
```

Посмотреть логи всех сервисов:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml logs
```

Смотреть логи в реальном времени:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml logs -f
```

Смотреть логи одного сервиса:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml logs -f backend
```

Пересобрать и запустить заново:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

Войти в PostgreSQL:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml exec postgres psql -U aggregator -d aggregator
```

## Режим разработки с автообновлением кода

Обычный запуск использует только `docker-compose.yml`.

Для разработки удобнее подключить `docker-compose.override.yml`: Python-сервисы будут запускаться через `uvicorn --reload`, а фронтенд через Vite dev server.

```powershell
docker compose `
  -f infra/docker-compose/docker-compose.yml `
  -f infra/docker-compose/docker-compose.override.yml `
  up --build
```

Этот режим нужен, если вы меняете код и хотите видеть изменения без полной пересборки контейнеров.

## Локальный запуск без Docker

Этот способ нужен в основном разработчику. Для обычного запуска проекта используйте Docker.

Потребуется установить:

- Python 3.12
- Node.js 22 или новее
- PostgreSQL 16
- RabbitMQ
- MinIO

Установить Python-зависимости:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,worker,parser]"
```

Запустить миграции:

```powershell
alembic upgrade head
```

Запустить backend-сервисы:

```powershell
uvicorn apps.scheduler.app.main:app --reload --port 8001
uvicorn apps.parser.app.main:app --reload --port 8002
uvicorn apps.normalizer.app.main:app --reload --port 8003
uvicorn apps.backend.app.main:app --reload --port 8004
```

Каждую команду нужно запускать в отдельном окне терминала.

Запустить frontend:

```powershell
cd apps/frontend
npm install
npm run dev
```

## Тесты и проверка кода

Для тестов удобнее использовать локальное Python-окружение.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,worker,parser]"
```

Запустить unit-тесты:

```powershell
pytest tests/unit -q
```

Запустить все основные тесты:

```powershell
pytest tests apps -q
```

Проверить стиль кода:

```powershell
ruff check .
```

Отформатировать код:

```powershell
ruff format .
```

Проверить frontend-сборку:

```powershell
cd apps/frontend
npm install
npm run build
```

## Структура проекта

```text
apps/
  backend/       публичный API для пользовательского интерфейса
  frontend/      React-приложение
  normalizer/    нормализация фактов и сборка карточек
  parser/        загрузка и парсинг источников
  scheduler/     источники, задания, discovery и запуск crawl-задач

libs/
  contracts/     общие DTO и event-контракты
  domain/        доменные модели
  observability/ healthz, readyz, metrics
  quality/       регрессии и проверки качества
  source_catalog/ каталог live-источников
  source_sdk/    SDK для адаптеров источников
  storage/       PostgreSQL, RabbitMQ, MinIO

schemas/
  canonical/     JSON Schema для канонических моделей
  events/        JSON Schema для событий
  openapi/       OpenAPI-спеки
  sql/           SQL-схемы

infra/
  docker-compose/ Docker Compose-стек
  env/            локальные переменные окружения
  grafana/        dashboards и provisioning
  minio/          bootstrap bucket'ов
  prometheus/     конфигурация мониторинга

migrations/
  alembic/       миграции базы данных

scripts/
  seed_demo_data/    демо-наполнение
  source_bootstrap/  регистрация live-источников
  replay/            повторный прогон стадий pipeline
  backfill/          backfill данных

tests/
  unit/
  integration/
  e2e/
  contract/
  regression/
```

## Типовые проблемы

### Docker не запускается

Проверьте, что Docker Desktop открыт и имеет статус `Docker Desktop is running`.

Если ошибка связана с WSL 2, выполните PowerShell от имени администратора:

```powershell
wsl --install
```

Затем перезагрузите компьютер.

### Порт уже занят

Проект использует порты `3000`, `5173`, `5432`, `5672`, `8001`, `8002`, `8003`, `8004`, `9000`, `9001`, `9090`, `15672`.

Если один из портов занят, остановите программу, которая его использует, или измените порт в:

```text
infra/docker-compose/docker-compose.yml
```

### Страница открылась, но данных нет

Запустите демо-наполнение:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml run --rm auto-seed
```

Затем подождите несколько минут и обновите страницу.

### Admin API возвращает 401 или 403

Для `/admin/v1/...` нужен заголовок:

```text
Authorization: Bearer local-dev-admin-key-change-me
```

Значение берётся из `infra/env/local/app.env`.

### Нужно начать с чистой базы

Остановите проект и удалите Docker volumes:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml down -v
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

### После изменения кода ничего не поменялось

Если вы используете обычный Docker-запуск, пересоберите контейнеры:

```powershell
docker compose -f infra/docker-compose/docker-compose.yml up --build
```

Для разработки используйте override-режим:

```powershell
docker compose `
  -f infra/docker-compose/docker-compose.yml `
  -f infra/docker-compose/docker-compose.override.yml `
  up --build
```

## Дополнительная документация

- Архитектура: `docs/ARCHITECTURE.md`
- Схема базы данных: `docs/DATABASE_SCHEMA.md`
- MVP runbook: `docs/mvp-demo-runbook.md`
- Паттерны проекта: `docs/PATTERNS.md`
- E2E-тесты: `tests/e2e/README.md`
- Integration-тесты: `tests/integration/README.md`

