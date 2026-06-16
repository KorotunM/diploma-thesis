# Diploma Thesis

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

### 2. Установите Git

### 3. Перейдите в проект

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

### 6. Откройте приложение

В браузере откройте:

```text
http://localhost:5173
```

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


## Дополнительная документация

- Архитектура: `docs/ARCHITECTURE.md`
- Схема базы данных: `docs/DATABASE_SCHEMA.md`
- Паттерны проекта: `docs/PATTERNS.md`


