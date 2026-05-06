# Offline Parser Test Script

Скрипт позволяет прогнать любой экстрактор на локальном HTML/PDF-файле и увидеть все извлечённые поля без запуска Docker-стека.

## Требования

Python 3.12+ с установленными зависимостями проекта. Скрипт запускается **локально**, не в Docker.

```bash
# Из корня репозитория:
pip install -e ".[worker]"
```

Либо внутри контейнера parser, передав fixture-файл через docker cp:

```bash
docker cp tests/fixtures/parser_ingestion/ docker-compose-parser-1:/workspace/tests/fixtures/
docker compose exec parser python -m scripts.test_parsers.run tabiturient.about_html \
  tests/fixtures/parser_ingestion/tabiturient_about_page.html
```

## Использование

```bash
python -m scripts.test_parsers.run <ADAPTER> <FIXTURE_PATH> [--url URL]
```

### Доступные адаптеры

| Ключ | Что парсит |
|------|-----------|
| `tabiturient.university_html` | Главная страница вуза на tabiturient.ru |
| `tabiturient.about_html` | Страница `/about/` вуза на tabiturient.ru |
| `tabiturient.proxodnoi_html` | Страница `/proxodnoi/` (проходные баллы) |
| `kubsu.abiturient_html` | Страница абитуриента на kubsu.ru |
| `kubsu.programs_html` | Таблица программ на kubsu.ru |
| `kubsu.places_pdf` | PDF с бюджетными местами kubsu.ru |

## Примеры

### Tabiturient — страница о вузе (`/about/`)

```bash
python -m scripts.test_parsers.run \
  tabiturient.about_html \
  tests/fixtures/parser_ingestion/tabiturient_about_page.html
```

**Ожидаемые поля:** `canonical_name`, `aliases`, `contacts.logo_url`, `location.city`,
`institutional.type`, `institutional.category`, `institutional.is_flagship`,
`description`, `reviews.rating`, `reviews.rating_count`

### Tabiturient — проходные баллы (`/proxodnoi/`)

```bash
python -m scripts.test_parsers.run \
  tabiturient.proxodnoi_html \
  tests/fixtures/parser_ingestion/tabiturient_proxodnoi_page.html
```

**Ожидаемые поля:** `programs.code`, `programs.name`, `programs.budget_places`,
`programs.passing_score`, `programs.faculty`, `programs.level`, `programs.study_form`

### KubGU — таблица программ

```bash
python -m scripts.test_parsers.run \
  kubsu.programs_html \
  tests/fixtures/parser_ingestion/kubsu_programs_page.html \
  --url https://kubsu.ru/priem/programmy
```

### KubGU — PDF с местами

```bash
python -m scripts.test_parsers.run \
  kubsu.places_pdf \
  tests/fixtures/parser_ingestion/kubsu_places.pdf \
  --url https://kubsu.ru/priem/places_2026.pdf
```

### KubGU — страница абитуриента

```bash
python -m scripts.test_parsers.run \
  kubsu.abiturient_html \
  tests/fixtures/parser_ingestion/kubsu_abiturient_page.html
```

## Как добавить новый фикстур

1. Сохраните HTML-страницу в `tests/fixtures/parser_ingestion/` (например `wget -O fixture.html <url>`)
2. Запустите скрипт с нужным адаптером и путём к файлу
3. Если страница не распознаётся — отладьте regex/CSS-селекторы в соответствующем экстракторе

## Добавление нового адаптера в скрипт

Откройте `scripts/test_parsers/run.py` и добавьте запись в словарь `_ADAPTERS` и ветку в `_build_extractor()`.

## Пример вывода

```
Adapter  : tabiturient.about_html
Profile  : aggregator.tabiturient.about_html
Fixture  : tests/fixtures/parser_ingestion/tabiturient_about_page.html
URL      : https://tabiturient.ru/vuzu/kubsu/about
Size     : 2,341 bytes

Extracted 10 fragment(s) across 9 field(s):

  [  1] field='canonical_name'                  conf=0.95  val="Кубанский государственный университет"
        locator='h1[itemprop="name"]'
  [  2] field='aliases'                          conf=0.88  val=["КубГУ"]
        locator='span[itemprop="alternateName"]'
  [  3] field='contacts.logo_url'               conf=0.99  val="https://tabiturient.ru/logovuz/kubsu.png"
        locator='img[src*='/logovuz/']'
  [  4] field='location.city'                   conf=0.78  val="Краснодар"
        locator='span[itemprop="addressLocality"]'
  ...

── Field summary (9 unique fields) ──
  aliases                                   1x  conf=0.88
  canonical_name                            1x  conf=0.95
  contacts.logo_url                         1x  conf=0.99
  description                               1x  conf=0.80
  institutional.category                    1x  conf=0.85
  institutional.is_flagship                 1x  conf=0.87
  institutional.type                        1x  conf=0.90
  location.city                             1x  conf=0.78
  reviews.rating                            1x  conf=0.92
```
