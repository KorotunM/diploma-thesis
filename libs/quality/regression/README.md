# Regression

Здесь живёт regression suite на captured raw pages, чтобы изменение parser/normalizer logic можно было проверять воспроизводимо.

MVP baseline:

- fixture bundle из трёх источников: `official`, `aggregator`, `ranking`;
- проверка integrity manifest `sha256 + content_length`;
- adapter regression на реальных captured payload;
- normalizer regression на authoritative merge и ranking fact resolution.
