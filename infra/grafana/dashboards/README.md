# Dashboards

Здесь лежат auto-provisioned Grafana dashboards для локального observability стека.

Стартовый набор:
- `pipeline-health-overview.json` для crawl/parse/normalize/card-build throughput и latency;
- provisioning находится в `infra/grafana/provisioning/`;
- datasource по умолчанию указывает на локальный `prometheus:9090`.
