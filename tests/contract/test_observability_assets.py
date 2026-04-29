from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_prometheus_and_grafana_assets_are_wired_for_pipeline_health() -> None:
    prometheus_config = (ROOT / "infra" / "prometheus" / "prometheus.yml").read_text(
        encoding="utf-8"
    )
    prometheus_rules = (
        ROOT / "infra" / "prometheus" / "rules" / "pipeline-health.yml"
    ).read_text(encoding="utf-8")
    compose = (ROOT / "infra" / "docker-compose" / "docker-compose.yml").read_text(
        encoding="utf-8"
    )
    grafana_datasource = (
        ROOT / "infra" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    ).read_text(encoding="utf-8")
    grafana_dashboards = (
        ROOT / "infra" / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
    ).read_text(encoding="utf-8")

    assert "/etc/prometheus/rules/*.yml" in prometheus_config
    assert "pipeline:crawl_jobs_published:rate5m" in prometheus_rules
    assert "pipeline:parse_duration_seconds:p95" in prometheus_rules
    assert "../prometheus/rules:/etc/prometheus/rules:ro" in compose
    assert "../grafana/provisioning:/etc/grafana/provisioning:ro" in compose
    assert "url: http://prometheus:9090" in grafana_datasource
    assert "folder: Pipeline Health" in grafana_dashboards


def test_pipeline_health_dashboard_targets_domain_metrics() -> None:
    dashboard = json.loads(
        (
            ROOT
            / "infra"
            / "grafana"
            / "dashboards"
            / "pipeline-health-overview.json"
        ).read_text(encoding="utf-8")
    )

    assert dashboard["title"] == "Pipeline Health Overview"
    panel_titles = {panel["title"] for panel in dashboard["panels"]}
    assert "Crawl Jobs Published Rate" in panel_titles
    assert "Parse Runs Rate" in panel_titles
    assert "Normalize Runs Rate" in panel_titles
    assert "Card Builds Rate" in panel_titles

    panel_exprs = {
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    }
    assert any("pipeline:crawl_jobs_published:rate5m" in expr for expr in panel_exprs)
    assert any("pipeline:parse_runs:rate5m" in expr for expr in panel_exprs)
    assert any("pipeline:normalize_runs:rate5m" in expr for expr in panel_exprs)
    assert any("pipeline:card_builds:rate5m" in expr for expr in panel_exprs)
    assert any("pipeline_parse_fragments_per_run_sum" in expr for expr in panel_exprs)
