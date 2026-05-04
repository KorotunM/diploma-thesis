.PHONY: up down down-clean logs ps seed bootstrap test test-unit test-int test-e2e demo lint format migrate migrate-fresh shell-postgres

COMPOSE := docker compose -f infra/docker-compose/docker-compose.yml

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

down-clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

bootstrap:
	python -m scripts.source_bootstrap

seed:
	python -m scripts.seed_demo_data

demo: up
	@echo "Waiting 20s for services to settle..."
	@python -c "import time; time.sleep(20)"
	$(MAKE) bootstrap
	$(MAKE) seed

test: test-unit test-int

test-unit:
	pytest tests/unit -q

test-int:
	pytest tests/integration -q

test-e2e:
	pytest tests/e2e -q

lint:
	ruff check .

format:
	ruff format .

migrate:
	alembic upgrade head

migrate-fresh:
	alembic downgrade base
	alembic upgrade head

shell-postgres:
	$(COMPOSE) exec postgres psql -U aggregator -d aggregator
