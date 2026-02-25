# ============================================
# Makefile for Airflow E-Commerce Pipeline
# ============================================

.PHONY: help build up down restart logs shell test lint format clean generate-data

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "  Docker Commands:"
	@echo "    make build       - Build Docker images"
	@echo "    make up          - Start all services"
	@echo "    make down        - Stop all services"
	@echo "    make restart     - Restart all services"
	@echo "    make logs        - View logs (follow mode)"
	@echo "    make shell       - Open shell in scheduler container"
	@echo ""
	@echo "  Development Commands:"
	@echo "    make test        - Run pytest"
	@echo "    make lint        - Run ruff linter"
	@echo "    make format      - Format code with black"
	@echo "    make clean       - Remove generated files"
	@echo ""
	@echo "  Data Commands:"
	@echo "    make generate-data  - Generate sample data"
	@echo ""
	@echo "  Full Workflow:"
	@echo "    make all         - Build, start, and show logs"

# ============================================
# Docker Commands
# ============================================

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

logs-scheduler:
	docker-compose logs -f airflow-scheduler

logs-webserver:
	docker-compose logs -f airflow-webserver

shell:
	docker-compose exec airflow-scheduler bash

shell-db:
	docker-compose exec postgres psql -U airflow -d ecommerce

# ============================================
# Development Commands
# ============================================

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

lint:
	ruff check src/ dags/ tests/

lint-fix:
	ruff check src/ dags/ tests/ --fix

format:
	black src/ dags/ tests/

format-check:
	black --check src/ dags/ tests/

typecheck:
	mypy src/

pre-commit:
	pre-commit run --all-files

# ============================================
# Data Commands
# ============================================

generate-data:
	python src/data_generator.py --output-dir ./data/incoming --customers 1000 --products 500 --orders 10000

generate-data-docker:
	docker-compose exec airflow-scheduler python /opt/airflow/src/data_generator.py

# ============================================
# Cleanup Commands
# ============================================

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete
	find . -type f -name "coverage.xml" -delete

clean-data:
	rm -f data/incoming/*.csv

clean-docker:
	docker-compose down -v
	docker system prune -f

clean-all: clean clean-data clean-docker

# ============================================
# Full Workflow
# ============================================

all: build up
	@echo ""
	@echo "============================================"
	@echo "Airflow is starting..."
	@echo "============================================"
	@echo ""
	@echo "  Web UI:     http://localhost:8080"
	@echo "  Login:      airflow / airflow"
	@echo ""
	@echo "  Run 'make logs' to view logs"
	@echo "============================================"

# ============================================
# CI Commands
# ============================================

ci: lint-fix format test
	@echo "CI checks passed!"

# ============================================
# Setup Commands
# ============================================

setup-venv:
	python -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt

setup-precommit:
	pre-commit install

setup: setup-venv setup-precommit
	cp .env.example .env
	@echo "Setup complete! Edit .env file as needed."
