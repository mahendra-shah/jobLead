# Makefile for Placement Dashboard Backend (Docker-based Development)

.PHONY: help setup dev stop logs test clean migrate upgrade downgrade shell

help:
	@echo "Available commands:"
	@echo "  make setup        - Initial setup (build images, download models)"
	@echo "  make dev          - Start all development services"
	@echo "  make stop         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - View logs (all services)"
	@echo "  make test         - Run tests in container"
	@echo "  make clean        - Clean cache and temp files"
	@echo "  make migrate      - Create new migration"
	@echo "  make upgrade      - Apply migrations"
	@echo "  make downgrade    - Rollback last migration"
	@echo "  make shell        - Access backend container shell"
	@echo "  make psql         - Connect to PostgreSQL"

setup:
	@echo "Running setup script..."
	chmod +x setup.sh
	./setup.sh

dev:
	@echo "Starting development services..."
	docker-compose up -d
	@echo "Services started!"
	@echo "API: http://localhost:8000/docs"
	@echo "Flower: http://localhost:5555"

stop:
	@echo "Stopping services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose restart

logs:
	docker-compose logs -f

logs-backend:
	docker-compose logs -f backend

logs-worker:
	docker-compose logs -f celery_worker

test:
	@echo "Running tests..."
	docker-compose run --rm backend pytest -v --cov=app --cov-report=html

clean:
	@echo "Cleaning cache files..."
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	docker-compose down -v

migrate:
	@echo "Creating new migration..."
	@read -p "Enter migration message: " msg; \
	docker-compose run --rm backend alembic revision --autogenerate -m "$$msg"

upgrade:
	@echo "Applying migrations..."
	docker-compose run --rm backend alembic upgrade head

downgrade:
	@echo "Rolling back last migration..."
	docker-compose run --rm backend alembic downgrade -1

shell:
	@echo "Accessing backend container shell..."
	docker-compose exec backend bash

psql:
	@echo "Connecting to PostgreSQL..."
	@echo "Note: Make sure you have psql installed locally"
	@echo "Or use: docker-compose exec backend bash, then use psql from there"
	psql $$DATABASE_URL

build:
	@echo "Building Docker images..."
	docker-compose build

rebuild:
	@echo "Rebuilding Docker images (no cache)..."
	docker-compose build --no-cache

ps:
	@echo "Service status:"
	docker-compose ps

format:
	@echo "Formatting code..."
	docker-compose run --rm backend black app/
	docker-compose run --rm backend isort app/

lint:
	@echo "Linting code..."
	docker-compose run --rm backend ruff check app/

type-check:
	@echo "Type checking..."
	docker-compose run --rm backend mypy app/
