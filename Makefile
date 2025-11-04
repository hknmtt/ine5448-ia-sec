# ===== VARIABLES =====
PYTHON := python
COMPOSE := docker compose

# ===== TARGETS =====

build:
	docker build -t registry.mth.sh/chatbot:latest .

## 🚀 Start Meilisearch + Chat
up:
	mkdir -p meili_data data/1-raw data/2-processed data/3-out data/4-sync
	$(COMPOSE) up -d meilisearch chat
	@echo "✅ App running at http://localhost:8501"

## 💬 View logs (follow)
logs:
	$(COMPOSE) logs -f

## 🧱 Run the pipeline once (manual indexing)
pipeline:
	$(COMPOSE) run --rm pipeline

## 🧹 Stop and remove containers
down:
	$(COMPOSE) down

## 🧽 Clean local data (use with care)
clean:
	rm -rf meili_data data/2-processed data/3-out data/4-sync || true
	@echo "🧹 Cleaned Meili data and sync cache."

## ℹ️ Help menu
help:
	@echo "Available commands:"
	@echo "  make up         - Start Meili + chat containers"
	@echo "  make down       - Stop all containers"
	@echo "  make logs       - Follow container logs"
	@echo "  make pipeline   - Run pipeline once inside Docker"
	@echo "  make clean      - Delete Meili data + cache files"

.PHONY: up down logs pipeline clean rebuild help
