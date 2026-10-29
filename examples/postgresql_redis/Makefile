# ========================================
# Dagster AuthKit - PostgreSQL
# ========================================

.PHONY: help up down logs restart shell shell-db clean test-health open init-db

help:
	@echo "╔════════════════════════════════════════════════════════╗"
	@echo "║      Dagster AuthKit - PostgreSQL Backend              ║"
	@echo "╚════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Quick Commands:"
	@echo "  make up              - Start PostgreSQL + Redis + Dagster"
	@echo "  make down            - Stop all containers"
	@echo "  make logs            - Show logs (follow mode)"
	@echo "  make restart         - Restart everything"
	@echo ""
	@echo "Utilities:"
	@echo "  make shell           - Bash in Dagster container"
	@echo "  make shell-db        - psql shell in PostgreSQL"
	@echo "  make init-db         - Initialize database & admin"
	@echo "  make test-health     - Test health endpoint"
	@echo "  make open            - Open Dagster UI"
	@echo "  make clean           - Remove ALL data (DB + volumes)"
	@echo ""

init-db:
	@echo "🔧 Initializing database..."
	@docker-compose exec dagster dagster-authkit-plus init-db --with-admin
	@echo "✅ Database initialized"

up:
	@echo "🚀 Starting PostgreSQL environment..."
	@docker-compose up -d --build
	@echo ""
	@echo "⏳ Waiting for services..."
	@sleep 8
	@echo ""
	@echo "🔧 Initializing database..."
	@$(MAKE) init-db 2>/dev/null || echo "⚠️  Database might already exist"
	@echo ""
	@echo "✅ PostgreSQL Environment Ready!"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  🌐 Dagster UI:   http://localhost:3000"
	@echo "  🗄️  PostgreSQL:   localhost:5432"
	@echo "  💾 Redis:        localhost:6379"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "💡 Tip: Run 'make shell-db' to access PostgreSQL"
	@echo ""

down:
	@echo "🛑 Stopping all containers and removing volumes..."
	@docker-compose down -v
	@echo "✅ Environment cleaned (volumes removed)"

logs:
	@docker-compose logs -f dagster

restart:
	@$(MAKE) down
	@sleep 2
	@$(MAKE) up

shell:
	@echo "🐚 Opening bash in Dagster container..."
	@docker-compose exec dagster bash

shell-db:
	@echo "🗄️  Opening psql shell..."
	@docker-compose exec postgres psql -U dagster_user -d dagster_auth

test-health:
	@echo "🏥 Testing health endpoint..."
	@curl -s http://localhost:3000/auth/health | python -m json.tool

open:
	@command -v open >/dev/null 2>&1 && open http://localhost:3000 || \
	 command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:3000 || \
	 echo "Open manually: http://localhost:3000"

clean:
	@echo "⚠️  WARNING: This will DELETE all PostgreSQL data!"
	@read -p "Are you sure? [y/N]: " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo ""
	@echo "🗑️  Stopping containers and removing volumes..."
	@docker-compose down -v
	@echo "✅ All data removed!"