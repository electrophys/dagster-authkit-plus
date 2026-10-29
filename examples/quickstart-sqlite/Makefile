# ========================================
# Dagster AuthKit - SQLite Quickstart
# ========================================

.PHONY: help up down logs restart shell clean test-health test-login open show-password init-db

help:
	@echo "╔════════════════════════════════════════════════════════╗"
	@echo "║      Dagster AuthKit - SQLite Quickstart               ║"
	@echo "╚════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "Quick Commands:"
	@echo "  make up              - Start Dagster (SQLite backend)"
	@echo "  make down            - Stop containers"
	@echo "  make logs            - Show logs (follow mode)"
	@echo "  make restart         - Restart everything"
	@echo ""
	@echo "Utilities:"
	@echo "  make shell           - Open bash in container"
	@echo "  make init-db         - Initialize database & admin user"
	@echo "  make show-password   - Show admin password"
	@echo "  make test-health     - Test health endpoint"
	@echo "  make test-login      - Test login page"
	@echo "  make open            - Open browser to Dagster UI"
	@echo "  make clean           - Remove ALL data (SQLite DB)"
	@echo ""

init-db:
	@echo "🔧 Initializing database..."
	@docker-compose exec dagster dagster-authkit-plus init-db --with-admin
	@echo "✅ Database initialized with admin user"

up:
	@echo "🚀 Starting SQLite Quickstart..."
	@docker-compose up -d --build
	@echo ""
	@echo "⏳ Waiting for container to be ready..."
	@sleep 5
	@echo ""
	@echo "🔧 Initializing database..."
	@$(MAKE) init-db 2>/dev/null || echo "⚠️  Database might already exist"
	@echo ""
	@echo "⏳ Waiting for Dagster to start..."
	@max_attempts=30; \
	attempt=0; \
	while [ $$attempt -lt $$max_attempts ]; do \
		if curl -sf http://localhost:3000/ >/dev/null 2>&1; then \
			echo ""; \
			echo "✅ Dagster AuthKit is running!"; \
			echo ""; \
			echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
			echo "  🌐 URL:    http://localhost:3000"; \
			echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
			echo ""; \
			echo "💡 Tip: Run 'make open' to open browser"; \
			echo "💡 Tip: Change password after first login!"; \
			echo ""; \
			exit 0; \
		fi; \
		printf "."; \
		sleep 2; \
		attempt=$$((attempt + 1)); \
	done; \
	echo ""; \
	echo "❌ Timeout waiting for Dagster to start"; \
	echo "💡 Run 'make logs' to see what happened"; \
	exit 1

down:
	@echo "🛑 Stopping containers and removing volumes..."
	@docker-compose down -v
	@echo "✅ Environment cleaned (volumes removed)"

logs:
	@docker-compose logs -f dagster

restart:
	@echo "🔄 Restarting..."
	@$(MAKE) down
	@sleep 2
	@$(MAKE) up

shell:
	@echo "🐚 Opening bash shell..."
	@docker-compose exec dagster bash

show-password:
	@echo "🔑 Admin Credentials:"
	@echo "  Username: admin"
	@echo "  Password: admin123"

test-health:
	@echo "🏥 Testing health endpoint..."
	@echo ""
	@curl -s http://localhost:3000/auth/health | python -m json.tool || echo "❌ Health check failed"

test-login:
	@echo "🧪 Testing login page..."
	@echo ""
	@curl -sI http://localhost:3000/auth/login | head -n 1

open:
	@echo "🌐 Opening browser..."
	@command -v open >/dev/null 2>&1 && open http://localhost:3000 || \
	 command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:3000 || \
	 echo "❌ Could not detect browser. Open manually: http://localhost:3000"

clean:
	@echo "⚠️  WARNING: This will DELETE the SQLite database!"
	@read -p "Are you sure? [y/N]: " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo ""
	@echo "🗑️  Stopping containers and removing volumes..."
	@docker-compose down -v
	@echo "✅ All data removed!"