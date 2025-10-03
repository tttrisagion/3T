.PHONY: install test clean restart render-diagrams

install:
	docker compose up -d --build

test:
	@cleanup() { \
		if [ -f .tmp_config_created ]; then \
			echo "Removing temporary config.yml"; \
			rm -f config.yml .tmp_config_created; \
		fi; \
	}; \
	trap cleanup EXIT; \
	if [ ! -f config.yml ]; then \
		echo "config.yml not found. Creating temporary copy for testing."; \
		cp config.yml.example config.yml; \
		touch .tmp_config_created; \
	fi; \
	python3 -m ruff check --fix || true; \
	python3 -m ruff format; \
	DISABLE_OTEL_EXPORTER=true python3 -m pytest -v

render-diagrams:
	@echo "Rendering PlantUML diagrams..."
	@./render_diagrams.sh

clean:
	docker compose down
	docker system prune -f
	docker volume rm 3t_mariadb_data
	docker volume prune

restart:
	docker compose restart
