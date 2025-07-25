.PHONY: install test clean restart render-diagrams

install:
	docker-compose up -d --build

test:
	python3 -m ruff check --fix || true
	python3 -m ruff format
	python3 -m pytest -v
	@$(MAKE) render-diagrams || echo "Warning: Failed to render diagrams. Docker may not be available."

render-diagrams:
	@echo "Rendering PlantUML diagrams..."
	@./render_diagrams.sh

clean:
	docker-compose down
	docker system prune -f

restart:
	docker-compose restart
