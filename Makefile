.PHONY: install test clean

install:
	docker-compose up -d --build

test:
	python3 -m ruff check --fix || true
	python3 -m ruff format
	python3 -m pytest -v

clean:
	docker-compose down
	docker system prune -f