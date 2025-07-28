# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing & Quality
- Run full test suite: `make test` (runs ruff check, ruff format, and pytest - also used by pre-commit hooks)
- Format code: `python3 -m ruff format`
- Lint code: `python3 -m ruff check --fix`
- Individual test: `python3 -m pytest path/to/test_file.py::test_function`

### Docker Operations
- Start all services: `make install` or `docker-compose up -d --build`
- Stop and clean: `make clean` (stops containers, removes networks, and prunes Docker system)
- Restart services: `make restart`
- Rebuild single service: `docker-compose up -d --build --no-deps <service_name>`

### Database Operations
- Complete database reset (development only):
  1. `make clean`
  2. `docker volume rm 3t_mariadb_data`
  3. `make install`

### Documentation
- Render PlantUML diagrams: `make render-diagrams`

## Architecture Overview

3T is a microservices-based automated trading platform with the following key components:

### Core Services
- **Celery Workers** (`celery-services/`): Asynchronous task processing for market data fetching and balance updates
- **Components** (`components/`): Real-time services including:
  - Balance update consumers (`example_balance_consumer.py`)
  - Price streaming producer (`price_stream_producer.py`) - WebSocket connection to HyperLiquid
  - Price streaming consumer (`price_stream_consumer.py`) - Example consumer for price updates
- **Shared** (`shared/`): Common utilities including config management, Celery app, HyperLiquid client, and OpenTelemetry setup

### Infrastructure Services
- **Redis**: Message broker for Celery and streams for real-time events
- **MariaDB**: Persistent storage for trading data, positions, and balance history
- **Prometheus + Grafana**: Metrics collection and visualization  
- **Jaeger + OTEL Collector**: Distributed tracing
- **Flower**: Celery monitoring dashboard

### Configuration Management
- Application config: `config.yml` - contains database, Redis, Celery schedules, and market data settings
- Secrets: `secrets.yml` - contains exchange API keys and passwords (copy from `secrets.yml.example`)
- Both files are mounted into all containers and accessed via the singleton `Config` class in `shared/config.py`

### Key Data Flow
1. **Market Data**: Celery beat scheduler triggers market data fetching tasks that pull OHLCV data from exchanges and store in `market_data` table (MEMORY engine)
2. **Balance Updates**: Scheduled balance fetch tasks update positions and publish events to Redis streams
3. **Real-time Price Streaming**: WebSocket connection to HyperLiquid streams live price updates to Redis for all supported instruments
4. **Event Processing**: Component services consume Redis stream events for real-time processing

### Database Schema
- `exchanges`: Exchange definitions (currently HyperLiquid)
- `instruments`: Trading instruments (BTC, ETH, etc.)
- `products`: Exchange-specific trading pairs with leverage limits
- `positions`: Current trading positions
- `balance_history`: Account balance over time
- `market_data`: OHLCV candlestick data (MEMORY table for performance)

## Development Guidelines

### Code Quality & Testing
- Uses Ruff for formatting and linting with line length of 88 characters
- Test files should follow `test_*.py` naming convention in `components/tests/` and `celery-services/tests/`
- Pre-commit hooks automatically run `make test` - install with `pre-commit install`

### Defensive Programming Principles
- **Fail Fast with Clear Errors**: Validate critical inputs (especially secrets) before use and raise informative exceptions
- **Example**: The `update_balance` task validates all required API secrets before connecting to exchange
- Always check that required configuration values exist and are properly formatted

### Naming Conventions & Import Safety
- **CRITICAL**: Directory names must not conflict with Python library imports
- The current `/celery-services/` avoids conflicts with the `celery` Python package
- Always check PyPI for existing package names before creating new directories
- Test imports from project root to ensure no shadowing occurs

### Secrets Management
- Never commit actual secrets to the repository
- Use `secrets.yml.example` as template for required secrets structure
- Exchange API credentials stored in `secrets.yml` under `exchanges.hyperliquid.*`
- Always validate that secrets exist before attempting to use them

### OpenTelemetry Integration
- All services instrumented with OpenTelemetry for distributed tracing
- Service names configured via `OTEL_SERVICE_NAME` environment variable
- Traces exported to OTEL collector on port 4318

### Celery Best Practices
- Name Beat schedules based on the action they perform (e.g., `update-balance`), not frequency
- Frequencies should be configured in `config.yml`, not hardcoded
- Use `eventlet` concurrency pool for I/O-bound tasks with autoscaling

## Service Access
- Grafana dashboards: http://localhost:3000/dashboards
- Jaeger tracing: http://localhost:16686
- Flower Celery monitoring: http://localhost:5555
- Prometheus metrics: http://localhost:9090

## Task Scheduling
- Celery Beat runs on configurable intervals (default 30 seconds) for balance updates and market data scheduling
- Market data fetching uses stateful Redis tracking to avoid duplicate fetches
- Concurrency controlled via `market_data.concurrency_limit` config (default: 10)

## Troubleshooting

### Common Issues
- **Service inaccessible**: Always run `docker-compose ps` first to check container status
- **Database schema errors**: Usually means `init.sql` wasn't executed - requires database reset (see Database Operations above)
- **ModuleNotFoundError/FileNotFoundError**: Check volume mounts in `docker-compose.yml`

### Production Database Changes
- Never reset database in production - use manual migrations
- Example for adding new products:
  ```bash
  docker exec -i <mariadb_container> mysql -u root -p"$MARIADB_ROOT_PASSWORD" 3t < "INSERT INTO instruments (name) VALUES ('NEW_INSTRUMENT');"
  ```

### Development Notes
- Volume mounts enable live code reloading without rebuilds
- Some services have shared dependencies (e.g., flower needs celery-services requirements)
- When modifying code, rebuild containers to see changes: `docker-compose up -d --build --no-deps <service_name>`