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
- **Deploy code changes: `docker-compose restart`** (Python files are mounted as volumes - no rebuild needed)
- Restart services: `make restart`
- Rebuild single service: `docker-compose up -d --build --no-deps <service_name>` (only needed for requirements.txt changes)

### Automated Setup
- **Kibana Index Patterns**: Automatically created by `kibana-setup` service on deployment
- **Setup Scripts**: Located in `setup/` directory for automated configuration
- **Fresh Environment**: All necessary index patterns and configurations are created automatically

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
- **Celery Workers** (`celery-services/`): Asynchronous task processing for market data fetching, balance updates, and portfolio reconciliation
  - **Reconciliation Engine** (`reconciliation_engine.py`): The brain of the trading system that continuously compares desired vs actual positions and generates reconciliation orders
- **Components** (`components/`): Real-time services including:
  - Balance update consumers (`example_balance_consumer.py`)
  - Price streaming producer (`price_stream_producer.py`) - WebSocket connection to HyperLiquid
  - Price streaming consumer (`price_stream_consumer.py`) - Example consumer for price updates
- **Shared** (`shared/`): Common utilities including config management, Celery app, HyperLiquid client, and OpenTelemetry setup

### Infrastructure Services
- **Redis**: Message broker for Celery and streams for real-time events
- **MariaDB**: Persistent storage for trading data, positions, and balance history
- **Prometheus + Grafana**: Metrics collection and visualization  
- **Elasticsearch + Jaeger + OTEL Collector**: Distributed tracing with persistent storage
- **Kibana**: Web interface for browsing Elasticsearch/Jaeger trace data
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
5. **Portfolio Reconciliation**: Reconciliation engine compares desired positions (from `runs` table) with actual positions (consensus between local DB and observer nodes) and generates corrective trades via Order Gateway

### Database Schema
- `exchanges`: Exchange definitions (currently HyperLiquid)
- `instruments`: Trading instruments (BTC, ETH, etc.)
- `products`: Exchange-specific trading pairs with leverage limits
- `positions`: Current trading positions
- `balance_history`: Account balance over time
- `market_data`: OHLCV candlestick data (MEMORY table for performance)
- `runs`: Trading run definitions with position directions and PnL for reconciliation engine

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
- For CI/CD: Copy `secrets.yml.example` which contains safe mock credentials
- Always validate that secrets exist before attempting to use them

### OpenTelemetry Integration
- All services instrumented with OpenTelemetry for distributed tracing
- Service names configured via `OTEL_SERVICE_NAME` environment variable
- Traces exported to OTEL collector on port 4318

### Network Resilience
- **ExchangeManager**: Singleton pattern prevents connection buildup issues
  - Connection pooling and reuse across all exchange operations
  - Health checks every 5 minutes with automatic connection recreation
  - Circuit breaker opens after 5 failures, resets after 5 minutes
  - Retry logic with exponential backoff (1s, 2s, 4s)
  - **Proxy Support**: Configure proxy and origin headers for bypassing IP restrictions
- **NetworkMonitor**: Comprehensive monitoring and alerting
  - Error classification and counting by type (timeout, connection_refused, etc.)
  - Latency tracking with OpenTelemetry metrics
  - Alert thresholds with cooldown periods
- **HyperLiquid Compatibility**: Health checks use `load_markets()` instead of `fetch_time()`
- **Usage**: All services use `exchange_manager.get_exchange()` and `exchange_manager.execute_with_retry()`

### Proxy Configuration
- **Purpose**: Bypass IP restrictions and DDOS protection on exchange APIs
- **Configuration** (in `config.yml`):
  ```yaml
  exchanges:
    hyperliquid:
      proxy: "http://3tsgp01.trisagion.xyz:8080/"  # CORS proxy URL (set to null to disable)
      origin: "https://trisagion.finance"           # Origin header for CORS
      poll_interval: 1.0   # Polling interval for price updates (seconds)
      poll_batch_size: 10  # Number of symbols per batch
  ```
- **Automatic Mode Selection**:
  - **With Proxy**: Automatically uses polling-based price producer (REST API)
  - **Without Proxy**: Automatically uses WebSocket streaming for real-time prices
- **Components**:
  - **cors-proxy**: Docker service running CORS Anywhere (optional for local proxy)
  - **price_producer_selector**: Automatically selects WebSocket or polling mode based on proxy configuration
- **Testing**: Run `python components/tests/test_proxy_connection.py` to verify proxy setup

### Celery Best Practices
- Name Beat schedules based on the action they perform (e.g., `update-balance`), not frequency
- Frequencies should be configured in `config.yml`, not hardcoded
- Use `eventlet` concurrency pool for I/O-bound tasks with autoscaling

## Service Access
- Grafana dashboards: http://localhost:3000/dashboards
- Jaeger tracing: http://localhost:16686
- Kibana (Elasticsearch/Jaeger data): http://localhost:5601
- Flower Celery monitoring: http://localhost:5555
- Prometheus metrics: http://localhost:9090
- Elasticsearch API: http://localhost:9200

## Task Scheduling
- Celery Beat runs on configurable intervals (default 30 seconds) for balance updates and market data scheduling
- Market data fetching uses stateful Redis tracking to avoid duplicate fetches
- Concurrency controlled via `market_data.concurrency_limit` config (default: 10)
- **Reconciliation Engine**: Runs every 15 minutes (configurable) to reconcile desired vs actual positions

## Reconciliation Engine

### Overview
The reconciliation engine is the brain of the trading system that continuously compares the desired portfolio state with the actual state and generates corrective orders to eliminate any gaps. It operates without explicit locking, relying on idempotent execution and state consensus for safety.

### Key Components
- **Desired State Calculation**: Reads active runs from the `runs` table to determine target positions based on `position_direction`, `live_pnl`, and `risk_pos_size`
- **Actual State Consensus**: Compares positions between local database and external observer nodes to establish consensus before taking action
- **Reconciliation Logic**: Implements extensively tested logic for all position scenarios (long/short/flat transitions)
- **Order Execution**: Sends market orders to the Order Gateway for actual trade execution

### Configuration (`config.yml`)
```yaml
reconciliation_engine:
  rebalance_frequency: 900.0  # 15 minutes between reconciliation cycles
  risk_pos_size: 20.25        # USD equivalent position size per run instance
  symbols:                    # Symbols to manage
    - "BTC/USDC:USDC"
    - "BCH/USDC:USDC"  
    - "ENA/USDC:USDC"
  observer_nodes:             # Observer nodes for consensus validation
    - "http://localhost:8001/3T-observer.json"
  order_gateway_url: "http://localhost:8002"
```

### Safety Features
- **Consensus Requirement**: Only trades when at least 2 observers agree on current position
- **No-Lock Design**: Uses state consensus instead of locking for thread safety
- **Minimum Trade Threshold**: Only executes trades above $20 USD equivalent to avoid micro-adjustments
- **Idempotent Operations**: Safe to run multiple times without adverse effects

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
- **When modifying Python code, just restart services: `docker-compose restart`**
- Only rebuild if changing dependencies: `docker-compose up -d --build --no-deps <service_name>`