# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Guidelines

**IMPORTANT: Configuration Values in Documentation**
- **NEVER hardcode specific config values in this document** unless using them as illustrative examples
- Configuration values change frequently and documentation quickly becomes stale and misleading
- Always reference `config.yml` for current values rather than documenting specific numbers
- When documenting features, describe what configuration options exist and where to find them, not their current values
- Exception: You may show example config snippets to illustrate structure/format, but clearly label them as examples

## Development Commands

### Testing & Quality
- Run full test suite: `make test` (runs ruff check, ruff format, and pytest - also used by pre-commit hooks)
- Format code: `python3 -m ruff format`
- Lint code: `python3 -m ruff check --fix`
- Individual test: `python3 -m pytest path/to/test_file.py::test_function`

### Docker Operations
- Start all services: `make install` or `docker-compose up -d --build`
- Stop and clean: `make clean` (stops containers, removes networks, and prunes Docker system)
- **Deploy code changes: `make install`** (CRITICAL: Always use this, NOT `docker-compose restart` - restart doesn't clear Python bytecode cache)
- Quick restart (config-only changes): `make restart` (use only for config.yml changes, not code)
- Rebuild single service: `docker-compose up -d --build --no-deps <service_name>` (only needed for requirements.txt changes)

**IMPORTANT**: Python bytecode caching means `docker-compose restart` will NOT pick up code changes. Always use `make install` after modifying .py files.

### Automated Setup
- **Setup Scripts**: Located in `setup/` directory for automated configuration
- **Fresh Environment**: All necessary configurations are created automatically

### Database Operations
- Complete database reset (development only):
  1. `make clean`
  2. `docker volume rm 3t_mariadb_data`
  3. `make install`

### Documentation
- Render PlantUML diagrams: `make render-diagrams`
- Whitepaper: https://trisagion.xyz/trisagion.pdf
- GitBook docs: https://trisagion.gitbook.io/

### Git Workflow
- Feature branches: Use `feature/` prefix (e.g., `feature/providence-v3-celery`)
- Commit messages: For major features, save message to `commit.txt` and use `git commit -F commit.txt`
- Branch from main: `git checkout -b feature/your-feature-name`

## Architecture Overview

3T is a microservices-based automated trading platform with the following key components:

### Core Services
- **Celery Workers** (`celery-services/`): Distributed task queue with priority routing
  - **Providence System** (`worker/providence.py`): Core trading engine running concurrent trading runs (count configured in `config.yml`)
    - `providence_supervisor`: Spawns and maintains desired run count
    - `providence_iteration_scheduler`: Triggers iterations on configured schedule with jitter to prevent thundering herd
    - `providence_trading_iteration`: Executes single iteration per run (1-2ms each)
  - **Reconciliation Engine** (`worker/reconciliation_engine.py`): Compares desired vs actual positions, generates corrective orders
  - **Feed/Volatility** (`worker/feed.py`, `worker/volatility.py`): Price feed consumption and volatility calculations
  - **Purge** (`worker/purge.py`): Stale run cleanup
- **Components** (`components/`): Real-time services including:
  - Balance update consumers (`example_balance_consumer.py`)
  - Price streaming producer (`price_stream_producer.py` / `price_poll_producer.py`) - WebSocket or polling mode
  - Order Gateway (`order_gateway.py`) - Trade execution service
- **Shared** (`shared/`): Common utilities including config management, Celery app, HyperLiquid client, and OpenTelemetry setup

### Infrastructure Services
- **Redis**: Message broker for Celery and streams for real-time events
- **MariaDB**: Persistent storage for trading data, positions, and balance history
- **Prometheus + Grafana**: Metrics collection and visualization
- **Jaeger + OTEL Collector**: Distributed tracing with Badger embedded storage (disk-based)
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
- `runs`: Providence trading runs with position directions, PnL, and state
  - `run_state`: JSON state for stateless task execution (cached in Redis)
  - `height`: Block height/epoch for grouping runs (assigned at take profit events)
  - `exit_run`: Flag for graceful shutdown (set by drawdown reset only)

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

### OpenTelemetry Integration & Sampling
- All services instrumented with OpenTelemetry for distributed tracing
- Service names configured via `OTEL_SERVICE_NAME` environment variable
- Traces exported to OTEL collector on port 4318
- **Sampling Configuration**: High-volume tasks use configurable sampling to reduce noise
  - Configured via `observability.sampling_rate` in `config.yml` (0.01 = 1%, 1.0 = 100%)
  - Noisy tasks (providence iterations, get_market_weight, etc.) sampled at configured rate
  - Critical tasks (supervisor, reconciliation) always fully traced
  - **Log Sampling**: Parallel log filtering applies same sampling to reduce log volume
  - Defined in `shared/opentelemetry_config.py` - add new noisy tasks to `noisy_tasks` set

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
- **Configuration**: See `config.yml` under `exchanges.hyperliquid` for:
  - `proxy` - CORS proxy URL (set to null to disable)
  - `origin` - Origin header for CORS requests
  - `poll_interval` - Polling interval for price updates (seconds)
  - `poll_batch_size` - Number of symbols per batch
- **Automatic Mode Selection**:
  - **With Proxy**: Automatically uses polling-based price producer (REST API)
  - **Without Proxy**: Automatically uses WebSocket streaming for real-time prices
- **Components**:
  - **cors-proxy**: Docker service running CORS Anywhere (optional for local proxy)
  - **price_producer_selector**: Automatically selects WebSocket or polling mode based on proxy configuration
- **Testing**: Run `python components/tests/test_proxy_connection.py` to verify proxy setup

### Celery Best Practices & Priority Queues
- **Priority Routing**: System uses two queues to prevent task starvation
  - `high_priority`: Critical tasks (supervisor, reconciliation, balance updates) - always run immediately
  - `low_priority`: Trading iterations - use spare capacity without blocking system operations
- **Task Routes** configured in `shared/celery_app.py` - add new tasks to appropriate queue
- **Worker Configuration**: Workers listen to both queues with `-Q high_priority,low_priority`
- Name Beat schedules based on the action they perform (e.g., `update-balance`), not frequency
- Frequencies should be configured in `config.yml`, not hardcoded
- Use `eventlet` concurrency pool for I/O-bound tasks with autoscaling

## Service Access
- Grafana dashboards: http://localhost:3000/dashboards
- Jaeger tracing: http://localhost:16686
- Flower Celery monitoring: http://localhost:5555
- Prometheus metrics: http://localhost:9090

## Task Scheduling & Providence System
- Celery Beat runs on configurable intervals for all scheduled tasks
- **Providence Iteration**: Scheduler triggers iteration tasks for all active runs
  - Each iteration executes in 1-2ms (fast, stateless)
  - Tasks route to `low_priority` queue to avoid blocking system operations
  - Jitter window spreads task execution to prevent thundering herd (configured via `providence.iteration_jitter_seconds`)
- **Providence Supervisor**: Maintains desired run count (configured via `providence.desired_run_count`)
  - Routes to `high_priority` queue to ensure it never waits
- **Reconciliation Engine**: Reconciles desired vs actual positions (frequency configured via `reconciliation_engine.rebalance_frequency`)
- Market data fetching uses stateful Redis tracking to avoid duplicate fetches
- Concurrency controlled via `market_data.concurrency_limit` in config

## Providence Trading System

### Overview
Providence is the core trading engine that executes concurrent trading runs using permutation entropy signals and clonal adaptation. It evolved from forking (v1) → async (v2) → Celery distributed architecture (v3). Run count configured in `config.yml`.

### Architecture (v3)
- **Iteration-based**: Short-lived tasks (1-2ms) vs long-running processes
- **Stateless execution**: Run state cached in Redis with 24h TTL, persisted to DB
- **Distributed**: Worker pool processes iterations in parallel across high/low priority queues
- **Clonal dynamics**: Successful entropy interpretations compound through Kelly sizing, losers exit
- **Load Balancing**: Jitter window spreads iteration execution to prevent resource spikes

### Key Components
- **Supervisor** (`providence_supervisor`): Maintains configured active run count, spawns new runs with random ANN parameters
- **Iteration Scheduler** (`providence_iteration_scheduler`): Triggers iterations for all active runs with randomized jitter to spread load
- **Trading Iteration** (`providence_trading_iteration`): Single iteration per run
  - Reads state from Redis cache (or DB if not cached)
  - Calculates permutation entropy from market data
  - Generates position direction {-1, 0, +1} based on entropy signal
  - Updates live PnL and Kelly-scaled position size
  - Saves state back to Redis + DB

### Mathematical Core
Providence maximizes portfolio **expected geometric growth rate** through parallel search:
```
max E[ln(W_total)] = max E[Σ_i ∫ f_i(H_i(s)) · sign(H_i(s) - threshold) · dS_s]
```
Where `H_i` is permutation entropy measured by run i, `f_i` is Kelly-scaled position size, and `dS/S` is the market process.

### Block Heights (Epochs)
- **Height Assignment**: Take profit events assign `height` value to all active runs (groups cohort)
- **NOT Exit Signal**: `height` assignment does NOT set `exit_run` flag - runs continue trading
- **Drawdown Reset**: Sets `exit_run=1` without height - runs terminate immediately
- **Permutation Entropy**: Uses block heights as epochs for calculating cross-run entropy

### State Management
- **Redis Keys**:
  - `providence:state:{run_id}` - Run state (24h TTL)
  - `providence:exit:{run_id}` - Exit signal for graceful shutdown
  - `providence:completed:{run_id}` - Completion tracking for idempotency
- **Database**: `runs` table with `run_state` JSON column (persistent)

### Configuration
See `config.yml` for Providence configuration options including:
- `providence.desired_run_count` - Number of concurrent runs to maintain
- `providence.iteration_jitter_seconds` - Jitter window for spreading task execution

## Reconciliation Engine

### Overview
The reconciliation engine continuously compares the desired portfolio state (from Providence runs) with the actual state and generates corrective orders to eliminate gaps. It operates without explicit locking, relying on idempotent execution and state consensus for safety.

### Key Components
- **Desired State Calculation**: Reads active runs from the `runs` table to determine target positions based on `position_direction`, `live_pnl`, and `risk_pos_size`
- **Actual State Consensus**: Compares positions between local database and external observer nodes to establish consensus before taking action
- **Reconciliation Logic**: Implements extensively tested logic for all position scenarios (long/short/flat transitions)
- **Order Execution**: Sends market orders to the Order Gateway for actual trade execution

### Configuration
See `config.yml` for Reconciliation Engine configuration options including:
- `reconciliation_engine.rebalance_frequency` - Time between reconciliation cycles
- `reconciliation_engine.risk_pos_percentage` - Position size as percentage of balance
- `reconciliation_engine.symbols` - List of trading symbols to manage
- `reconciliation_engine.observer_nodes` - URLs for consensus validation
- `reconciliation_engine.order_gateway_url` - Order execution service endpoint
- `reconciliation_engine.minimum_trade_threshold` - Minimum USD value to execute trades

### Safety Features
- **Consensus Requirement**: Only trades when at least 2 observers agree on current position
- **No-Lock Design**: Uses state consensus instead of locking for thread safety
- **Minimum Trade Threshold**: Only executes trades above configured USD threshold to avoid micro-adjustments
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
- Volume mounts enable code access but Python bytecode caching requires proper deployment
- Some services have shared dependencies (e.g., flower needs celery-services requirements)
- **When modifying Python code, always use: `make install`** (clears bytecode cache and restarts)
- Only rebuild if changing dependencies: `docker-compose up -d --build --no-deps <service_name>`

## Critical Architecture Decisions

### Database Connection Management (`shared/database.py`)
- **NO Connection Pooling**: Direct MySQL connections bypass mysql.connector's connection hard limit
- **Why**: Large worker pools with multiple connections would exceed pool limits
- **Solution**: Direct connections let MariaDB handle its native max_connections setting
- **Redis Pooling**: Configured connection pool (separate pools for decoded/raw)
- **decode_responses Default**: `True` for backward compatibility with order_gateway
  - Explicit `False` only where bytes needed (state serialization)
  - **Critical**: Changing this default breaks price lookups in order_gateway

### Task Priority and Queue Starvation
- **Problem**: High-volume trading iterations can starve critical tasks without priority queues
- **Solution**: Priority-based routing prevents supervisor from waiting hours in queue
- **Load Balancing**: Iteration jitter spreads task spawning to smooth resource usage
- **Monitor**: Check queue depths with `redis-cli LLEN high_priority` / `LLEN low_priority`
- **Healthy State**: high_priority should be near 0, low_priority manageable relative to worker capacity
- **Jaeger Traces**: If supervisor wait time >1 minute, queue starvation may be occurring

### Observability Sampling
- **Problem**: High-volume iteration tasks can flood Jaeger with traces
- **Solution**: Configurable sampling rate reduces noise while preserving critical task traces
- **Configuration**: Set `observability.sampling_rate` in `config.yml` to control sampling (0.01 = 1%, 1.0 = 100%)
- **Effect**: Iteration task logs/traces sampled at configured rate, supervisor/reconciliation always 100%
- **Tuning**: Lower sampling rate for higher run counts to keep trace volume manageable