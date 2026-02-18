# Trisagion Tactical Trend Trader (3T) Project

This project provides a production grade service-oriented architecture for the 3T system using Docker.

## Components

*   **Redis:** Streaming and key/value cache.
*   **Celery:** Task broker for managing asynchronous tasks.
*   **Flower:** Observability and management of Celery tasks.
*   **Prometheus:** Collects metrics from Flower and other services.
*   **Grafana:** Displays metrics from Prometheus.
*   **MariaDB:** Persistent storage for larger datasets.
*   **Python Controllers:** The core logic of the trading bot, running in multiple threads.

## Getting Started

1.  Run `docker-compose up -d` to start all the services.
2.  The Grafana dashboard will be available at http://localhost:3000.
3.  The Flower dashboard will be available at http://localhost:5555.

## Development Workflow

This project uses a `Makefile` to standardize the development and testing workflow.

-   **Install/Update Services:** To build or rebuild the Docker containers after a code change, run:
    ```bash
    make install
    ```
-   **Run Tests:** To run the entire test suite, including linting and formatting, use:
    ```bash
    make test
    ```
    This command is also run automatically as a pre-commit hook to ensure code quality before it enters the repository.

-   **Clean Up:** To stop and remove all running containers and networks, run:
    ```bash
    make clean
    ```

### Database Resets

If you need to completely reset the database to re-run the `init.sql` script (for example, after changing the schema), you must manually delete the persistent volume:

1.  `make clean`
2.  `docker volume rm 3t_mariadb_data`
3.  `make install`

### Database Migrations (Production)

In a production environment, resetting the database is not a viable option as it would result in data loss. When the data model changes (e.g., adding new products, altering tables), migrations must be performed manually by executing SQL commands directly against the running database.

**Example: Adding a New Product**

1.  **Identify the running container:** Use `docker-compose ps` to find the name of the MariaDB container (e.g., `3t_mariadb`).
2.  **Execute `INSERT` statements:** Use `docker exec` to apply the changes.
    ```bash
    # Add the new instrument
    docker exec -i 3t_mariadb mysql -u root -p"$MARIADB_ROOT_PASSWORD" 3t < "INSERT INTO instruments (name) VALUES ('NEW_INSTRUMENT');"

    # Add the new product
    docker exec -i 3t_mariadb mysql -u root -p"$MARIADB_ROOT_PASSWORD" 3t < "INSERT INTO products (instrument_id, exchange_id, symbol, product_type, max_leverage) VALUES ((SELECT id from instruments where name = 'NEW_INSTRUMENT'), 1, 'NEW/USDC:USDC', 'PERP', 10);"
    ```
    *Note: You must have the `MARIADB_ROOT_PASSWORD` environment variable set for this command to work. The password can be found in `secrets.yml`.*

This approach ensures that new data is added without affecting existing records. The `init.sql` file should also be updated to reflect the new schema for future environment setups.


### Pre-commit Hooks

The repository is configured with a pre-commit hook that automatically runs `make test`. To enable this, you must have `pre-commit` installed and run the following command once:

```bash
pre-commit install
```

## Observability

The system is instrumented with OpenTelemetry for distributed tracing. Traces are sent to Grafana Tempo via an OpenTelemetry Collector, and viewed through the Grafana Explore UI.

## Testing

A comprehensive testing framework will be implemented to ensure code quality and reliability.

## Architecture

The system architecture is documented using C4 diagrams, which can be found in the `docs/arch` directory.

- **C4 Diagram Best Practices:** C4 diagrams should not contain specific configuration values. For example, instead of "every 30 seconds," the diagram should say "on a configured schedule." This prevents the diagrams from becoming outdated when configuration changes.

## Reconciliation Engine

The reconciliation engine is a key component of the 3T system, responsible for ensuring that the actual portfolio state matches the desired state. It runs as a periodic task within the Celery worker.

### Key Features:

- **State Comparison:** The engine compares the desired positions, defined in the `runs` table, with the actual positions reported by the local database and external observer nodes.
- **Consensus-Based Trading:** It requires consensus between multiple sources before executing trades, enhancing safety and reliability.
- **Automated Rebalancing:** The engine automatically generates and executes orders to correct any discrepancies between the desired and actual portfolio states.
- **Dynamic Position Sizing:** The engine adjusts position sizes based on a configurable percentage of the latest account balance, allowing risk to scale with portfolio health. This is controlled by the `risk_pos_percentage` in `config.yml`.
- **Configurable:** The engine's behavior, including rebalance frequency and risk parameters, can be configured in `config.yml`.

## Providence Strategy: State Persistence and Resumption

The core autonomous trading logic resides in `providence/3T-PROVIDENCE-v2.py`. To ensure resilience against restarts and crashes, this script persists the state of each active trading run to a corresponding JSON file in `providence/saved_runs/`.

### Key Aspects:

- **Purpose**: Each JSON file acts as a complete snapshot of a run, including its unique ID, parameters, and the full history of its virtual trades stored in a Virtual Order Management System (`VOMS`) object.
- **Resumption Bug**: A critical bug was identified where, upon restarting the manager, a run would resume but its `live_pnl` (live profit and loss) would be reset to zero. The run's other attributes, like `position_direction`, appeared to be preserved, but the PNL calculation was starting from scratch.
- **Root Cause**: The issue stemmed from a fragile state deserialization process. While the saved file was correct, the logic for loading the state back into memory was faulty, failing to correctly reconstruct the internal state of the `VOMS` object.
- **Solution: Explicit State Reconstruction**: The robust solution was to abandon direct object deserialization. The corrected pattern involves:
    1.  Initializing a fresh, empty `VOMS` object upon resumption.
    2.  Reading the list of historical trades from the `voms_state` object in the JSON file.
    3.  Iterating through the saved trades and "replaying" them into the new `VOMS` object by calling the `add_trade` method for each one.
    4.  Updating the `VOMS` object with the latest market price.

This "Explicit State Reconstruction" method guarantees that the in-memory state of a resumed run is a perfect reflection of its saved history, preventing data corruption and ensuring PNL calculations continue seamlessly across restarts.

## Troubleshooting

When a service is inaccessible, always run `docker-compose ps` first to check if the container is running. If it is not running, attempt to start it before further troubleshooting.

- **Database Schema Errors:** If you encounter errors like `Table '3t.positions' doesn't exist`, it means the database initialization script was not executed. This happens because the script only runs when the database volume is first created. To force a re-initialization, you must completely reset the database:
  1.  `docker-compose down` (Stops and removes all containers)
  2.  `docker volume rm 3t_mariadb_data` (Deletes the persistent database data)
  3.  `docker-compose up -d --build` (Restarts the system with a fresh database)

## Development Notes

- **Applying Changes:** When you modify code or dependencies, you must rebuild the container for the changes to take effect. Use `docker-compose up -d --build --no-deps <service_name>`.
- **Volume Mounts:** If you encounter a `ModuleNotFoundError` or `FileNotFoundError`, the most likely cause is a missing or incorrect volume mount in the `docker-compose.yml` file for the service in question.
- **Shared Dependencies:** Some services depend on others. For instance, `flower` inspects the `celery_worker`, so its Dockerfile must install the same dependencies from `celery/requirements.txt`.
- **Celery Beat Naming:** Name Celery Beat schedules based on the *action* they perform (e.g., `update-balance`), not their frequency (e.g., `update-balance-every-30-seconds`). Frequencies should be managed in `config.yml` to avoid hardcoding configuration into code.
- **Celery Autoscaling:** For autoscaling to work with I/O-bound tasks, the worker must use the `eventlet` concurrency pool. This requires adding `eventlet` to `celery/requirements.txt` and using the `-P eventlet` flag in the `docker-compose.yml` command for the `celery_worker`.

## Naming Conventions

- **CRITICAL: Avoid Python Library Name Conflicts:** Directory names must not conflict with Python library imports. The current `/celery` directory creates import shadowing issues with the `celery` Python package, causing `ModuleNotFoundError` and unpredictable import behavior.
  - **Problem:** When Python sees `from celery import Celery`, it may resolve to the local `/celery` folder instead of the installed package
  - **Solution:** Use descriptive prefixes like `/celery-workers`, `/celery-services`, or `/task-workers`
  - **Best Practice:** Always check PyPI for existing package names before creating directories
- **Import Path Testing:** When adding new modules, always test imports from the project root to ensure no shadowing occurs

## Defensive Programming

To improve system resilience and accelerate development, all modules and services should be built with a defensive posture. This means they should anticipate and gracefully handle potential failures, invalid inputs, and missing configuration.

- **Fail Fast with Clear Errors:** Instead of allowing a cryptic error to emerge from a third-party library, perform pre-flight checks on inputs and configuration. If validation fails, raise an immediate, informative exception that clearly explains the root cause.
- **Validate Critical Inputs:** Always validate critical data, especially secrets and configuration values, before they are used. Ensure they are not null, empty, or in an incorrect format.
- **Example (`update_balance` task):** The `update_balance` task provides a reference implementation. Before attempting to connect to the exchange, it verifies that all required API secrets (`apiKey`, `walletAddress`, `privateKey`) have been loaded from `secrets.yml`. If any are missing, it raises a `ValueError` that pinpoints the exact missing items, preventing the `ccxt` library from failing with a generic and unhelpful `ExchangeError`.