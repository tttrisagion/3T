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

## Development

The Python components are located in the `components` directory. The main entry point is `src/main.py`.

## Development

After making changes to the codebase, it is important to run the test suite to ensure that the changes have not introduced any regressions. The tests can be run by executing the `docker-compose.yml` file with the `components` service and the `test` command.

## Observability

The system will be instrumented with OpenTelemetry for distributed tracing. Traces will be sent to a Jaeger instance via an OpenTelemetry Collector.

## Testing

A comprehensive testing framework will be implemented to ensure code quality and reliability.

## Architecture

The system architecture is documented using C4 diagrams, which can be found in the `docs/arch` directory.

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
- **Celery Autoscaling:** For autoscaling to work with I/O-bound tasks, the worker must use the `eventlet` concurrency pool. This requires adding `eventlet` to `celery/requirements.txt` and using the `-P eventlet` flag in the `docker-compose.yml` command for the `celery_worker`.

## Defensive Programming

To improve system resilience and accelerate development, all modules and services should be built with a defensive posture. This means they should anticipate and gracefully handle potential failures, invalid inputs, and missing configuration.

- **Fail Fast with Clear Errors:** Instead of allowing a cryptic error to emerge from a third-party library, perform pre-flight checks on inputs and configuration. If validation fails, raise an immediate, informative exception that clearly explains the root cause.
- **Validate Critical Inputs:** Always validate critical data, especially secrets and configuration values, before they are used. Ensure they are not null, empty, or in an incorrect format.
- **Example (`update_balance` task):** The `update_balance` task provides a reference implementation. Before attempting to connect to the exchange, it verifies that all required API secrets (`apiKey`, `walletAddress`, `privateKey`) have been loaded from `secrets.yml`. If any are missing, it raises a `ValueError` that pinpoints the exact missing items, preventing the `ccxt` library from failing with a generic and unhelpful `ExchangeError`.