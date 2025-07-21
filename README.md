# Tactical Trend Trader (3T)

```
              P

           /  .  \
        S /  -|-  \ S 
         /` . | . `\
        /    `.`    \
              |   
         _ _ _ _ _ _

              F
```

## Introduction

The Trisagion Tactical Trend Trader (3T) is a sophisticated, service-oriented trading bot designed for automated trend trading. It leverages a robust architecture to analyze market data, execute trades, and provide real-time observability into its operations.

## Architecture

The 3T system is built on a microservices architecture, with each component containerized using Docker for portability and scalability. The primary components are:

- **Python Controllers**: The core logic of the trading bot, responsible for executing trading strategies.
- **Redis**: A high-performance in-memory data store used for message brokering and caching.
- **Celery**: A distributed task queue that manages asynchronous tasks, such as order execution and data analysis.
- **Flower**: A web-based tool for monitoring and administering Celery jobs and workers.
- **Prometheus**: A monitoring and alerting toolkit that collects metrics from various services.
- **Grafana**: A platform for visualizing and analyzing metrics, with pre-built dashboards for monitoring the 3T system.
- **MariaDB**: A relational database used for persistent storage of trading data and configuration.

The system architecture is documented using the C4 model, and the diagrams can be found in the `docs/arch` directory. The diagrams are written in PlantUML and can be generated using the PlantUML extension for VS Code or other PlantUML-compatible tools.


![context](https://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/tttrisagion/3T/refs/heads/main/docs/arch/level-1-context.puml)

![container](https://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/tttrisagion/3T/refs/heads/main/docs/arch/level-2-container.puml)

![inference](https://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/tttrisagion/3T/refs/heads/main/docs/arch/level-3-inference.puml)

![strategy](https://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/tttrisagion/3T/refs/heads/main/docs/arch/level-3-strategy-runner.puml)

## Getting Started

To get started with the 3T system, you will need to have Docker and Docker Compose installed on your system.

1. **Clone the repository:**

   ```bash
   git clone https://github.com/tttrisagion/3T.git
   cd 3T
   ```

2. **Create a `secrets.yml` file:**

   Copy the `secrets.yml.example` file to `secrets.yml` and populate it with your actual secrets and API keys.

   ```bash
   cp secrets.yml.example secrets.yml
   ```

3. **Start the services:**

   ```bash
   docker-compose up -d
   ```

   This will start all the services in detached mode.

4. **Access the dashboards:**

   - **Grafana**: http://localhost:3000
   - **Jaeger**: http://localhost:16686

## Development

The Python components are located in the `components` directory. The main entry point for the trading bot is `src/example.py`.

### Prerequisites

- Python 3.11
- Docker
- Docker Compose

### Dependencies

The Python dependencies for the `components` service are listed in the `components/requirements.txt` file. The main dependencies are:

- **celery[redis]**: For asynchronous task management.
- **numba**: For high-performance Python code.
- **numpy**: For numerical computing.
- **mysql-connector-python**: For connecting to the MariaDB database.
- **ccxt**: For interacting with cryptocurrency exchanges.
- **opentelemetry-api**: For OpenTelemetry instrumentation.
- **opentelemetry-sdk**: For OpenTelemetry instrumentation.
- **opentelemetry-exporter-otlp**: For exporting OpenTelemetry data.
- **opentelemetry-instrumentation-celery**: For instrumenting Celery with OpenTelemetry.

### Running Tests

A comprehensive testing framework will be implemented to ensure code quality and reliability.

## Observability

The 3T system is designed for high observability, with a suite of tools for monitoring and tracing.

- **Metrics**: Prometheus scrapes metrics from Flower and other services, which are then visualized in Grafana.
- **Distributed Tracing**: The system will be instrumented with OpenTelemetry for distributed tracing, providing insights into the flow of requests across services.

# Documentation

For technical documentation please see https://trisagion.gitbook.io/trisagion-docs/technology

## Roadmap

The future plans for the 3T project include:

- **Enhanced Trading Strategies**: Implementing more sophisticated trading strategies and models.
- **Advanced Analytics**: Integrating advanced analytics and machine learning to improve trading performance.
- **Real-time Portfolio Management**: Developing a real-time portfolio management system to track and manage assets.
- **Expanded Exchange Support**: Adding support for more cryptocurrency exchanges and trading pairs.

## Contributing

Contributions to the 3T project are welcome. Please refer to the contributing guidelines for more information.

## License

The 3T project is licensed under the MIT License.
