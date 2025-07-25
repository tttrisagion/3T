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

Tactical Trend Trader (3T) is a sophisticated, service-oriented platform designed for automated trend trading. It leverages a robust architecture to analyze market data, execute trades, and provide real-time observability into its operations.

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


![context](docs/arch/level-1-context.png)

![container](docs/arch/level-2-container.png)

![inference](docs/arch/level-3-inference.png)

![strategy](docs/arch/level-3-strategy-runner.png)

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

   - **Grafana**: http://localhost:3000/dashboards
   - **Jaeger**: http://localhost:16686
   - **Flower**: http://localhost:5555

## Observability

The 3T system is designed for high observability, with a suite of tools for monitoring and tracing.

- **Metrics**: Prometheus scrapes metrics from Flower and other services, which are then visualized in Grafana.
- **Distributed Tracing**: The system will be instrumented with OpenTelemetry for distributed tracing, providing insights into the flow of requests across services.

# Documentation

For technical documentation please see https://trisagion.gitbook.io/trisagion-docs/technology

## Roadmap

The current plans for this repository involve migrating the proof-of-concept functionality into a more refined architecture that provides:

- **Enhanced Trading Strategies**: Implementing more sophisticated trading strategies and models.
- **Advanced Analytics**: Integrating advanced analytics and machine learning to improve trading performance.
- **Real-time Portfolio Management**: Developing a real-time portfolio management system to track and manage assets.
- **Expanded Exchange Support**: Adding support for more cryptocurrency exchanges and trading pairs.

The future plans for the 3T project are updated here https://trisagion.gitbook.io/trisagion-docs/technology/research-and-roadmap

## Contributing

Contributions to the 3T project are welcome. Please refer to the [contributing](https://github.com/tttrisagion/3T/blob/main/CONTRIBUTING.md) guidelines for more information.

## License

The 3T project is licensed under the [MIT License](https://github.com/tttrisagion/3T/blob/main/LICENSE).
