> ⚠️ **Warning: Highly Experimental Project**
> This project is currently in a highly experimental and pre-alpha state. It is **not ready for production use** and may contain significant bugs, breaking changes, or incomplete features. Use with caution.

# Tactical Trend Trader (3T)

```
              P

           /  .  \
       S  /  -|-  \  S 
         /`•. | .•`\
        /    `.`    \
              |   
         _ _ _|_ _ _

              F
```

## Introduction

Tactical Trend Trader (3T) is a sophisticated, distributed trading platform powered by **Providence v3** - a massively parallel trading engine that executes concurrent runs using permutation entropy signals, Kelly criterion position sizing, and evolutionary clonal adaptation. The system leverages a microservices architecture with priority-based task queuing to analyze market data, execute trades, and provide comprehensive observability.

Read the latest [whitepaper](https://trisagion.xyz/trisagion.pdf) and documentation at [trisagion.gitbook.io](https://trisagion.gitbook.io/trisagion-docs/).

[![LOC][loc-badge]][loc-report]

[loc-badge]: https://tokei.rs/b1/github/tttrisagion/3T
[loc-report]: https://github.com/tttrisagion/3T

### ✨ Key Features

- 🚀 **Massive Parallelism**: concurrent trading runs executing core strategy
- 🧬 **Clonal Adaptation**: Evolutionary search through entropy signal space with Kelly criterion
- 📊 **Permutation Entropy**: Natural logarithm-based disorder measurement for time-invariant patterns
- ⚖️ **Priority Queuing**: Prevents task starvation - critical tasks never wait
- 🔄 **Stateless Design**: Redis state caching (24h TTL) enables horizontal scalability
- 📈 **Consensus Trading**: Observer nodes validate positions before reconciliation
- 🔍 **Full Observability**: Distributed tracing (Grafana Tempo), metrics (Prometheus), log sampling


### 📖 Table of Contents

- [Architecture](#architecture)
- [Observability & Monitoring Stack](#observability--monitoring-stack)
- [Data Pipeline](#data-pipeline)
- [Networking](#networking)
- [Getting Started](#getting-started)
- [Documentation](#documentation)
- [Development](#development)
- [Observability](#observability)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Disclaimer](#disclaimer)
- [License](#license)

## Architecture

The 3T system is built on a microservices architecture, with each component containerized using Docker for portability and scalability.

### Providence v3 Trading Engine

The core of the system is **Providence v3**, a Celery-based distributed trading engine:

- **Concurrent Runs**: Each run explores different permutation entropy interpretations of market structure
- **Iteration-Based**: Short-lived tasks (1-2ms) execute every 5 seconds, enabling massive parallelism
- **Priority Queuing**: High-priority (supervisor, reconciliation) and low-priority (iterations) queues prevent task starvation
- **Stateless Execution**: Run state cached in Redis (24h TTL) with persistent DB backup
- **Clonal Adaptation**: Successful entropy signals compound through Kelly sizing, unsuccessful runs exit

### Infrastructure Components

- **Celery Services**: Worker (512 concurrent) and beat services for distributed task processing with priority routing
- **Python Components**: Real-time services for price streaming, balance monitoring, and order execution
- **Redis**: Message broker, state cache (providence:state:*), and real-time event streams
- **MariaDB**: Persistent storage for trading data, positions, and run state (runs.run_state JSON)

### Observability & Monitoring Stack
- **Grafana Tempo**: Distributed tracing system with disk-based block storage (lightweight, bounded memory).
- **OpenTelemetry Collector**: Receives, processes, and exports telemetry data from services to Tempo.
- **Prometheus**: Monitoring and alerting toolkit that collects metrics from various services.
- **Grafana**: Platform for visualizing metrics with pre-built dashboards for monitoring the 3T system.
- **Flower**: Web-based tool for monitoring and administering Celery jobs and workers.

The system architecture is documented using the C4 model, and the diagrams can be found in the `docs/arch` directory. The diagrams are written in PlantUML and can be generated using the PlantUML extension for VS Code or other PlantUML-compatible tools.

### Data Pipeline

The system features several integrated data pipelines:

1.  **Price Data**: Auto-selects WebSocket (`price_stream_producer`) or polling (`price_poll_producer`) based on proxy config. Streams real-time prices to Redis for consumption.
2.  **Balance Data**: Celery workers periodically fetch balance/position data and publish to Redis streams. Balance consumers log and monitor financial status.
3.  **Providence Trading**:
    - **Supervisor** (high-priority): Spawns/maintains active runs with random ANN parameters
    - **Iteration Scheduler** (high-priority): Triggers iteration tasks
    - **Trading Iterations** (low-priority): Calculate permutation entropy, generate position directions, update PnL
4.  **Portfolio Reconciliation**: Reconciliation engine (high-priority) compares desired positions (from Providence runs) with actual state (local DB + observer consensus) and generates corrective orders via order_gateway.

### Networking

#### Proxy Configuration

To enhance reliability and bypass potential IP-based restrictions from exchanges, the system supports routing traffic through a proxy. This is particularly useful for overcoming DDOS protection measures.

-   **Automatic Mode Selection**: The system automatically detects the proxy configuration in `config.yml`.
    -   **With Proxy**: It uses a polling-based price producer (`price_poll_producer.py`) that sends requests via the proxy.
    -   **Without Proxy**: It falls back to the default real-time WebSocket streaming (`price_stream_producer.py`).
-   **Configuration**: To enable the proxy, add the following to your `config.yml`:
    ```yaml
    exchanges:
      hyperliquid:
        proxy: "http://your-proxy-url:8080/"  # URL of the CORS proxy
        origin: "https://your-origin.com"      # Origin header for CORS
    ```
-   **CORS Proxy Service**: The `docker-compose.yml` file includes a `cors-proxy` service (`redocly/cors-anywhere`) to facilitate this. For local development, you can use the default, but for production, you should configure it with a proper whitelist.

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

Note: Your system must have docker and docker-compose available. The docker service must be running and you may need elevated sudo permissions. It is also reccomended to prepare your environment with `pip install -r requirements-dev.txt` before proceeding further.

   ```bash
   make install
   # or alternatively:
   docker compose up -d --build
   ```

   This will start all services in detached mode.

4. **Initialize the database:**

   After starting the services for the first time, you need to manually initialize the database schema.

   ```bash
   docker compose exec -T mariadb mysql -u root -psecret 3t < database/init.sql
   ```

5. **Access the dashboards:**

   - **Grafana**: http://localhost:3000/dashboards
   - **Tempo Traces**: http://localhost:3000/explore (select Tempo datasource in Grafana)
   - **Flower**: http://localhost:5555 (Celery monitoring)
   - **Prometheus**: http://localhost:9090 (metrics)

# Documentation

- Whitepaper  https://trisagion.xyz/trisagion.pdf
- Documentation https://trisagion.gitbook.io/

## Development

For development commands, testing procedures, and detailed technical guidance, see [GEMINI.md](GEMINI.md) and [CLAUDE.md](CLAUDE.md). Key commands:

- **Deploy code changes**: `docker compose restart` (no rebuild needed)
- **Run tests**: `make test`
- **Stop and clean**: `make clean`

![context](docs/arch/level-1-context.png)

**Architecture Diagrams** (C4 Model):
- **Level 1**: [System Context](docs/arch/level-1-context.puml) - External actors and systems
- **Level 2**: [Container](docs/arch/level-2-container.puml) - Microservices and infrastructure
- **Level 3**: [Providence System](docs/arch/level-3-providence.puml) - Trading engine components (NEW)
- **Level 3**: [Reconciliation Engine](docs/arch/level-3-reconciliation-service.puml) - Position reconciliation
- **Level 3**: [Order Gateway](docs/arch/level-3-order-gateway.puml) - Trade execution service

*Render diagrams: `make render-diagrams` or use PlantUML extension*

### Observability

The 3T system is designed for high observability, with a comprehensive suite of tools for monitoring, tracing, and debugging.

- **Metrics**: Prometheus scrapes metrics from Flower and other services, visualized in Grafana dashboards.
- **Distributed Tracing**: All services instrumented with OpenTelemetry, sending traces through OTEL Collector to Grafana Tempo with disk-based block storage.
- **Intelligent Sampling**: Configurable sampling (default 1% for high-volume tasks) reduces noise while preserving critical traces
  - High-volume tasks (providence iterations): sampled at configured rate
  - Critical tasks (supervisor, reconciliation): always 100% traced
  - Log sampling mirrors trace sampling for consistent observability
- **Real-time Monitoring**: Live trace data with auto-refresh capabilities to monitor system activity.

## Quick Reference

### Service URLs (localhost)
| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3000/dashboards | Metrics visualization |
| Tempo (via Grafana) | http://localhost:3000/explore | Distributed tracing UI |
| Flower | http://localhost:5555 | Celery monitoring |
| Prometheus | http://localhost:9090 | Metrics storage |
| Order Gateway | http://localhost:8002 | Trade execution API |

### Key Commands
```bash
# Deploy code changes (no rebuild)
docker compose restart

# Full system restart
make install

# Run tests with linting
make test

# Stop and clean
make clean

# Check queue health
docker compose exec redis redis-cli LLEN high_priority
docker compose exec redis redis-cli LLEN low_priority

# View logs with filtering
docker compose logs celery_worker --tail=100 | grep providence_supervisor
```

### Configuration Files
- **config.yml**: Application settings (schedules, symbols, run count)
- **secrets.yml**: API keys and passwords (copy from secrets.yml.example)
- **GEMINI.md** and **CLAUDE.md**: Technical guidance for agentic code development
- **commit.txt**: Detailed commit message template for major features

## Roadmap

The current plans for this repository involve migrating the proof-of-concept functionality into a more refined architecture that provides:

- **Enhanced Trading Strategies**: Implementing more sophisticated trading strategies and models.
- **Advanced Analytics**: Integrating advanced analytics and machine learning to improve trading performance.
- **Real-time Portfolio Management**: Developing a real-time portfolio management system to track and manage assets.
- **Expanded Exchange Support**: Adding support for more cryptocurrency exchanges and trading pairs.

The future plans for the 3T project are updated here https://trisagion.gitbook.io/trisagion-docs/technology/research-and-roadmap

## Contributing

Contributions to the 3T project are welcome. Please refer to the [contributing](https://github.com/tttrisagion/3T/blob/main/CONTRIBUTING.md) guidelines for more information.

## Disclaimer

This project is provided "as is" and "with all faults." The authors and contributors disclaim all warranties, express or implied, including any warranties of merchantability, fitness for a particular purpose, and non-infringement.

In no event shall the authors or contributors be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.

## License

The 3T project is licensed under the [MIT License](https://github.com/tttrisagion/3T/blob/main/LICENSE).
