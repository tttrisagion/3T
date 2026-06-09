# Technology

![Technology](/img/masthead-technology.jpg)

The heart of the Trisagion project is the **Tactical Trend Trader (3T)**. This collection of integrated components represents the technical manifestation of a rational, ordered, and robust system. It is an algorithmic, fully automated trading agent built on a foundation of quantitative analysis, concurrent systems, and decentralized finance protocols.

## System Architecture and Operational Cycle

The Tactical Trend Trader (3T) utilizes an adaptive, real-time optimization framework powered by **Providence**—a massively parallel trading engine. Providence executes concurrent runs using permutation entropy signals, performance-based position sizing, and evolutionary adaptation.

The architecture is designed to favor continuous, exploratory adaptation over retrospective prediction. The core operational cycle comprises four distinct stages that run in a continuous loop:

1. **Data Ingestion:** The system acquires low-latency, real-time market data from exchange APIs and websocket streams for the optimization engine, while REST APIs provide aggregated historical data for broader market analysis.
2. **Multi-Parameter Optimization:** A massively parallel optimization engine continuously tests parameter permutations against live market data. This engine functions as a continuous stochastic policy generator, allowing the system to identify locally optimal configurations for the current market environment in real-time.
3. **Trade Evaluation and Execution:** Using the locally optimal parameter set discovered by the optimization engine, this module analyzes the market regime, evaluates entry signals, calculates position size, and executes trades on the blockchain.
4. **State Reconciliation:** At scheduled intervals, the Reconciliation Engine assesses the health of all open positions against the dynamic benchmark provided by the optimization engine and makes corrective adjustments, serving as an intelligent, portfolio-wide risk manager.

---

## The Parameter Optimization Engine: Adaptation Over Prediction

The current architecture of 3T is the result of a deliberate evolution away from conventional machine learning prediction towards online stochastic exploration.

### The Abandoned Predictive Model
Early development focused on a regression-based approach using a sophisticated bootstrap-aggregated ensemble of heterogeneous learners (including a Multi-layer Perceptron, various gradient-boosted tree models like LightGBM/CatBoost/XGBoost, and Random Forests). These models were retrained hourly with the goal of forecasting the expected profit and loss of potential trades.

However, over several months of study, this predictive approach consistently underperformed. The core issue was identified as **adaptation lag**: models trained on historical data are fundamentally retrospective and their learned patterns become stale in the face of new market dynamics. In highly unpredictable environments, online stochastic exploration is more effective than classic supervised learning. Consequently, the predictive ML ensemble was removed entirely.

### Real-Time Stochastic Optimization
The successful replacement for the ML ensemble is the **Parameter Optimization Engine**. This engine operates as a continuous, massively parallel simulation layer running concurrently with the live trading system. Its purpose is to discover the most profitable set of parameters for the core trading algorithm as a function of out-of-sample performance on live data.

It functions as a stochastic decision engine. At each decision point, it generates a candidate policy by applying a random perturbation to a set of base parameters. This new policy is evaluated in a virtual simulation against live market data. The engine employs an "accept-if-profitable" hill-climbing rule: if the simulated profit is positive, its parameters are adopted for the next cycle; otherwise, the policy is discarded. This mechanism allows the system to rapidly adapt to favorable market conditions while continuously exploring the parameter space.

---

## Dynamic Risk & Portfolio Management

The 3T framework introduces a hybrid risk management model that abstracts risk from individual trades and manages it at the portfolio level.

### Performance-Based Position Sizing
The system departs from classical fixed Kelly Criterion sizing, which can be highly sensitive to estimation errors. Instead, it employs a performance-based heuristic that dynamically modulates a base position size according to recent profitability.

A core rule is the **Annual Percentage Rate (APR) Gating Condition**: the system will only increase its risk exposure if its rolling APR is trending positively and is above a target benchmark. This ensures that risk is escalated only during periods of sustained, proven profitability. If this condition is met, the final position size is calculated by adjusting a base size with a factor derived from the relative performance of current versus historical simulations.

### The State Reconciliation Module
A key innovation in 3T is the omission of traditional, per-trade stop-loss orders, which are highly susceptible to transient volatility and stop-hunts. Instead, risk is managed holistically through the **Reconciliation Engine**.

This engine runs as a scheduled Celery task (e.g., every 10 minutes) rather than being driven by immediate, noisy price tick action. During each cycle, it compares the desired positions (defined by the active trading runs) with the actual positions reported by the local database and external exchange observer nodes.

A position is not closed simply because it is losing money, but because the system has discovered, via live simulation, that better parameter configurations exist for the current market conditions. This process serves as an intelligent, system-wide stop-loss mechanism that is always adapted to the present market environment.

---

## Market Regime Analysis via Permutation Entropy

To prevent trading in highly stochastic and unpredictable market conditions, the system performs a market regime analysis. 3T utilizes **Permutation Entropy** to distinguish between predictable, trending regimes and random, non-trending noise.

The calculation uses a natural logarithm-based complexity measure to analyze price patterns. The process is as follows:
1. A time series is constructed from the most recent price points.
2. Permutation Entropy is calculated on this time series using an optimized embedding dimension and time lag. This is handled by a high-performance C++ module for maximum speed and efficiency.
3. If the calculated entropy value is below a dynamically optimized threshold, the market is classified as "trending" and the algorithm proceeds to evaluate trades. Otherwise, the market is classified as "noisy," and no new trading action is initiated.

---

## Execution Venue: HyperLiquid DEX

3T executes its strategies on the **HyperLiquid** decentralized exchange (DEX). This venue was specifically chosen for its unique combination of features that align with the system's performance and structural requirements.

* **On-Chain Order Book:** HyperLiquid is built on its own high-performance Layer-1 blockchain, which supports a fully on-chain Central Limit Order Book (CLOB). This architecture provides low-latency, high-throughput execution (supporting up to 200,000 orders per second) and advanced order types.
* **Non-Custodial Integrity:** Preserves the core DeFi principles of transparency, non-custodial asset management, and permissionless access.
* **Seamless Execution:** Once a symbol's simulated portfolio P&L shows a gain, 3T constructs, cryptographically signs, and broadcasts a transaction to the exchange on-chain via market "liquidity taker" orders.
