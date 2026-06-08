# Specifications

The Tactical Trend Trader (3T) manages a portfolio of instruments utilizing a continuously updated risk management model. Position sizing, margin utilization, and direction allocation are governed automatically.

## Supported Instruments

3T focuses on a curated set of highly liquid perpetual contracts available on the HyperLiquid platform. This focus ensures sufficient market depth to execute trades with minimal slippage and price impact. 

3T's end-to-end data and execution flow are managed through HyperLiquid's public APIs. For streaming time and sales data, 3T establishes a persistent connection to the HyperLiquid WebSocket API.

For the latest list of default symbols currently enabled in the Reconciliation Engine, please refer directly to:
[`config.yml.example`](https://github.com/tttrisagion/3T/blob/main/config.yml.example) in the project root directory.

---

## Trading Hours

The trading engine is scheduled to operate:
* **00:00 Monday through 23:00 Friday** (UTC+1 / UTC+2)

The system aligns its operational schedule with **Vatican City State (Central European Time CET / CEST)**. 

### Scheduled Market Closures (Holidays)
To observe solemnities and facilitate system-wide maintenance, the Reconciliation Engine is halted and no trades are executed during the following holidays:
* **Solemnity of Mary, Mother of God** (January 1)
* **Passion of Our Lord, Good Friday** (Varies annually)
* **Nativity of Jesus, Christmas Day** (December 25)

---

## Settlement & Accounting Heuristics

Currently, the system is running strictly as an experimental protocol. 

* All infrastructure, engineering, and data feed costs are absorbed directly by the founding members and are not offset by portfolio earnings.
* As trades are executed, realized profits are allocated directly back into the portfolio's core capital base to compound organically.
* During each Take Profit (TP) epoch, the starting capital pool compounds at the current rate specified per block.
