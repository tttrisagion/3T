"""
Prometheus metrics exporter for 3T trading system.

Exposes business and system metrics that lack historical snapshotting:
- Active run PnL (equity curve)
- Run counts and direction distribution
- Account balance and margin
- Position values
- Celery queue depths
- Market data freshness
- Run epoch (height) distribution
- Order execution stats
"""

import logging
import time

import mysql.connector
import redis
from prometheus_client import Gauge, Histogram, start_http_server

from shared.config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Providence / Equity Curve ---
ACTIVE_PNL_TOTAL = Gauge(
    "providence_active_runs_pnl_total",
    "Sum of live_pnl for all runs with height IS NULL (equity curve)",
)
OPEN_PNL_TOTAL = Gauge(
    "providence_open_runs_pnl_total",
    "Sum of live_pnl for active runs excluding exiting/ended (exit_run=0, end_time IS NULL)",
)
ACTIVE_PNL_BY_SYMBOL = Gauge(
    "providence_active_runs_pnl_by_symbol",
    "Sum of live_pnl for height IS NULL runs per symbol",
    ["symbol"],
)
ACTIVE_RUNS_TOTAL = Gauge(
    "providence_active_runs_count",
    "Count of runs with height IS NULL",
)
ACTIVE_RUNS_BY_SYMBOL = Gauge(
    "providence_active_runs_by_symbol",
    "Count of active runs per symbol",
    ["symbol"],
)
DIRECTION_DISTRIBUTION = Gauge(
    "providence_active_runs_direction",
    "Count of active runs by position direction bucket",
    ["direction"],
)
ACTIVE_PNL_MEAN = Gauge(
    "providence_active_runs_pnl_mean",
    "Mean live_pnl across active runs",
)
ACTIVE_PNL_POSITIVE_COUNT = Gauge(
    "providence_active_runs_pnl_positive_count",
    "Number of active runs with positive PnL",
)
ACTIVE_PNL_NEGATIVE_COUNT = Gauge(
    "providence_active_runs_pnl_negative_count",
    "Number of active runs with negative PnL",
)
RUN_AGE_HISTOGRAM = Histogram(
    "providence_run_age_seconds",
    "Age distribution of active runs",
    buckets=[60, 300, 900, 1800, 3600, 7200, 14400, 28800, 86400, 259200],
)

# --- Account / Balance ---
ACCOUNT_VALUE = Gauge(
    "account_balance_value",
    "Latest account value from balance_history",
)
MARGIN_USED = Gauge(
    "account_margin_used",
    "Latest cross maintenance margin used",
)
MARGIN_RATIO = Gauge(
    "account_margin_ratio",
    "Margin used / account value ratio",
)

# --- Positions ---
POSITION_VALUE_TOTAL = Gauge(
    "positions_total_value",
    "Sum of absolute position values across all products",
)
POSITION_UNREALIZED_PNL = Gauge(
    "positions_unrealized_pnl_total",
    "Sum of unrealized PnL across all positions",
)
POSITION_COUNT = Gauge(
    "positions_open_count",
    "Number of open positions (non-zero size)",
)
POSITION_BY_SYMBOL = Gauge(
    "positions_value_by_symbol",
    "Position value per symbol",
    ["symbol"],
)

# --- Celery Queues ---
QUEUE_DEPTH = Gauge(
    "celery_queue_depth",
    "Number of pending tasks in Celery queue",
    ["queue"],
)

# --- Market Data Freshness ---
MARKET_DATA_AGE = Gauge(
    "market_data_age_seconds",
    "Seconds since most recent market data row per symbol/timeframe",
    ["symbol", "timeframe"],
)
STREAM_DATA_AGE = Gauge(
    "stream_data_age_seconds",
    "Seconds since most recent stream price per symbol",
    ["symbol"],
)

# --- Run Heights / Epochs ---
HEIGHT_RUN_COUNT = Gauge(
    "providence_height_run_count",
    "Number of completed runs per height epoch (top 20)",
    ["height"],
)
TOTAL_COMPLETED_RUNS = Gauge(
    "providence_completed_runs_total",
    "Total number of runs with height assigned (completed epochs)",
)

# --- Account Performance ---
ACCOUNT_PCT_CHANGE = Gauge(
    "account_pct_change_from_baseline",
    "Percentage change of account value from take_profit_state baseline",
)
MAX_THEORETICAL_POSITION = Gauge(
    "providence_max_theoretical_position_usd",
    "Maximum theoretical USD position size at max Kelly and current run directions",
)

# --- Order Execution ---
ORDER_COUNT_BY_STATUS = Gauge(
    "order_execution_count_by_status",
    "Count of orders in execution log by status",
    ["status"],
)
ORDERS_LAST_HOUR = Gauge(
    "order_execution_last_hour",
    "Number of orders submitted in the last hour",
)


def get_db():
    return mysql.connector.connect(
        host=config.get("database.host"),
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )


def get_redis():
    return redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )


def collect_run_metrics(cursor):
    # All current-cohort runs: height IS NULL (includes exiting runs still carrying PnL)
    cursor.execute(
        "SELECT symbol, live_pnl, position_direction, "
        "TIMESTAMPDIFF(SECOND, start_time, NOW()) AS age_seconds, "
        "exit_run, end_time "
        "FROM runs WHERE height IS NULL"
    )
    rows = cursor.fetchall()

    total_pnl = 0.0
    open_pnl = 0.0
    count = 0
    positive = 0
    negative = 0
    pnl_by_symbol = {}
    count_by_symbol = {}
    direction_counts = {"long": 0, "short": 0, "flat": 0}

    # Reset histogram (re-observe each scrape)
    # Histograms accumulate, so we create fresh observations each cycle
    for symbol, pnl, direction, age, exit_run, end_time in rows:
        pnl = pnl or 0.0
        total_pnl += pnl
        count += 1
        if pnl > 0:
            positive += 1
        elif pnl < 0:
            negative += 1

        # Open PnL: exclude exiting and ended runs
        if not exit_run and end_time is None:
            open_pnl += pnl

        sym = symbol or "unknown"
        pnl_by_symbol[sym] = pnl_by_symbol.get(sym, 0.0) + pnl
        count_by_symbol[sym] = count_by_symbol.get(sym, 0) + 1

        if direction and direction > 0:
            direction_counts["long"] += 1
        elif direction and direction < 0:
            direction_counts["short"] += 1
        else:
            direction_counts["flat"] += 1

        if age is not None:
            RUN_AGE_HISTOGRAM.observe(age)

    ACTIVE_PNL_TOTAL.set(total_pnl)
    OPEN_PNL_TOTAL.set(open_pnl)
    ACTIVE_RUNS_TOTAL.set(count)
    ACTIVE_PNL_POSITIVE_COUNT.set(positive)
    ACTIVE_PNL_NEGATIVE_COUNT.set(negative)
    ACTIVE_PNL_MEAN.set(total_pnl / count if count > 0 else 0.0)

    for sym, pnl in pnl_by_symbol.items():
        ACTIVE_PNL_BY_SYMBOL.labels(symbol=sym).set(pnl)
    for sym, c in count_by_symbol.items():
        ACTIVE_RUNS_BY_SYMBOL.labels(symbol=sym).set(c)
    for d, c in direction_counts.items():
        DIRECTION_DISTRIBUTION.labels(direction=d).set(c)


def collect_balance_metrics(cursor):
    cursor.execute(
        "SELECT account_value, cross_maintenance_margin_used "
        "FROM balance_history ORDER BY timestamp DESC LIMIT 1"
    )
    row = cursor.fetchone()
    if row:
        val, margin = float(row[0]), float(row[1])
        ACCOUNT_VALUE.set(val)
        MARGIN_USED.set(margin)
        MARGIN_RATIO.set(margin / val if val > 0 else 0.0)


def collect_position_metrics(cursor):
    cursor.execute(
        "SELECT p.symbol, pos.position_size, pos.position_value, pos.unrealized_pnl "
        "FROM positions pos "
        "JOIN products p ON pos.product_id = p.id "
        "WHERE pos.id IN ("
        "  SELECT MAX(id) FROM positions GROUP BY product_id"
        ")"
    )
    rows = cursor.fetchall()
    total_value = 0.0
    total_pnl = 0.0
    open_count = 0
    for symbol, size, value, pnl in rows:
        val = abs(float(value or 0))
        total_value += val
        total_pnl += float(pnl or 0)
        if float(size or 0) != 0:
            open_count += 1
        POSITION_BY_SYMBOL.labels(symbol=symbol).set(float(value or 0))

    POSITION_VALUE_TOTAL.set(total_value)
    POSITION_UNREALIZED_PNL.set(total_pnl)
    POSITION_COUNT.set(open_count)


def collect_queue_metrics(r):
    for queue in ["high_priority", "low_priority"]:
        depth = r.llen(queue)
        QUEUE_DEPTH.labels(queue=queue).set(depth)


def collect_market_data_freshness(cursor):
    now_ms = int(time.time() * 1000)
    cursor.execute(
        "SELECT symbol, timeframe, MAX(timestamp) AS latest "
        "FROM market_data GROUP BY symbol, timeframe"
    )
    for symbol, timeframe, latest in cursor.fetchall():
        if latest:
            age_s = (now_ms - int(latest)) / 1000.0
            MARKET_DATA_AGE.labels(symbol=symbol, timeframe=timeframe).set(
                max(age_s, 0)
            )

    cursor.execute(
        "SELECT symbol, MAX(timestamp) AS latest FROM stream_data GROUP BY symbol"
    )
    for symbol, latest in cursor.fetchall():
        if latest:
            age_s = (now_ms - int(latest)) / 1000.0
            STREAM_DATA_AGE.labels(symbol=symbol).set(max(age_s, 0))


def collect_height_metrics(cursor):
    cursor.execute(
        "SELECT height, COUNT(*) as cnt FROM runs "
        "WHERE height IS NOT NULL GROUP BY height ORDER BY height DESC LIMIT 20"
    )
    for height, cnt in cursor.fetchall():
        HEIGHT_RUN_COUNT.labels(height=str(height)).set(cnt)

    cursor.execute("SELECT COUNT(*) FROM runs WHERE height IS NOT NULL")
    row = cursor.fetchone()
    TOTAL_COMPLETED_RUNS.set(row[0] if row else 0)


def collect_account_performance_metrics(cursor):
    # Metric 1: Account % change from take_profit_state baseline
    cursor.execute(
        "SELECT "
        "  (SELECT account_value FROM balance_history "
        "   ORDER BY timestamp DESC LIMIT 1) AS current_balance, "
        "  (SELECT last_balance FROM take_profit_state "
        "   ORDER BY id DESC LIMIT 1) AS baseline_balance"
    )
    row = cursor.fetchone()
    current_balance = float(row[0]) if row and row[0] else None
    baseline = float(row[1]) if row and row[1] else None

    if current_balance and baseline and baseline > 0:
        ACCOUNT_PCT_CHANGE.set((current_balance - baseline) / baseline)

    # Metric 2: Max theoretical position size
    # Formula: balance * risk_pos_pct * (1 + kelly_threshold) * SUM(ABS(position_direction))
    if current_balance:
        cursor.execute(
            "SELECT SUM(ABS(position_direction)) FROM runs WHERE height IS NULL"
        )
        direction_row = cursor.fetchone()
        total_abs_direction = (
            float(direction_row[0]) if direction_row and direction_row[0] else 0.0
        )
        if total_abs_direction > 0:
            risk_pos_pct = config.get("reconciliation_engine.risk_pos_percentage")
            kelly_threshold = config.get("reconciliation_engine.kelly_threshold")
            max_pos = (
                current_balance
                * risk_pos_pct
                * (1 + kelly_threshold)
                * total_abs_direction
            )
            MAX_THEORETICAL_POSITION.set(max_pos)


def collect_order_metrics(cursor):
    cursor.execute("SELECT status, COUNT(*) FROM order_execution_log GROUP BY status")
    for status, cnt in cursor.fetchall():
        ORDER_COUNT_BY_STATUS.labels(status=status).set(cnt)

    cursor.execute(
        "SELECT COUNT(*) FROM order_execution_log "
        "WHERE created_at >= NOW() - INTERVAL 1 HOUR"
    )
    row = cursor.fetchone()
    ORDERS_LAST_HOUR.set(row[0] if row else 0)


def collect_all():
    conn = None
    r = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        collect_run_metrics(cursor)
        collect_balance_metrics(cursor)
        collect_position_metrics(cursor)
        collect_market_data_freshness(cursor)
        collect_height_metrics(cursor)
        collect_account_performance_metrics(cursor)
        collect_order_metrics(cursor)
        cursor.close()
    except Exception:
        log.exception("Error collecting DB metrics")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    try:
        r = get_redis()
        collect_queue_metrics(r)
    except Exception:
        log.exception("Error collecting Redis metrics")
    finally:
        if r:
            try:
                r.close()
            except Exception:
                pass


def main():
    port = 9101
    interval = 15  # seconds between collection cycles
    log.info("Starting 3T metrics exporter on :%d (interval=%ds)", port, interval)
    start_http_server(port)

    while True:
        collect_all()
        time.sleep(interval)


if __name__ == "__main__":
    main()
