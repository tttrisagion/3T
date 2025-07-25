import os
import time
from datetime import UTC, datetime

import ccxt
import mysql.connector
import redis
from celery.signals import worker_ready
from eventlet.greenpool import GreenPool
from opentelemetry import context as opentelemetry_context
from opentelemetry.instrumentation.celery import CeleryInstrumentor

from shared.celery_app import app
from shared.config import config
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))

CeleryInstrumentor().instrument()

# Configure Celery Beat schedule
app.conf.beat_schedule = {
    "update-balance": {
        "task": "worker.tasks.update_balance",
        "schedule": config.get("celery.schedules.update_balance", 30.0),
    },
    "schedule-market-data-fetching": {
        "task": "worker.tasks.schedule_market_data_fetching",
        "schedule": config.get("celery.schedules.schedule_market_data_fetching", 60.0),
    },
}
app.conf.timezone = "UTC"


def get_db_connection():
    """Creates and returns a new database connection."""
    return mysql.connector.connect(
        host=config.get("database.host"),
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )


def get_redis_connection():
    """Creates and returns a new Redis connection."""
    return redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )


@worker_ready.connect
def initial_data_backfill(sender, **kwargs):
    """
    Triggered once when a worker starts. Ensures the database has an initial
    set of market data, solving the "empty MEMORY table" problem on restart.
    """
    with tracer.start_as_current_span("initial_data_backfill") as span:
        print("WORKER READY: Starting initial market data backfill...")
        span.add_event("Starting initial market data backfill")

        try:
            # Use the same logic as the scheduler to fetch all configured timeframes
            # This ensures the system is fully populated on startup.
            schedule_market_data_fetching.delay(is_backfill=True)
            span.add_event("Successfully triggered initial backfill task.")
            print("Initial market data backfill task dispatched.")
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"ERROR: Failed to dispatch initial backfill task: {e}")


@app.task(name="worker.tasks.schedule_market_data_fetching")
def schedule_market_data_fetching(is_backfill=False):
    """
    Stateful, resilient orchestrator for fetching market data.
    - Reads timeframes from config.
    - Uses Redis to track the last fetched bar for each symbol/timeframe.
    - Only fetches if a new bar should be available.
    - If is_backfill is True, it ignores Redis and fetches everything.
    """
    parent_context = opentelemetry_context.get_current()
    span_name = "schedule_market_data_fetching"
    if is_backfill:
        span_name += "_backfill"

    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("is_backfill", is_backfill)
        db_cnx = None
        redis_cnx = None
        failed_jobs = []
        try:
            db_cnx = get_db_connection()
            redis_cnx = get_redis_connection()
            cursor = db_cnx.cursor(dictionary=True)

            cursor.execute("SELECT symbol FROM products")
            products = cursor.fetchall()
            timeframes = config.get("market_data.timeframes", ["1m"])
            span.set_attribute("products.count", len(products))
            span.set_attribute("timeframes.configured", timeframes)

            lookback_config = config.get("market_data.lookback_bars", {})
            default_lookback = lookback_config.get("default", 20)

            jobs_to_run = []
            for product in products:
                for timeframe in timeframes:
                    symbol = product["symbol"]
                    lookback = lookback_config.get(timeframe, default_lookback)

                    if is_backfill:
                        jobs_to_run.append((symbol, timeframe, lookback))
                        continue

                    # Stateful check
                    last_fetch_key = f"last_fetch:{symbol}:{timeframe}"
                    try:
                        last_fetched_ts = int(redis_cnx.get(last_fetch_key) or 0)
                    except (ValueError, TypeError):
                        last_fetched_ts = 0

                    # Calculate the timestamp of the most recent, completed bar
                    now_utc = datetime.now(UTC)
                    timeframe_seconds = ccxt.Exchange.parse_timeframe(timeframe)
                    latest_bar_ts = (
                        (int(now_utc.timestamp()) // timeframe_seconds)
                        * timeframe_seconds
                        * 1000
                    )

                    if latest_bar_ts > last_fetched_ts:
                        jobs_to_run.append((symbol, timeframe, lookback))

            if not jobs_to_run:
                span.add_event("No new market data to fetch at this time.")
                return

            span.add_event(f"Found {len(jobs_to_run)} new data points to fetch.")
            concurrency_limit = config.get("market_data.concurrency_limit", 10)
            pool = GreenPool(size=concurrency_limit)

            threads = [
                (
                    job,
                    pool.spawn(
                        traced_fetch_and_store_ohlcv,
                        job[0],
                        job[1],
                        job[2],
                        parent_context,
                    ),
                )
                for job in jobs_to_run
            ]

            for job, gt in threads:
                try:
                    gt.wait()
                except Exception as e:
                    failed_jobs.append((job, e))

            if failed_jobs:
                raise Exception(
                    f"{len(failed_jobs)}/{len(jobs_to_run)} market data fetches failed."
                )

            span.add_event(
                f"Successfully completed {len(jobs_to_run) - len(failed_jobs)} jobs."
            )

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in {span_name}: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


def traced_fetch_and_store_ohlcv(
    symbol: str,
    timeframe: str,
    lookback: int,
    parent_context: opentelemetry_context.Context,
):
    """
    A wrapper that creates a dedicated OTEL span for each fetch, and upon success,
    updates the 'last_fetch' timestamp in Redis.
    """
    token = opentelemetry_context.attach(parent_context)
    try:
        with tracer.start_as_current_span(f"fetch_job.{symbol}.{timeframe}") as span:
            span.set_attribute("symbol", symbol)
            span.set_attribute("timeframe", timeframe)
            span.set_attribute("lookback", lookback)
            try:
                fetch_and_store_ohlcv(symbol, timeframe, lookback)

                # On success, update Redis with the timestamp of the latest bar
                redis_cnx = get_redis_connection()
                timeframe_seconds = ccxt.Exchange.parse_timeframe(timeframe)
                latest_bar_ts = (
                    (int(datetime.now(UTC).timestamp()) // timeframe_seconds)
                    * timeframe_seconds
                    * 1000
                )
                last_fetch_key = f"last_fetch:{symbol}:{timeframe}"
                redis_cnx.set(last_fetch_key, latest_bar_ts)

                span.set_attribute("otel.status_code", "OK")
                span.add_event("Fetch successful and Redis updated")

            except Exception as e:
                span.set_attribute("otel.status_code", "ERROR")
                span.record_exception(e)
                print(
                    f"ERROR in traced_fetch_and_store_ohlcv for {symbol} ({timeframe}): {e}"
                )
                raise
    finally:
        opentelemetry_context.detach(token)


def fetch_and_store_ohlcv(symbol: str, timeframe: str, lookback: int):
    """
    Connects to the exchange, fetches OHLCV data, and stores it in the database.
    """
    db_cnx = None
    try:
        api_key = config.get_secret("exchanges.hyperliquid.apiKey")
        wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")
        private_key = config.get_secret("exchanges.hyperliquid.privateKey")

        if not all([api_key, wallet_address, private_key]):
            raise ValueError("Missing required Hyperliquid secrets in secrets.yml")

        exchange = ccxt.hyperliquid(
            {
                "apiKey": api_key,
                "walletAddress": wallet_address,
                "privateKey": private_key,
            }
        )

        # Fetch the last N bars to be safe
        since = int(time.time() * 1000) - (
            lookback * exchange.parse_timeframe(timeframe) * 1000
        )
        ohlcv_data = exchange.fetchOHLCV(symbol, timeframe, since)

        if not ohlcv_data:
            return

        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()
        query = "REPLACE INTO market_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        rows_to_insert = [
            (
                int(r[0]),
                symbol,
                timeframe,
                round(float(r[1]), 8),
                round(float(r[2]), 8),
                round(float(r[3]), 8),
                round(float(r[4]), 8),
                round(float(r[5]), 8),
            )
            for r in ohlcv_data
        ]
        cursor.executemany(query, rows_to_insert)
        db_cnx.commit()

    except Exception:
        if db_cnx:
            db_cnx.rollback()
        raise
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.tasks.update_balance")
def update_balance():
    with tracer.start_as_current_span("update_balance_task") as span:
        span.set_attribute("task.name", "update_balance")
        try:
            account_value = fetch_and_store_balance()
            # After successful update, publish the new account value to Redis Streams
            if account_value is not None:
                publish_balance_update_event(account_value)
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in update_balance task: {e}")
            raise


def publish_balance_update_event(account_value: float):
    with tracer.start_as_current_span("publish_balance_update_event") as span:
        try:
            r = get_redis_connection()
            stream_name = config.get("redis.streams.balance_updates")
            event_data = {"account_value": account_value}
            r.xadd(stream_name, event_data)
            span.set_attribute("redis.stream.name", stream_name)
            span.set_attribute("redis.event.account_value", account_value)
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error publishing to Redis stream: {e}")
            raise


def fetch_and_store_balance() -> float | None:
    with tracer.start_as_current_span("fetch_and_store_balance") as span:
        db_cnx = None
        account_value = None
        try:
            api_key = config.get_secret("exchanges.hyperliquid.apiKey")
            wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")
            private_key = config.get_secret("exchanges.hyperliquid.privateKey")

            if not all([api_key, wallet_address, private_key]):
                missing = [
                    k
                    for k, v in {
                        "apiKey": api_key,
                        "walletAddress": wallet_address,
                        "privateKey": private_key,
                    }.items()
                    if not v
                ]
                raise ValueError(
                    f"Missing required Hyperliquid secrets: {', '.join(missing)}"
                )

            exchange = ccxt.hyperliquid(
                {
                    "apiKey": api_key,
                    "walletAddress": wallet_address,
                    "privateKey": private_key,
                }
            )
            exchange_status = exchange.fetch_balance()

            db_cnx = get_db_connection()
            cursor = db_cnx.cursor(dictionary=True)
            cursor.execute("START TRANSACTION")
            cursor.execute("DELETE FROM positions")

            cursor.execute(
                "SELECT id, symbol FROM products WHERE exchange_id = (SELECT id FROM exchanges WHERE name = 'HyperLiquid')"
            )
            products = {row["symbol"]: row["id"] for row in cursor.fetchall()}

            if len(exchange_status["info"]["assetPositions"]) > 0:
                for pos in exchange_status["info"]["assetPositions"]:
                    symbol = pos["position"]["coin"] + "/USDC:USDC"
                    if symbol in products:
                        query = "INSERT INTO positions (product_id, position_size, position_value, unrealized_pnl) VALUES (%s, %s, %s, %s)"
                        cursor.execute(
                            query,
                            (
                                products[symbol],
                                pos["position"]["szi"],
                                pos["position"]["positionValue"],
                                float(pos["position"]["unrealizedPnl"]),
                            ),
                        )

            account_value = float(
                exchange_status["info"]["marginSummary"]["accountValue"]
            )
            margin_used = exchange_status["info"]["crossMaintenanceMarginUsed"]

            cursor.execute("SELECT id FROM exchanges WHERE name = 'HyperLiquid'")
            exchange_id = cursor.fetchone()["id"]

            query = "INSERT INTO balance_history (exchange_id, account_value, cross_maintenance_margin_used) VALUES (%s, %s, %s)"
            cursor.execute(query, (exchange_id, account_value, margin_used))

            cursor.execute("COMMIT")

            return account_value

        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error during fetch_and_store_balance: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()
