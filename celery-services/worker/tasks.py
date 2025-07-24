import os
import time

import ccxt
import mysql.connector
import redis
from opentelemetry.instrumentation.celery import CeleryInstrumentor

from shared.celery_app import app
from shared.config import config
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))

CeleryInstrumentor().instrument()

# Configure Celery Beat schedule
app.conf.beat_schedule = {
    "update-balance-every-30-seconds": {
        "task": "worker.tasks.update_balance",
        "schedule": 30.0,
    },
    "schedule-market-data-fetching-every-minute": {
        "task": "worker.tasks.schedule_market_data_fetching",
        "schedule": 60.0,
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


@app.task(name="worker.tasks.schedule_market_data_fetching")
def schedule_market_data_fetching():
    """
    Dispatcher task that fetches all product symbols from the database
    and launches a worker task for each one.
    """
    with tracer.start_as_current_span("schedule_market_data_fetching") as span:
        cnx = None
        try:
            cnx = get_db_connection()
            cursor = cnx.cursor(dictionary=True)

            span.add_event("Fetching product symbols from database")
            cursor.execute("SELECT symbol FROM products")
            products = cursor.fetchall()
            span.set_attribute("products.count", len(products))

            for product in products:
                symbol = product["symbol"]
                # Launch a worker task for each symbol
                fetch_market_data_for_symbol.delay(symbol)

            span.add_event(f"Successfully dispatched {len(products)} worker tasks")

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in schedule_market_data_fetching: {e}")
            raise
        finally:
            if cnx and cnx.is_connected():
                cursor.close()
                cnx.close()


@app.task(name="worker.tasks.fetch_market_data_for_symbol")
def fetch_market_data_for_symbol(symbol: str):
    """
    Worker task to fetch OHLCV data for a single symbol and store it.
    """
    with tracer.start_as_current_span("fetch_market_data_for_symbol") as span:
        span.set_attribute("task.symbol", symbol)
        try:
            fetch_and_store_ohlcv(symbol)
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in fetch_market_data_for_symbol for {symbol}: {e}")
            raise


def fetch_and_store_ohlcv(symbol: str):
    """
    Connects to the exchange, fetches OHLCV data, and stores it in the database.
    """
    with tracer.start_as_current_span("fetch_and_store_ohlcv") as span:
        span.set_attribute("symbol", symbol)
        cnx = None
        exchange = None
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

            timeframe = config.get("market_data.timeframe", "1m")
            # Fetch last 20 minutes of data
            since = int(time.time() * 1000) - (20 * 60 * 1000)

            span.add_event(f"Fetching OHLCV data for {symbol}")
            ohlcv_data = exchange.fetchOHLCV(symbol, timeframe, since)
            # ohlcv_data = exchange.fetch_ohlcv(symbol, timeframe, since)
            span.add_event(
                f"Successfully fetched {len(ohlcv_data)} records for {symbol}"
            )

            if not ohlcv_data:
                span.add_event(f"No data returned for {symbol}")
                return

            cnx = get_db_connection()
            cursor = cnx.cursor()
            query = "REPLACE INTO market_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

            for row in ohlcv_data:
                values = (
                    int(row[0]),  # timestamp
                    symbol,
                    timeframe,
                    round(float(row[1]), 8),  # open
                    round(float(row[2]), 8),  # high
                    round(float(row[3]), 8),  # low
                    round(float(row[4]), 8),  # close
                    round(float(row[5]), 8),  # volume
                )
                cursor.execute(query, values)

            cnx.commit()
            span.add_event(
                f"Successfully inserted/updated {len(ohlcv_data)} records for {symbol}"
            )

        except Exception as e:
            if cnx:
                cnx.rollback()
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error during fetch_and_store_ohlcv for {symbol}: {e}")
            raise
        finally:
            # Note: synchronous ccxt doesn't need/have close() method
            if cnx and cnx.is_connected():
                cursor.close()
                cnx.close()


@app.task(name="worker.tasks.update_balance")
def update_balance():
    with tracer.start_as_current_span("update_balance_task") as span:
        span.set_attribute("task.name", "update_balance")
        try:
            fetch_and_store_balance()
            # After successful update, publish an event to Redis Streams
            publish_balance_update_event()
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in update_balance task: {e}")
            raise


def publish_balance_update_event():
    with tracer.start_as_current_span("publish_balance_update_event") as span:
        try:
            r = redis.Redis(
                host=config.get("redis.host"),
                port=config.get("redis.port"),
                db=config.get("redis.db"),
            )
            stream_name = config.get("redis.streams.balance_updates")
            event_data = {"status": "success"}
            r.xadd(stream_name, event_data)
            span.set_attribute("redis.stream.name", stream_name)
            print(f"Published event to Redis stream: {stream_name}")
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error publishing to Redis stream: {e}")
            raise


def fetch_and_store_balance():
    with tracer.start_as_current_span("fetch_and_store_balance") as span:
        cnx = None
        try:
            # --- Defensive Pre-flight Checks for Secrets ---
            api_key = config.get_secret("exchanges.hyperliquid.apiKey")
            wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")
            private_key = config.get_secret("exchanges.hyperliquid.privateKey")

            if not all([api_key, wallet_address, private_key]):
                missing = []
                if not api_key:
                    missing.append("apiKey")
                if not wallet_address:
                    missing.append("walletAddress")
                if not private_key:
                    missing.append("privateKey")
                raise ValueError(
                    f"Missing required Hyperliquid secrets in secrets.yml: {', '.join(missing)}"
                )

            exchange = ccxt.hyperliquid(
                {
                    "apiKey": api_key,
                    "walletAddress": wallet_address,
                    "privateKey": private_key,
                }
            )

            span.add_event("Fetching balance from exchange")
            exchange_status = exchange.fetch_balance()
            span.add_event("Successfully fetched balance")

            cnx = get_db_connection()
            cursor = cnx.cursor(dictionary=True)

            cursor.execute("START TRANSACTION")
            span.add_event("Starting database transaction")

            # Clear existing positions
            cursor.execute("DELETE FROM positions")

            # Fetch product IDs from the database
            cursor.execute(
                "SELECT id, symbol FROM products WHERE exchange_id = (SELECT id FROM exchanges WHERE name = 'HyperLiquid')"
            )
            products = {row["symbol"]: row["id"] for row in cursor.fetchall()}

            if len(exchange_status["info"]["assetPositions"]) > 0:
                for position_data in exchange_status["info"]["assetPositions"]:
                    symbol = position_data["position"]["coin"] + "/USDC:USDC"
                    if symbol in products:
                        product_id = products[symbol]
                        position_size = position_data["position"]["szi"]
                        position_value = position_data["position"]["positionValue"]
                        unrealized_pnl = float(
                            position_data["position"]["unrealizedPnl"]
                        )

                        query = "INSERT INTO positions (product_id, position_size, position_value, unrealized_pnl) VALUES (%s, %s, %s, %s)"
                        cursor.execute(
                            query,
                            (product_id, position_size, position_value, unrealized_pnl),
                        )

            account_value = float(
                exchange_status["info"]["marginSummary"]["accountValue"]
            )
            cross_maintenance_margin_used = exchange_status["info"][
                "crossMaintenanceMarginUsed"
            ]

            # Get HyperLiquid exchange ID
            cursor.execute("SELECT id FROM exchanges WHERE name = 'HyperLiquid'")
            exchange_id = cursor.fetchone()["id"]

            query = "INSERT INTO balance_history (exchange_id, account_value, cross_maintenance_margin_used) VALUES (%s, %s, %s)"
            cursor.execute(
                query, (exchange_id, account_value, cross_maintenance_margin_used)
            )

            cursor.execute("COMMIT")
            span.add_event("Committed database transaction")

        except Exception as e:
            if cnx:
                cursor.execute("ROLLBACK")
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error during fetch_and_store_balance: {e}")
            raise
        finally:
            if cnx and cnx.is_connected():
                cursor.close()
                cnx.close()
