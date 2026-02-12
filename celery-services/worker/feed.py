"""
Price Feed Engine - Celery Worker Implementation
"""

import os
import time

from celery.utils.log import get_task_logger

from shared.celery_app import app
from shared.config import config
from shared.database import get_redis_connection
from shared.opentelemetry_config import get_tracer

logger = get_task_logger(__name__)
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


@app.task(name="worker.feed.update_prices_in_redis", ignore_result=True)
def update_prices_in_redis():
    """


    A singleton task that continuously fetches the latest price from the Redis price stream


    and writes it to a simple Redis key for fast access by trading tasks.


    """

    with get_redis_connection(decode_responses=True) as redis_conn:
        lock = redis_conn.lock("lock:update_prices_in_redis", timeout=600)

        if not lock.acquire(blocking=False):
            logger.info("Another price feed task is already running. Exiting.")

            return

        try:
            symbols = config.get("reconciliation_engine.symbols", [])

            if not symbols:
                logger.warning("No symbols configured for price feed.")

                return

            price_stream = config.get("redis.streams.price_updates", "prices:updated")

            while True:
                for symbol in symbols:
                    try:
                        messages = redis_conn.xrevrange(price_stream, count=100)

                        latest_price = None

                        for _, msg_data in messages:
                            if msg_data.get("symbol") == symbol:
                                price = msg_data.get("price")

                                if price:
                                    latest_price = float(price)

                                    break

                        if latest_price is not None:
                            redis_conn.set(f"price:{symbol}", latest_price, ex=60)

                    except Exception as e:
                        logger.error(
                            f"Error processing price for {symbol}: {e}", exc_info=True
                        )

                lock.reacquire()

                time.sleep(0.1)

        finally:
            lock.release()


@app.task(name="worker.feed.supervisor")
def feed_supervisor():
    """


    Ensures the price feed task is running.


    """

    with get_redis_connection() as redis_conn:
        if not redis_conn.exists("lock:update_prices_in_redis"):
            logger.info("Price feed task not running. Starting it.")

            update_prices_in_redis.delay()
