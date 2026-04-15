"""
Price Feed Engine - Celery Worker Implementation
"""

import os

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

            last_id = "$"  # Start from the end of the stream

            while True:
                # Read new messages from the stream
                # block=500 means wait up to 500ms for new data
                messages_batch = redis_conn.xread(
                    {price_stream: last_id}, count=100, block=500
                )

                if messages_batch:
                    for stream, messages in messages_batch:
                        for msg_id, msg_data in messages:
                            symbol = msg_data.get("symbol")
                            price = msg_data.get("price")

                            if symbol in symbols and price:
                                try:
                                    redis_conn.set(
                                        f"price:{symbol}", float(price), ex=60
                                    )
                                except (ValueError, TypeError):
                                    continue

                            last_id = msg_id

                lock.reacquire()

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
