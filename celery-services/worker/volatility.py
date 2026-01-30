"""
Volatility Engine - Celery Worker Implementation
"""

import os
import time

from celery.utils.log import get_task_logger

from shared.celery_app import app
from shared.config import config
from shared.database import get_db_connection, get_redis_connection
from shared.opentelemetry_config import get_tracer

logger = get_task_logger(__name__)
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


def calculate_volatility_for_symbol(symbol):
    db_cnx = None
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()
        query = f"""
            WITH
            minute_data AS (
              SELECT timestamp, high - low AS range_high_low
              FROM market_data WHERE timeframe = '1m' AND symbol = '{symbol}'
                AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
            ),
            window_data AS (
              SELECT timestamp,
                MAX(high) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS max_high,
                MIN(low) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS min_low
              FROM market_data WHERE timeframe = '1m' AND symbol = '{symbol}'
                AND timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 24 HOUR) * 1000
            ),
            volatility_data AS (
              SELECT timestamp, max_high - min_low AS volatility
              FROM window_data
            )
            SELECT from_unixtime(timestamp/1000) AS timestamp,
              AVG(volatility) OVER (ORDER BY timestamp ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS moving_volatility_average
            FROM volatility_data ORDER BY 1 DESC LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return float(result[1])
        return None
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.volatility.update_volatility_in_redis")


def update_volatility_in_redis():


    """


    A singleton task that continuously calculates volatility for all symbols


    and writes it to a simple Redis key.


    """


    with get_redis_connection(decode_responses=True) as redis_conn:


        lock = redis_conn.lock("lock:update_volatility_in_redis", timeout=600)


        if not lock.acquire(blocking=False):


            logger.info("Another volatility task is already running. Exiting.")


            return





        try:


            symbols = config.get("reconciliation_engine.symbols", [])


            if not symbols:


                logger.warning("No symbols configured for volatility.")


                return





            while True:


                for symbol in symbols:


                    try:


                        volatility = calculate_volatility_for_symbol(symbol)


                        if volatility is not None:


                            redis_conn.set(f"volatility:{symbol}", volatility, ex=600)


                    except Exception as e:


                        logger.error(f"Error processing volatility for {symbol}: {e}", exc_info=True)


                


                lock.reacquire()


                time.sleep(10) # Calculate every 10 seconds


        finally:


            lock.release()





@app.task(name="worker.volatility.supervisor")


def volatility_supervisor():


    """


    Ensures the volatility task is running.


    """


    with get_redis_connection() as redis_conn:


        if not redis_conn.exists("lock:update_volatility_in_redis"):


            logger.info("Volatility task not running. Starting it.")


            update_volatility_in_redis.delay()



