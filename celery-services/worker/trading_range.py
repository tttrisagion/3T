import os
from datetime import datetime, timedelta

import pandas as pd
from celery.utils.log import get_task_logger

from shared.celery_app import app
from shared.database import get_db_connection
from shared.opentelemetry_config import get_tracer

tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))
logger = get_task_logger(__name__)


@app.task(name="worker.trading_range.update_trading_range")
def update_trading_range():
    """
    Connects to the database, calculates the trading range for each symbol,
    and stores the results in the `trading_range` table.

    The trading range is defined as the highest high and lowest low over a
    configurable time window (e.g., previous 7 days, excluding the last 24 hours).
    """
    with tracer.start_as_current_span("update_trading_range"):
        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor(dictionary=True)

            # Fetch all product symbols from the database
            cursor.execute("SELECT symbol FROM products")
            symbols = [item["symbol"] for item in cursor.fetchall()]
            if not symbols:
                logger.warning("No product symbols found in the database.")
                return

            # Define the time window for the calculation using UTC
            now_utc = datetime.utcnow()
            end_time = now_utc - timedelta(days=1)
            start_time = end_time - timedelta(days=2)

            # Convert to timestamps in milliseconds
            end_timestamp = int(end_time.timestamp() * 1000)
            start_timestamp = int(start_time.timestamp() * 1000)

            logger.info(
                f"Querying data from timestamp {start_timestamp} to {end_timestamp}"
            )

            for symbol in symbols:
                # Fetch the relevant 4-hour data using a parameterized query
                query = """
                    SELECT high, low
                    FROM market_data
                    WHERE symbol = %s
                    AND timeframe = '4h'
                    AND timestamp >= %s
                    AND timestamp < %s
                """
                cursor.execute(query, (symbol, start_timestamp, end_timestamp))

                results = cursor.fetchall()
                if results:
                    df = pd.DataFrame(results)

                    high_threshold = df["high"].max()
                    low_threshold = df["low"].min()

                    # Insert or update the trading range in the new table
                    upsert_query = """
                        INSERT INTO trading_range (symbol, high_threshold, low_threshold)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        high_threshold = VALUES(high_threshold),
                        low_threshold = VALUES(low_threshold)
                    """
                    cursor.execute(
                        upsert_query, (symbol, high_threshold, low_threshold)
                    )
                    logger.info(
                        f"Updated trading range for {symbol}: High={high_threshold}, Low={low_threshold}"
                    )
                else:
                    logger.info(
                        f"No data found for {symbol} in the specified time range."
                    )

            connection.commit()

        except Exception as e:
            logger.error(
                f"An error occurred during trading range calculation: {e}",
                exc_info=True,
            )
            if connection:
                connection.rollback()
            raise
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
                logger.info("MySQL connection is closed.")
