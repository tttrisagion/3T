"""
Example price stream consumer that demonstrates how to consume real-time price updates
from the Redis stream and store them in MariaDB.
"""

import os
import sys
import time
from datetime import datetime

import mysql.connector
import redis

# Import the Config class and database connection functions
from shared.config import Config
from shared.database import get_db_connection, get_redis_connection

# Force stdout to be unbuffered for Docker logs
sys.stdout.reconfigure(line_buffering=True)


def ensure_redis_group(redis_cnx, stream_name, group_name):
    """Ensures the consumer group exists for the given Redis stream."""
    try:
        redis_cnx.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        print(f"Created consumer group {group_name}", flush=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            print(f"An unexpected Redis error during group creation: {e}", flush=True)
            raise
        print(f"Consumer group {group_name} already exists", flush=True)


def process_price_update(db_cursor, symbol, price, timestamp):
    """Processes a single price update and inserts it into the database."""
    try:
        ts_float = float(timestamp)
        # MariaDB stores timestamps as BIGINT, so we multiply by 1000
        db_timestamp = int(ts_float * 1000)
        sql_date = datetime.fromtimestamp(ts_float).strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]

        print(f"{timestamp}:{sql_date}:PRICE:{symbol}:{price}", flush=True)

        # Insert into MariaDB
        db_cursor.execute(
            "INSERT INTO stream_data (timestamp, symbol, last_px) VALUES (%s, %s, %s)",
            (db_timestamp, symbol, price),
        )

    except (ValueError, TypeError) as e:
        print(f"Error processing timestamp '{timestamp}': {e}", flush=True)
    except mysql.connector.Error as err:
        print(f"Database insert error: {err}", flush=True)


def listen_for_price_updates():
    """
    Connects to Redis and MariaDB, processes price updates, and ensures
    resilient connections.
    """
    config = Config()
    stream_name = config.get("redis.streams.price_updates")
    group_name = "price_update_consumers"
    consumer_name = os.environ.get("HOSTNAME", "default-price-consumer")

    print(
        f"Starting price consumer {consumer_name} for stream {stream_name}", flush=True
    )

    while True:
        try:
            with get_redis_connection() as redis_cnx, get_db_connection() as db_cnx:
                db_cnx.autocommit = True  # Autocommit for each insert
                cursor = db_cnx.cursor()

                ensure_redis_group(redis_cnx, stream_name, group_name)
                print("Listening for price updates...", flush=True)

                while True:
                    response = redis_cnx.xreadgroup(
                        groupname=group_name,
                        consumername=consumer_name,
                        streams={stream_name: ">"},
                        count=1,
                        block=0,
                    )

                    if not response:
                        continue

                    for _stream, messages in response:
                        for message_id, event_data in messages:
                            process_price_update(
                                cursor,
                                event_data.get("symbol", "UNKNOWN"),
                                event_data.get("price", "0"),
                                event_data.get("timestamp", str(time.time())),
                            )
                            redis_cnx.xack(stream_name, group_name, message_id)

        except (
            redis.exceptions.ConnectionError,
            mysql.connector.Error,
            BrokenPipeError,
        ) as e:
            print(f"Connection lost: {e}. Reconnecting in 5 seconds...", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    listen_for_price_updates()
