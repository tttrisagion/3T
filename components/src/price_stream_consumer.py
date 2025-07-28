"""
Example price stream consumer that demonstrates how to consume real-time price updates
from the Redis stream.
"""

import os
import sys
import time
from datetime import datetime

import redis

# Import the Config class itself, not the global instance
from shared.config import Config

# Force stdout to be unbuffered for Docker logs
sys.stdout.reconfigure(line_buffering=True)


def get_redis_connection():
    """Creates and returns a new Redis connection."""
    config = Config()
    return redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )


def listen_for_price_updates():
    """
    Connects to the Redis Stream as part of a consumer group and processes
    price update notifications indefinitely.
    """
    config = Config()
    redis_cnx = get_redis_connection()

    # Test connection
    try:
        redis_cnx.ping()
    except Exception as e:
        print(f"Failed to connect to Redis: {e}", flush=True)
        return

    stream_name = config.get("redis.streams.price_updates")
    group_name = "price_update_consumers"
    consumer_name = os.environ.get("HOSTNAME", "default-price-consumer")

    print(
        f"Starting price consumer {consumer_name} for stream {stream_name}", flush=True
    )

    try:
        redis_cnx.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        print(f"Created consumer group {group_name}", flush=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            print(f"An unexpected Redis error occurred during group creation: {e}")
            raise
        else:
            print(f"Consumer group {group_name} already exists", flush=True)

    print("Listening for price updates...", flush=True)

    while True:
        try:
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
                    symbol = event_data.get("symbol", "UNKNOWN")
                    price = event_data.get("price", "0")
                    timestamp = event_data.get("timestamp", str(time.time()))

                    # Convert timestamp to readable format
                    try:
                        ts_float = float(timestamp)
                        sql_date = datetime.fromtimestamp(ts_float).strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        )[:-3]
                    except (ValueError, TypeError):
                        sql_date = "INVALID_TIMESTAMP"

                    print(f"{timestamp}:{sql_date}:PRICE:{symbol}:{price}", flush=True)

                    # Acknowledge the message
                    redis_cnx.xack(stream_name, group_name, message_id)

        except Exception as e:
            print(
                f"An error occurred while listening for price events: {e}", flush=True
            )
            time.sleep(5)


if __name__ == "__main__":
    listen_for_price_updates()
