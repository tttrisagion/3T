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
    # Instantiate the config object here to ensure it reads the mounted file
    config = Config()
    return redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )


def listen_for_balance_updates():
    """
    Connects to the Redis Stream as part of a consumer group and processes
    balance update notifications indefinitely.
    """
    # Instantiate the config object here as well
    config = Config()

    redis_cnx = get_redis_connection()

    # Test connection
    try:
        redis_cnx.ping()
    except Exception as e:
        print(f"Failed to connect to Redis: {e}", flush=True)
        return

    stream_name = config.get("redis.streams.balance_updates")
    group_name = "balance_update_consumers"

    consumer_name = os.environ.get("HOSTNAME", "default-consumer")

    try:
        redis_cnx.xgroup_create(stream_name, group_name, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            print(f"An unexpected Redis error occurred during group creation: {e}")
            raise

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

            for stream, messages in response:
                for message_id, event_data in messages:
                    if "account_value" in event_data:
                        balance = event_data["account_value"]
                        epoch_ms = round(time.time() * 1000, 3)
                        sql_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        print(f"{epoch_ms}:{sql_date}:BALANCE:{balance}")

                    # Acknowledge the message
                    redis_cnx.xack(stream_name, group_name, message_id)

        except Exception as e:
            print(f"An error occurred while listening for stream events: {e}")
            time.sleep(5)


if __name__ == "__main__":
    listen_for_balance_updates()
