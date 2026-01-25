import os
import sys
import time

import redis

from shared.celery_app import app
from shared.config import Config
from shared.database import get_db_connection

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


def get_last_balance(db_cnx):
    """Retrieves the last recorded balance from the database."""
    cursor = db_cnx.cursor(dictionary=True)
    cursor.execute(
        "SELECT last_balance FROM take_profit_state ORDER BY id DESC LIMIT 1"
    )
    result = cursor.fetchone()
    cursor.close()
    db_cnx.commit()
    return float(result["last_balance"]) if result else None


def update_last_balance(db_cnx, balance):
    """Updates or inserts the last balance in the database."""
    cursor = db_cnx.cursor()
    # Check if a record exists
    cursor.execute("SELECT id FROM take_profit_state LIMIT 1")
    exists = cursor.fetchone()
    if exists:
        cursor.execute("UPDATE take_profit_state SET last_balance = %s", (balance,))
    else:
        cursor.execute(
            "INSERT INTO take_profit_state (last_balance) VALUES (%s)", (balance,)
        )
    db_cnx.commit()
    cursor.close()


def get_next_height(db_cnx):
    """Get the next height value for grouping runs by take profit events."""
    cursor = db_cnx.cursor()
    cursor.execute("SELECT MAX(height) FROM runs")
    result = cursor.fetchone()
    cursor.close()

    max_height = result[0] if result and result[0] is not None else 0
    return max_height + 1


def trigger_take_profit(db_cnx):
    """Sets exit_run = 1 for all active runs and triggers immediate reconciliation."""
    cursor = db_cnx.cursor()

    # Get the next height value
    next_height = get_next_height(db_cnx)

    # Assign height to runs that contributed to take profit event
    cursor.execute(
        "UPDATE runs SET height = %s WHERE exit_run = 0 AND height IS NULL",
        (next_height,),
    )
    db_cnx.commit()
    cursor.close()
    print(
        f"TAKE PROFIT TRIGGERED: All active runs have been flagged to exit with height {next_height}.",
        flush=True,
    )

    # Trigger the reconciliation task
    for i in range(1, 4):
        # Double check that there was no edge case / race condition causing imbalance
        try:
            app.send_task("worker.reconciliation_engine.reconcile_positions")
            print("Successfully triggered reconciliation task.", flush=True)
        except Exception as e:
            print(f"Error triggering reconciliation task: {e}", flush=True)
        time.sleep(120 * i)


def trigger_drawdown_reset(db_cnx):
    """
    Flags all active runs with null height to exit, allowing the strategy to reset.
    This is triggered when the portfolio experiences a significant drawdown.
    """
    cursor = db_cnx.cursor()

    # Set exit_run = 1 to signal the run to exit.
    cursor.execute("UPDATE runs SET exit_run = 1 WHERE height IS NULL")
    db_cnx.commit()
    cursor.close()
    print(
        "DRAWDOWN RESET TRIGGERED: All active runs with null height have been flagged to exit.",
        flush=True,
    )

    # Trigger the reconciliation task to flatten positions
    for i in range(1, 4):
        # Double check that there was no edge case / race condition causing imbalance
        try:
            app.send_task("worker.reconciliation_engine.reconcile_positions")
            print("Successfully triggered reconciliation task.", flush=True)
        except Exception as e:
            print(f"Error triggering reconciliation task: {e}", flush=True)
        time.sleep(120 * i)


def listen_for_balance_updates():
    """
    Connects to the Redis Stream and processes balance updates to check for take-profit conditions.
    """
    config = Config()
    redis_cnx = None
    db_cnx = None

    while True:
        try:
            if redis_cnx is None:
                redis_cnx = get_redis_connection()
                redis_cnx.ping()
                print("Successfully connected to Redis.", flush=True)

            if db_cnx is None or not db_cnx.is_connected():
                db_cnx = get_db_connection()
                print("Successfully connected to the database.", flush=True)

            threshold = config.get("take_profit.threshold")
            reward_multiple = config.get("take_profit.reward_multiple")
            stream_name = config.get("redis.streams.balance_updates")
            group_name = "take_profit_consumers"
            consumer_name = os.environ.get("HOSTNAME", "take-profit-consumer-1")

            try:
                redis_cnx.xgroup_create(stream_name, group_name, id="0", mkstream=True)
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    print(f"Error creating consumer group: {e}", flush=True)
                    raise

            print("Listening for balance updates...", flush=True)
            print(f"Target profit pct: {threshold:.4f}", flush=True)
            print(f"Reward multiple: {reward_multiple:.4f}", flush=True)
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
                            if "account_value" in event_data:
                                current_balance = float(event_data["account_value"])
                                print(
                                    int(time.time()),
                                    f"Received balance update: {current_balance}",
                                    flush=True,
                                )

                                last_balance = get_last_balance(db_cnx)

                                if last_balance is None:
                                    print(
                                        "First run. Setting initial balance.",
                                        flush=True,
                                    )
                                    update_last_balance(db_cnx, current_balance)
                                else:
                                    profit = current_balance - last_balance
                                    if last_balance > 0:
                                        profit_percentage = profit / last_balance
                                        print(
                                            int(time.time()),
                                            f"Profit percentage: {profit_percentage:.4f}",
                                            flush=True,
                                        )

                                        if profit_percentage >= threshold:
                                            print(
                                                f"TAKE PROFIT: Threshold of {threshold} reached!",
                                                flush=True,
                                            )
                                            trigger_take_profit(db_cnx)
                                            update_last_balance(db_cnx, current_balance)
                                        elif (
                                            profit_percentage
                                            < -threshold / reward_multiple
                                        ):
                                            print(
                                                f"DRAWDOWN: Threshold of -{threshold / reward_multiple} reached, triggering reset!",
                                                flush=True,
                                            )
                                            trigger_drawdown_reset(db_cnx)
                                            update_last_balance(db_cnx, current_balance)

                            redis_cnx.xack(stream_name, group_name, message_id)

                except redis.exceptions.ConnectionError as e:
                    print(f"Redis connection error: {e}", flush=True)
                    redis_cnx = None
                    break  # Break inner loop to reconnect

                except Exception as e:
                    print(f"An error occurred: {e}", flush=True)
                    if not db_cnx.is_connected():
                        print("Database connection lost. Reconnecting...", flush=True)
                        db_cnx = None
                        break  # Break inner loop to reconnect
                    time.sleep(5)

        except Exception as e:
            print(f"An error occurred in the main loop: {e}", flush=True)
            redis_cnx = None
            db_cnx = None
            time.sleep(5)


if __name__ == "__main__":
    listen_for_balance_updates()
