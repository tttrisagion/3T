import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import mysql.connector
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

from shared.config import config


def get_db_connection():
    """Establish a connection to the MariaDB database."""
    return mysql.connector.connect(
        host=config.get("database.host"),
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )


def get_tables_with_timestamp(cursor):
    """Get a list of all tables that have a 'timestamp' column."""
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    tables_with_timestamp = []
    for table in tables:
        if table.startswith("metrics"):
            continue
        else:
            cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = [column[0] for column in cursor.fetchall()]
            if "timestamp" in columns:
                tables_with_timestamp.append(table)
    return tables_with_timestamp


def check_database_freshness(cursor, tables, threshold):
    """Check the freshness of all tables with a timestamp column."""
    for table in tables:
        cursor.execute(f"SELECT MAX(timestamp) FROM `{table}`")
        latest_timestamp = cursor.fetchone()[0]
        if latest_timestamp:
            if isinstance(latest_timestamp, int):
                latest_timestamp = datetime.fromtimestamp(
                    latest_timestamp / 1000, tz=UTC
                )
            else:
                latest_timestamp = latest_timestamp.replace(tzinfo=UTC)
            if datetime.now(UTC) - latest_timestamp > timedelta(seconds=threshold):
                print(f"- Table '{table}' is stale. Last update: {latest_timestamp}")
                return False
    return True


def check_observer_nodes_freshness(threshold):
    """Check the freshness of all configured observer nodes."""
    observer_nodes = config.get("reconciliation_engine.observer_nodes", [])
    print(f"Checking observer nodes: {observer_nodes}")
    all_healthy = True
    for url in observer_nodes:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            latest_timestamp_str = data.get("timestamp")
            if latest_timestamp_str:
                latest_timestamp = datetime.fromisoformat(latest_timestamp_str)
                if datetime.now(UTC) - latest_timestamp > timedelta(seconds=threshold):
                    print(
                        f"- Observer node '{url}' is stale. Last update: {latest_timestamp}"
                    )
                    all_healthy = False
            else:
                print(f"- Observer node '{url}' is missing the 'timestamp' field.")
                all_healthy = False
        except requests.RequestException as e:
            print(f"- Error connecting to observer node '{url}': {e}")
            all_healthy = False
        except (json.JSONDecodeError, TypeError) as e:
            print(f"- Observer node '{url}' returned invalid JSON: {e}")
            all_healthy = False
    return all_healthy


def main():
    """Main monitoring loop."""
    polling_interval = config.get("health_monitor.polling_interval", 0.5)
    polling_threshold = config.get("health_monitor.polling_threshold", 5)
    cron_url = config.get_secret("health_monitor.cron_healthy_state_url")

    while True:
        print(f"\n[{datetime.utcnow()}] Running health checks...")
        healthy = True

        try:
            # Check database freshness
            with get_db_connection() as conn:
                cursor = conn.cursor()
                tables_with_timestamp = get_tables_with_timestamp(cursor)
                print(f"Checking tables: {tables_with_timestamp}")
                if not check_database_freshness(
                    cursor, tables_with_timestamp, polling_threshold
                ):
                    healthy = False

            # Check observer nodes freshness
            if not check_observer_nodes_freshness(polling_threshold):
                healthy = False

            # If all checks passed, ping the cron URL
            if healthy:
                print("✅ All checks passed. System is healthy.")
                if cron_url:
                    try:
                        requests.get(cron_url, timeout=10)
                        print(f"- Successfully pinged cron URL: {cron_url}")
                    except requests.RequestException as e:
                        print(f"- Error pinging cron URL: {e}")
                else:
                    print("- Cron URL not configured. Skipping ping.")
            else:
                print("❌ Some health checks failed. System is degraded.")

        except Exception as e:
            print(f"An error occurred during health checks: {e}")

        time.sleep(polling_interval)


if __name__ == "__main__":
    main()
