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
    results = {}
    all_healthy = True
    for table in tables:
        cursor.execute(f"SELECT MAX(timestamp) FROM `{table}`")
        latest_timestamp = cursor.fetchone()[0]
        if latest_timestamp:
            if isinstance(latest_timestamp, int):
                latest_timestamp_dt = datetime.fromtimestamp(
                    latest_timestamp / 1000, tz=UTC
                )
            else:
                latest_timestamp_dt = latest_timestamp.replace(tzinfo=UTC)
            
            staleness = (datetime.now(UTC) - latest_timestamp_dt).total_seconds()
            is_healthy = staleness <= threshold
            
            results[table] = {
                "healthy": is_healthy,
                "latest_update": str(latest_timestamp_dt),
                "staleness_seconds": staleness
            }
            
            if not is_healthy:
                print(f"- Table '{table}' is stale. Last update: {latest_timestamp_dt}")
                all_healthy = False
        else:
            results[table] = {"healthy": False, "error": "No data"}
            all_healthy = False
            
    return all_healthy, results


def check_observer_nodes_freshness(threshold):
    """Check the freshness of all configured observer nodes."""
    observer_nodes = config.get("reconciliation_engine.observer_nodes", [])
    print(f"Checking observer nodes: {observer_nodes}")
    all_healthy = True
    results = {}
    
    for url in observer_nodes:
        try:
            response = requests.get(url, timeout=10)
            status_code = response.status_code
            response.raise_for_status()
            data = response.json()
            latest_timestamp_str = data.get("timestamp")
            
            if latest_timestamp_str:
                latest_timestamp = datetime.fromisoformat(latest_timestamp_str.replace('Z', '+00:00'))
                staleness = (datetime.now(UTC) - latest_timestamp).total_seconds()
                is_healthy = staleness <= threshold
                
                results[url] = {
                    "healthy": is_healthy,
                    "latest_update": str(latest_timestamp),
                    "staleness_seconds": staleness,
                    "status_code": status_code
                }
                
                if not is_healthy:
                    print(f"- Observer node '{url}' is stale. Last update: {latest_timestamp}")
                    all_healthy = False
            else:
                print(f"- Observer node '{url}' is missing the 'timestamp' field.")
                results[url] = {"healthy": False, "error": "Missing timestamp", "status_code": status_code}
                all_healthy = False
        except requests.RequestException as e:
            print(f"- Error connecting to observer node '{url}': {e}")
            results[url] = {"healthy": False, "error": str(e), "status_code": getattr(e.response, 'status_code', 500) if hasattr(e, 'response') else 500}
            all_healthy = False
        except (json.JSONDecodeError, TypeError) as e:
            print(f"- Observer node '{url}' returned invalid JSON: {e}")
            results[url] = {"healthy": False, "error": "Invalid JSON", "status_code": 200}
            all_healthy = False
            
    return all_healthy, results


def check_service_endpoints():
    """Probe the health endpoints of various services."""
    services_to_check = {
        "mission-control": "http://mission-control:80/",
        "flower": "http://flower:5555/",
        "grafana": "http://grafana:3000/api/health",
        "prometheus": "http://prometheus:9090/-/healthy",
        "tempo": "http://tempo:3200/ready",
        "order-gateway": "http://order-gateway:8002/health",
        "exchange-observer": "http://exchange-observer:8001/3T-observer.json"
    }
    
    results = {}
    for name, url in services_to_check.items():
        try:
            response = requests.get(url, timeout=5)
            results[name] = {
                "healthy": response.status_code < 400,
                "status_code": response.status_code,
                "response_time": response.elapsed.total_seconds()
            }
        except Exception as e:
            results[name] = {
                "healthy": False,
                "error": str(e),
                "status_code": 0
            }
    return results


def main():
    """Main monitoring loop."""
    polling_interval = config.get("health_monitor.polling_interval", 0.5)
    polling_threshold = config.get("health_monitor.polling_threshold", 5)
    cron_url = config.get_secret("health_monitor.cron_healthy_state_url")
    health_json_path = "/app/mission-control/health.json"

    while True:
        print(f"\n[{datetime.now(UTC)}] Running health checks...")
        report = {
            "timestamp": str(datetime.now(UTC)),
            "status": "healthy",
            "checks": {
                "database": {},
                "observers": {},
                "services": {}
            }
        }
        healthy = True

        try:
            # Check database freshness
            with get_db_connection() as conn:
                cursor = conn.cursor()
                tables_with_timestamp = get_tables_with_timestamp(cursor)
                db_healthy, db_results = check_database_freshness(
                    cursor, tables_with_timestamp, polling_threshold
                )
                report["checks"]["database"] = db_results
                if not db_healthy:
                    healthy = False

            # Check observer nodes freshness
            obs_healthy, obs_results = check_observer_nodes_freshness(polling_threshold)
            report["checks"]["observers"] = obs_results
            if not obs_healthy:
                healthy = False
                
            # Check individual service endpoints
            service_results = check_service_endpoints()
            report["checks"]["services"] = service_results
            for s in service_results.values():
                if not s["healthy"]:
                    # We don't necessarily mark the WHOLE system as degraded 
                    # if Grafana is down, but we could. For now, let's keep it informative.
                    pass

            # If all checks passed, ping the cron URL
            if healthy:
                report["status"] = "healthy"
                print("✅ All checks passed. System is healthy.")
                if cron_url:
                    try:
                        requests.get(cron_url, timeout=10)
                        print(f"- Successfully pinged cron URL: {cron_url}")
                    except requests.RequestException as e:
                        print(f"- Error pinging cron URL: {e}")
            else:
                report["status"] = "degraded"
                print("❌ Some health checks failed. System is degraded.")

        except mysql.connector.Error as e:
            print(f"Database connection error during health checks: {e}")
            report["status"] = "error"
            report["error"] = str(e)
            healthy = False
        except Exception as e:
            print(f"An unexpected error occurred during health checks: {e}")
            report["status"] = "error"
            report["error"] = str(e)
            healthy = False

        # Write the report to a JSON file for Mission Control
        try:
            with open(health_json_path, "w") as f:
                json.dump(report, f, indent=2)
        except Exception as e:
            print(f"Error writing health report: {e}")

        time.sleep(polling_interval)


if __name__ == "__main__":
    main()
