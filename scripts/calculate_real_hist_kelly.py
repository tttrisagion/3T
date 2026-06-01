#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add project root to path to import shared/worker modules
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "celery-services"))

# Set environment variable to bypass tracing initialization issues
os.environ["DISABLE_OTEL_EXPORTER"] = "true"

from shared.config import config

# Monkeypatch config to resolve database host when run outside docker
original_get = config.get
def patched_get(key, default=None):
    if key == "database.host":
        return os.environ.get("DB_HOST", "127.0.0.1")
    return original_get(key, default)
config.get = patched_get

# Import the actual function from reconciliation_engine
from worker.reconciliation_engine import _calculate_kelly_metrics, calculate_kelly_position_size

def main():
    print("=== Re-calculating Real Historical Kelly and Current Multipliers ===\n")
    
    # Dynamically find established symbols
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            user="root",
            password="secret",
            database="3t"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM runs WHERE height IS NOT NULL")
        est_symbols = [r[0] for r in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return
        
    print(f"Found established symbols: {est_symbols}\n")
    
    historical_kellys = []
    
    for sym in est_symbols:
        _, _, k_hist = _calculate_kelly_metrics("height IS NOT NULL", sym)
        _, _, k_curr = _calculate_kelly_metrics("height IS NULL AND exit_run = 0", sym)
        
        # Calculate actual dynamic position size using base_size = 1.0
        final_size = calculate_kelly_position_size(1.0, sym)
        
        print(f"Symbol: {sym}")
        print(f"  k_historical:  {f'{k_hist:.4f}' if k_hist is not None else 'None'}")
        print(f"  k_current:     {f'{k_curr:.4f}' if k_curr is not None else 'None'}")
        print(f"  Active Sizing Multiplier (calculate_kelly_position_size): {final_size:.4f}x")
        print()
        
        if k_hist is not None:
            historical_kellys.append(k_hist)

    if historical_kellys:
        unified_avg = sum(historical_kellys) / len(historical_kellys)
        print(f"=== Unified Probationary Sizing Calculation ===")
        print(f"Average of Stable Benchmarks (k_historical): {unified_avg:.4f}x ({unified_avg*100:.2f}%)")
    else:
        print("No valid historical Kelly benchmarks found.")

if __name__ == "__main__":
    main()
