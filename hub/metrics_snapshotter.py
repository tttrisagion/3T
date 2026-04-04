# hub/metrics_snapshotter.py
import os
import time
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("snapshotter")

VM_URL = "http://localhost:8428"
OUTPUT_DIR = "/app/www"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "stats.json")

def query_vm_range(query, step="5m"):
    try:
        end = int(time.time())
        start = end - (7 * 24 * 3600) # 7 days
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step
        }
        resp = requests.get(f"{VM_URL}/api/v1/query_range", params=params)
        resp.raise_for_status()
        res = resp.json()['data']['result']
        return res
    except Exception as e:
        logger.error(f"Range query failed: {e}")
        return []

def get_latest(query):
    try:
        resp = requests.get(f"{VM_URL}/api/v1/query", params={'query': query})
        resp.raise_for_status()
        return resp.json()['data']['result']
    except Exception as e:
        logger.error(f"Instant query failed: {e}")
        return []

def update_snapshot():
    logger.info("Updating snapshot...")
    snapshot = {"timestamp": time.time(), "wallets": {}}
    
    wallets_result = get_latest("count(up) by (wallet)")
    wallets = [r['metric']['wallet'] for r in wallets_result if 'wallet' in r['metric']]
    
    for wallet in wallets:
        wallet_data = {
            "equity_curve": [],
            "nodes": {},
            "summary": {}
        }
        
        # 1. Total Equity Curve (Balance) - 5m resolution for the "Curve" feel
        equity_query = f'max(account_balance_value{{wallet="{wallet}"}})'
        equity_results = query_vm_range(equity_query, step="5m")
        if equity_results and len(equity_results[0]['values']) > 0:
            wallet_data["equity_curve"] = equity_results[0]['values']
        else:
            # Fallback to single point if no history
            latest_val = get_latest(equity_query)
            if latest_val:
                wallet_data["equity_curve"] = [latest_val[0]['value']]
            
        # 2. Margin Utilization Curve
        margin_query = f'max(account_margin_ratio{{wallet="{wallet}"}})'
        margin_results = query_vm_range(margin_query, step="5m")
        if margin_results and len(margin_results[0]['values']) > 0:
            wallet_data["margin_curve"] = margin_results[0]['values']
        else:
            latest_margin = get_latest(margin_query)
            if latest_margin:
                wallet_data["margin_curve"] = [latest_margin[0]['value']]

        # 3. Node Heartbeat
        nodes_query = f'account_margin_ratio{{wallet="{wallet}"}}'
        nodes_results = get_latest(nodes_query)
        for r in nodes_results:
            node_id = r['metric'].get('node_id', 'unknown')
            wallet_data["nodes"][node_id] = {
                "risk_ratio": float(r['value'][1]),
                "status": "online" if (time.time() - float(r['value'][0])) < 300 else "offline"
            }
            
        # 4. Summary Block
        summary_metrics = {
            "balance": f'max(account_balance_value{{wallet="{wallet}"}})',
            "pnl": f'max(providence_open_runs_pnl_total{{wallet="{wallet}"}})',
            "positions": f'max(positions_total_value{{wallet="{wallet}"}})',
            "open_runs": f'max(providence_active_runs_count{{wallet="{wallet}"}})',
            "long_runs": f'max(providence_active_runs_direction{{wallet="{wallet}", direction="long"}})',
            "short_runs": f'max(providence_active_runs_direction{{wallet="{wallet}", direction="short"}})',
            "flat_runs": f'max(providence_active_runs_direction{{wallet="{wallet}", direction="flat"}})',
            "open_positions": f'max(positions_open_count{{wallet="{wallet}"}})',
            "max_data_age": f'max(market_data_age_seconds{{wallet="{wallet}"}})'
        }
        
        for key, query in summary_metrics.items():
            res = get_latest(query)
            if res:
                wallet_data["summary"][key] = float(res[0]['value'][1])
            
        snapshot["wallets"][wallet] = wallet_data

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(snapshot, f)

if __name__ == "__main__":
    time.sleep(5)
    while True:
        try:
            update_snapshot()
        except Exception as e:
            logger.error(f"Loop error: {e}")
        time.sleep(30)
