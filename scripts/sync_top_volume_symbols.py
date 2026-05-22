#!/usr/bin/env python3
"""
Sync Top Volume Symbols:
1. Fetches all perpetual markets from Hyperliquid via CCXT.
2. Ranks them by 24h trading volume.
3. Synchronizes the top 30 symbols to the instruments and products tables.
4. Updates config.yml with the top 30 symbols.
"""

import os
import sys
import yaml
import mysql.connector
import ccxt
from pathlib import Path

# Add project root to path to import shared modules
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from shared.config import config

# Database configuration from environment or defaults
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "secret")
DB_NAME = os.environ.get("DB_NAME", "3t")

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def sync_top_symbols(limit=30, dry_run=False):
    mode_label = "DRY RUN" if dry_run else "LIVE SYNC"
    print(f"=== {mode_label}: Syncing Top {limit} Symbols by Volume ===\n")
    
    print("Initializing Hyperliquid via CCXT...")
    exchange = ccxt.hyperliquid()
    
    print("Loading markets and fetching tickers...")
    try:
        markets = exchange.load_markets()
        tickers = exchange.fetch_tickers()
    except Exception as e:
        print(f"Error loading data from exchange: {e}")
        return

    # Filter for perpetual markets and collect volume data
    stats = []
    for symbol, m in markets.items():
        if m.get('type') != 'swap' and m.get('linear') != True:
            # We only want perpetuals
            continue
            
        ticker = tickers.get(symbol, {})
        volume = ticker.get('quoteVolume', 0) or 0
        
        stats.append({
            'symbol': symbol,
            'market': m,
            'volume_24h': volume
        })

    # Sort by volume descending
    stats.sort(key=lambda x: x['volume_24h'], reverse=True)
    top_stats = stats[:limit]

    print(f"\nTop {limit} Symbols by Volume:")
    print(f"{'Symbol':<25} {'Volume (24h)':<15}")
    print("-" * 40)
    for row in top_stats:
        print(f"{row['symbol']:<25} ${row['volume_24h']:>13,.0f}")

    if dry_run:
        print("\nDry run requested. Skipping database and config updates.")
        return

    # Database Synchronization
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor(dictionary=True)
    except Exception as e:
        print(f"\nError connecting to database: {e}")
        return

    # Ensure HyperLiquid exchange exists
    cursor.execute("SELECT id FROM exchanges WHERE name = 'HyperLiquid'")
    exch_row = cursor.fetchone()
    if not exch_row:
        # Check case variation
        cursor.execute("SELECT id FROM exchanges WHERE LOWER(name) = 'hyperliquid'")
        exch_row = cursor.fetchone()
        
    if not exch_row:
        print("Error: 'HyperLiquid' exchange not found in database.")
        db_cnx.close()
        return
    exchange_id = exch_row["id"]

    new_symbols_for_config = []

    print(f"\nSyncing to database...")
    for row in top_stats:
        m = row['market']
        ccxt_symbol = row['symbol']
        hl_id = m['id']
        market_type = "PERP" # Standardized for our DB
        max_lev = float(m.get('info', {}).get('maxLeverage', 50.0))
        
        # Instrument mapping
        # CCXT symbol is like 'BTC/USDC:USDC' or 'XYZ-INTC/USDC:USDC'
        # Base is 'BTC' or 'XYZ-INTC'
        base = m['base']
        instrument_name = base.replace("XYZ-", "").upper()
        
        if "XYZ-" in base:
            market_type = "HIP3_PERP"

        # 1. Ensure instrument exists
        cursor.execute("SELECT id FROM instruments WHERE name = %s", (instrument_name,))
        instr = cursor.fetchone()
        if not instr:
            cursor.execute("INSERT INTO instruments (name) VALUES (%s)", (instrument_name,))
            db_cnx.commit()
            instrument_id = cursor.lastrowid
            print(f"Added Instrument: {instrument_name}")
        else:
            instrument_id = instr["id"]

        # 2. Ensure product exists
        cursor.execute(
            "SELECT id FROM products WHERE exchange_id = %s AND symbol = %s",
            (exchange_id, ccxt_symbol)
        )
        prod = cursor.fetchone()
        if not prod:
            cursor.execute(
                """
                INSERT INTO products (instrument_id, exchange_id, symbol, product_type, max_leverage)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (instrument_id, exchange_id, ccxt_symbol, market_type, max_lev)
            )
            db_cnx.commit()
            print(f"Added Product:    {ccxt_symbol}")
        
        new_symbols_for_config.append(ccxt_symbol)

    db_cnx.close()

    # Update config.yml
    config_path = project_root / "config.yml"
    if config_path.exists():
        print(f"\nUpdating {config_path}...")
        try:
            with open(config_path, 'r') as f:
                app_config = yaml.safe_load(f)
            
            # Update the symbols list
            if 'reconciliation_engine' not in app_config:
                app_config['reconciliation_engine'] = {}
            
            app_config['reconciliation_engine']['symbols'] = new_symbols_for_config
            
            with open(config_path, 'w') as f:
                yaml.dump(app_config, f, default_flow_style=False, sort_keys=False)
            
            print(f"Successfully updated config.yml with {len(new_symbols_for_config)} symbols.")
        except Exception as e:
            print(f"Error updating config.yml: {e}")
    else:
        print(f"\nconfig.yml not found at {config_path}. Please update it manually.")

    print(f"\nSync Complete!")

if __name__ == "__main__":
    is_dry = "--dry-run" in sys.argv
    sync_top_symbols(limit=30, dry_run=is_dry)
