#!/usr/bin/env python3
"""
Unified HIP-3 Symbol Manager:
1. Ranks all HIP-3 (XYZ-) and PIP symbols by activity (Volume + Open Interest).
2. Synchronizes only the TOP 13 symbols to the instruments and products tables.
"""

import os
import sys
import mysql.connector
import ccxt
import pandas as pd

# Database configuration from environment or defaults
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "secret")
DB_NAME = os.environ.get("DB_NAME", "3t")

def sync_top_symbols(limit=13, dry_run=False):
    mode_label = "DRY RUN" if dry_run else "LIVE SYNC"
    print(f"=== {mode_label}: Syncing Top {limit} HIP-3 Symbols via CCXT ===\n")
    
    print("Initializing Hyperliquid via CCXT...")
    exchange = ccxt.hyperliquid()
    
    print("Loading markets and fetching activity stats...")
    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(f"Error loading markets: {e}")
        return

    # Filter for XYZ- or PIP symbols
    candidate_symbols = [s for s, m in markets.items() if 'XYZ' in s.upper() or 'PIP' in s.upper() or 'XYZ' in m['id'].upper()]
    
    if not candidate_symbols:
        print("No HIP-3 (XYZ-) or PIP symbols found.")
        return

    print(f"Found {len(candidate_symbols)} candidate symbols. Ranking by Volume + OI...")
    
    # Fetch tickers for all candidates to get volume and price
    try:
        tickers = exchange.fetch_tickers(candidate_symbols)
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return

    stats = []
    for symbol in candidate_symbols:
        m = markets[symbol]
        ticker = tickers.get(symbol, {})
        
        # Volume (24h USD)
        volume = ticker.get('quoteVolume', 0) or 0
        
        # Open Interest (USD)
        oi = 0
        if 'info' in m and 'openInterest' in m['info']:
            oi = float(m['info']['openInterest'])
        elif 'info' in ticker and 'openInterest' in ticker['info']:
            oi = float(ticker['info']['openInterest'])
        
        last_price = ticker.get('last', 0) or 0
        oi_usd = oi * last_price if oi and last_price else 0
        
        stats.append({
            'symbol': symbol,
            'market': m,
            'volume_24h': volume,
            'open_interest_usd': oi_usd,
            'last_price': last_price
        })

    df = pd.DataFrame(stats)
    if df.empty:
        print("No stats collected.")
        return

    # Normalized ranking
    vol_max = df['volume_24h'].max() if df['volume_24h'].max() > 0 else 1
    oi_max = df['open_interest_usd'].max() if df['open_interest_usd'].max() > 0 else 1
    df['score'] = (df['volume_24h'] / vol_max) + (df['open_interest_usd'] / oi_max)
    
    top_df = df.sort_values(by='score', ascending=False).head(limit)

    print(f"\nTop {limit} Active Symbols:")
    print(f"{'Symbol':<25} {'Volume (24h)':<15} {'Open Interest':<15}")
    print("-" * 60)
    for _, row in top_df.iterrows():
        print(f"{row['symbol']:<25} ${row['volume_24h']:>13,.0f} ${row['open_interest_usd']:>13,.0f}")

    # Database Synchronization
    try:
        db_cnx = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        cursor = db_cnx.cursor(dictionary=True)
    except Exception as e:
        print(f"\nError connecting to database: {e}")
        sys.exit(1)

    cursor.execute("SELECT id FROM exchanges WHERE name = 'HyperLiquid'")
    exch_row = cursor.fetchone()
    if not exch_row:
        print("Error: 'HyperLiquid' exchange not found in database.")
        sys.exit(1)
    exchange_id = exch_row["id"]

    added_count = 0
    skipped_count = 0
    newly_added_for_config = []

    print(f"\nProceeding to sync top {len(top_df)} symbols to database...")

    for _, row in top_df.iterrows():
        # ... (rest of market data extraction remains the same)
        m = row['market']
        ccxt_symbol = row['symbol']
        hl_id = m['id']
        market_type = m['type'].upper()
        max_lev = float(m.get('info', {}).get('maxLeverage', 40.0))
        
        # Instrument mapping
        base_name = hl_id if 'XYZ' in hl_id.upper() else m['base']
        instrument_name = base_name.replace("XYZ-", "").upper()
        
        # 1. Ensure instrument exists
        cursor.execute("SELECT id FROM instruments WHERE name = %s", (instrument_name,))
        instr = cursor.fetchone()
        if not instr:
            if not dry_run:
                cursor.execute("INSERT INTO instruments (name) VALUES (%s)", (instrument_name,))
                db_cnx.commit()
                instrument_id = cursor.lastrowid
                print(f"Added Instrument: {instrument_name}")
            else:
                instrument_id = "NEW"
                print(f"[DRY] Would add Instrument: {instrument_name}")
        else:
            instrument_id = instr["id"]

        # 2. Ensure product exists
        cursor.execute(
            "SELECT id FROM products WHERE exchange_id = %s AND symbol = %s",
            (exchange_id, ccxt_symbol)
        )
        prod = cursor.fetchone()
        if not prod:
            if not dry_run:
                cursor.execute(
                    """
                    INSERT INTO products (instrument_id, exchange_id, symbol, product_type, max_leverage)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (instrument_id, exchange_id, ccxt_symbol, "HIP3_PERP" if market_type == "SWAP" else market_type, max_lev)
                )
                db_cnx.commit()
                print(f"Added Product:    {ccxt_symbol}")
            else:
                print(f"[DRY] Would add Product:    {ccxt_symbol}")
            
            added_count += 1
            newly_added_for_config.append(ccxt_symbol)
        else:
            skipped_count += 1

    db_cnx.close()
    
    print(f"\n{mode_label} Complete!")
    print(f"{'Added' if not dry_run else 'Would add'}: {added_count}")
    print(f"Skipped: {skipped_count}")
    
    if newly_added_for_config:
        print(f"\nCopy these NEWLY added symbols to your config.yml:")
        for s in newly_added_for_config:
            print(f"    - \"{s}\"")
    else:
        print("\nNo new symbols to add to config.yml (all top active symbols already exist in DB).")

if __name__ == "__main__":
    is_dry = "--dry-run" in sys.argv
    sync_top_symbols(limit=13, dry_run=is_dry)
