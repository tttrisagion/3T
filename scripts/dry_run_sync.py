#!/usr/bin/env python3
"""
DRY RUN: Sync HIP-3 (XYZ) symbols from Hyperliquid to the 3T database.
Shows what WOULD be created without actually modifying the database.
"""

import os
import sys
import mysql.connector
from hyperliquid.info import Info

# Database configuration from environment or defaults
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "secret")
DB_NAME = os.environ.get("DB_NAME", "3t")

def dry_run_sync():
    print("=== DRY RUN MODE: No changes will be saved ===\n")
    print("Fetching metadata from Hyperliquid...")
    try:
        info = Info()
        meta = info.meta()
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return
    
    universe = meta.get("universe", [])
    hip3_symbols = [u for u in universe if u["name"].startswith("XYZ-") or u["name"] == "PIP"]
    
    if not hip3_symbols:
        print("No HIP-3 (XYZ-) or PIP symbols found on Hyperliquid.")
        return

    print(f"Found {len(hip3_symbols)} potential symbols to sync.\n")

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
        print(f"Error connecting to database: {e}")
        print("Note: If running outside docker, ensure DB_HOST is set correctly (e.g. 127.0.0.1).")
        sys.exit(1)

    # Get HyperLiquid exchange ID
    cursor.execute("SELECT id FROM exchanges WHERE name = 'HyperLiquid'")
    exchange = cursor.fetchone()
    if not exchange:
        print("Error: 'HyperLiquid' exchange not found in database.")
        sys.exit(1)
    exchange_id = exchange["id"]

    would_add_instr = 0
    would_add_prod = 0
    skipped_count = 0
    potential_symbols = []

    for u in hip3_symbols:
        hl_name = u["name"]
        max_lev = float(u.get("maxLeverage", 40.0))
        
        instrument_name = hl_name.replace("XYZ-", "")
        product_symbol = f"{hl_name}/USDC:USDC"
        
        # 1. Check instrument
        cursor.execute("SELECT id FROM instruments WHERE name = %s", (instrument_name,))
        instr = cursor.fetchone()
        instr_status = "[EXISTS]" if instr else "[NEW INSTRUMENT]"
        if not instr:
            would_add_instr += 1

        # 2. Check product
        cursor.execute(
            "SELECT id FROM products WHERE exchange_id = %s AND symbol = %s",
            (exchange_id, product_symbol)
        )
        prod = cursor.fetchone()
        
        if not prod:
            print(f"{instr_status.ljust(17)} Would add product: {product_symbol.ljust(20)} (max_leverage={max_lev})")
            would_add_prod += 1
            potential_symbols.append(product_symbol)
        else:
            skipped_count += 1

    db_cnx.close()
    print(f"\nDry Run Complete!")
    print(f"Would add instruments: {would_add_instr}")
    print(f"Would add products:    {would_add_prod}")
    print(f"Already exist:         {skipped_count}")
    
    if potential_symbols:
        print("\nSymbols that would be ready for activation in config.yml:")
        for s in potential_symbols:
            print(f"    - \"{s}\"")

if __name__ == "__main__":
    dry_run_sync()
