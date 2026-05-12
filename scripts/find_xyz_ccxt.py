#!/usr/bin/env python3
"""
Find XYZ and PIP symbols using CCXT on Hyperliquid.
"""
import ccxt

def find_symbols():
    print("Initializing Hyperliquid via CCXT...")
    exchange = ccxt.hyperliquid()
    
    print("Loading markets (this may take a moment)...")
    markets = exchange.load_markets()
    
    print(f"Total markets found: {len(markets)}")
    
    # Filter for symbols containing XYZ or PIP
    # We look at both the CCXT ID and the exchange's original symbol
    xyz_pip_symbols = []
    
    for symbol, market in markets.items():
        # Check the unified CCXT symbol and the underlying ID
        if 'XYZ' in symbol.upper() or 'PIP' in symbol.upper() or 'XYZ' in market['id'].upper():
            xyz_pip_symbols.append({
                'symbol': symbol,
                'id': market['id'],
                'type': market.get('type', 'unknown'),
                'max_leverage': market.get('info', {}).get('maxLeverage', 40)
            })
            
    if not xyz_pip_symbols:
        print("No XYZ or PIP symbols found via CCXT.")
        # Print a few examples to see formatting
        examples = list(markets.keys())[:5]
        print(f"Example symbols: {examples}")
        return

    print(f"\nFound {len(xyz_pip_symbols)} matching symbols:")
    print(f"{'CCXT Symbol':<25} {'Exchange ID':<15} {'Type':<10} {'Max Lev'}")
    print("-" * 65)
    for m in xyz_pip_symbols:
        print(f"{m['symbol']:<25} {m['id']:<15} {m['type']:<10} {m['max_leverage']}")

if __name__ == "__main__":
    find_symbols()
