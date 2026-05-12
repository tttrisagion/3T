#!/usr/bin/env python3
"""
Rank HIP-3 (XYZ) and PIP symbols by 24h Volume and Open Interest via CCXT.
Returns the top 12 symbols.
"""
import ccxt
import pandas as pd

def rank_symbols():
    print("Initializing Hyperliquid via CCXT...")
    exchange = ccxt.hyperliquid()
    
    print("Loading markets...")
    markets = exchange.load_markets()
    
    # Filter for XYZ- or PIP symbols
    hip3_symbols = [s for s, m in markets.items() if 'XYZ' in s.upper() or 'PIP' in s.upper() or 'XYZ' in m['id'].upper()]
    
    print(f"Found {len(hip3_symbols)} candidate symbols. Fetching stats...")
    
    stats = []
    
    # Fetch all tickers at once for volume
    print("Fetching tickers for volume...")
    try:
        tickers = exchange.fetch_tickers(hip3_symbols)
    except Exception as e:
        print(f"Error fetching tickers: {e}")
        return
    
    for symbol in hip3_symbols:
        try:
            m = markets[symbol]
            ticker = tickers.get(symbol, {})
            
            # Volume in USD (quote volume)
            volume = ticker.get('quoteVolume', 0)
            if volume is None: volume = 0
            
            # Open Interest - requires individual fetch for some exchanges, 
            # but Hyperliquid often includes it in the 'info' of the market or ticker
            # Let's check 'info' first as it's more efficient
            oi = 0
            if 'info' in m and 'openInterest' in m['info']:
                oi = float(m['info']['openInterest'])
            elif 'info' in ticker and 'openInterest' in ticker['info']:
                oi = float(ticker['info']['openInterest'])
            
            # Note: volume is 24h USD, OI is often in base currency, 
            # but for comparison we just want a relative rank.
            # To be more accurate, we'll multiply OI by price if available.
            last_price = ticker.get('last', 0)
            if last_price is None: last_price = 0
            oi_usd = oi * last_price if oi and last_price else 0
            
            stats.append({
                'symbol': symbol,
                'volume_24h': volume,
                'open_interest_usd': oi_usd,
                'last_price': last_price
            })
        except Exception as e:
            print(f"Error fetching stats for {symbol}: {e}")

    if not stats:
        print("No stats collected.")
        return

    df = pd.DataFrame(stats)
    
    # Rank by Volume and OI (normalized composite score)
    if not df.empty:
        # Avoid division by zero
        vol_max = df['volume_24h'].max() if df['volume_24h'].max() > 0 else 1
        oi_max = df['open_interest_usd'].max() if df['open_interest_usd'].max() > 0 else 1
        
        df['vol_rank'] = df['volume_24h'] / vol_max
        df['oi_rank'] = df['open_interest_usd'] / oi_max
        df['score'] = df['vol_rank'] + df['oi_rank']
        
        top_12 = df.sort_values(by='score', ascending=False).head(12)
        
        print("\nTop 12 HIP-3 / PIP Symbols by Activity (Volume + OI):")
        print(f"{'Symbol':<25} {'Volume (24h)':<15} {'Open Interest':<15} {'Last Price'}")
        print("-" * 75)
        for _, row in top_12.iterrows():
            print(f"{row['symbol']:<25} ${row['volume_24h']:>13,.0f} ${row['open_interest_usd']:>13,.0f} {row['last_price']:>10.4f}")
            
        print("\nFormatted for config.yml:")
        for s in top_12['symbol']:
            print(f"    - \"{s}\"")

if __name__ == "__main__":
    rank_symbols()
