#!/usr/bin/env python3
import json
from hyperliquid.info import Info

def check():
    print("Fetching metadata from Hyperliquid...")
    info = Info()
    meta = info.meta()
    universe = meta.get("universe", [])
    
    names = [u["name"] for u in universe]
    print(f"Total symbols in universe: {len(names)}")
    
    xyz_names = [n for n in names if "XYZ" in n.upper()]
    pip_names = [n for n in names if "PIP" in n.upper()]
    
    print(f"\nNames containing 'XYZ': {xyz_names}")
    print(f"Names containing 'PIP': {pip_names}")
    
    if len(names) > 0:
        print(f"\nFirst 10 symbols: {names[:10]}")

if __name__ == "__main__":
    check()
