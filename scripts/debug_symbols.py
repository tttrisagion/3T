#!/usr/bin/env python3
import json
from hyperliquid.info import Info

def debug():
    info = Info()
    print("Checking PERP Meta...")
    meta = info.meta()
    universe = meta.get("universe", [])
    names = [u["name"] for u in universe]
    
    intc_perps = [n for n in names if "INTC" in n.upper()]
    xyz_perps = [n for n in names if "XYZ" in n.upper()]
    
    print(f"Total Perps: {len(names)}")
    print(f"Perps with 'INTC': {intc_perps}")
    print(f"Perps with 'XYZ': {xyz_perps}")
    
    print("\nChecking SPOT Meta...")
    try:
        spot_meta = info.spot_meta()
        spot_universe = spot_meta.get("universe", [])
        spot_names = [u["name"] for u in spot_universe]
        intc_spot = [n for n in spot_names if "INTC" in n.upper()]
        xyz_spot = [n for n in spot_names if "XYZ" in n.upper()]
        print(f"Total Spot: {len(spot_names)}")
        print(f"Spot with 'INTC': {intc_spot}")
        print(f"Spot with 'XYZ': {xyz_spot}")
    except Exception as e:
        print(f"Spot meta error: {e}")

if __name__ == "__main__":
    debug()
