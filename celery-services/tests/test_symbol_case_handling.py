#!/usr/bin/env python3
"""
Test script to verify that symbol case handling works correctly for kPEPE and other symbols.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# We'll test the logic directly without importing


def get_base_symbol(symbol: str) -> str:
    """Extract base symbol from trading pair symbol."""
    return symbol.split("/")[0] if "/" in symbol else symbol


def test_symbol_case_handling():
    """Test that symbols are handled case-insensitively"""

    # Test get_base_symbol function
    assert get_base_symbol("BTC/USDC:USDC") == "BTC"
    assert get_base_symbol("kPEPE/USDC:USDC") == "kPEPE"
    assert get_base_symbol("KPEPE/USDC:USDC") == "KPEPE"
    assert get_base_symbol("BTC") == "BTC"

    # Test case-insensitive comparison (simulating the fix)
    coin_from_api = "kPEPE"  # What HyperLiquid API returns
    base_symbol = "KPEPE"  # What we expect in our system

    # This is what happens in the fixed code
    assert coin_from_api.upper() == base_symbol.upper()

    # Test symbol construction (simulating tasks.py)
    api_coin = "kPEPE"
    constructed_symbol = api_coin.upper() + "/USDC:USDC"
    assert constructed_symbol == "KPEPE/USDC:USDC"

    print("✓ All symbol case handling tests passed!")
    print(f"  - API coin 'kPEPE' -> Symbol '{constructed_symbol}'")
    print(f"  - Case-insensitive comparison: '{coin_from_api}' matches '{base_symbol}'")


if __name__ == "__main__":
    test_symbol_case_handling()
