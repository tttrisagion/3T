# This file can be used as an example for building a strategy run

from shared.voms import VOMS


def run_voms_test():
    """Demonstrates the usage of the VOMS class."""
    print("--- Initializing VOMS ---")
    # Initialize VOMS with a starting balance of $10,000, 10x leverage
    voms = VOMS(starting_balance=10000, leverage=10)
    print(voms)
    print("\n")

    # --- Scenario: Open a new long position ---
    print("--- Scenario 1: Open Long Position ---")
    print("Updating price to 100...")
    voms.update_price(100)

    print("Adding a trade: size = 10 (long)")
    voms.add_trade(size=10)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print("\n")

    # --- Scenario: Price goes up ---
    print("--- Scenario 2: Price Increases ---")
    print("Updating price to 110...")
    voms.update_price(110)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print("\n")

    # --- Scenario: Add to the long position ---
    print("--- Scenario 3: Increase Long Position ---")
    print("Adding a trade: size = 5 (long)")
    voms.add_trade(size=5)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print("\n")

    # --- Scenario: Price goes down ---
    print("--- Scenario 4: Price Decreases ---")
    print("Updating price to 95...")
    voms.update_price(95)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print("\n")

    # --- Scenario: Partially close the position ---
    print("--- Scenario 5: Partially Close Position ---")
    print("Adding a trade: size = -8 (short)")
    voms.add_trade(size=-8)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print(voms)
    print("\n")

    # --- Scenario: Smaller position goes up now ---
    print("--- Scenario 6: Smaller position goes up now---")
    voms.update_price(110)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print(voms)
    print("\n")

    # --- Scenario: Go fully short---
    print("--- Scenario 7: Go fully short---")
    voms.add_trade(size=-17)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print(voms)
    print("\n")

    # --- Scenario: Short position draws down ---
    print("--- Scenario 8: Short position draws down ---")
    voms.update_price(115)

    metrics = voms.get_metrics()
    print("VOMS Metrics:", metrics)
    print(voms)
    print("\n")


if __name__ == "__main__":
    run_voms_test()
