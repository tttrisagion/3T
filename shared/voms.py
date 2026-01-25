import numpy as np


class VOMS:
    """
    Virtual Order Management System (VOMS).

    Manages the state of a trading position in-memory to calculate profit/loss
    and other metrics without database interaction.
    """

    def __init__(
        self, starting_balance: float, leverage: float, fee_rate: float = 0.0004
    ):
        """
        Initializes the VOMS.

        Args:
            starting_balance (float): The initial account balance.
            leverage (float): The leverage used for margin calculations.
            fee_rate (float): The transaction fee rate.
        """
        if starting_balance <= 0:
            raise ValueError("Starting balance must be positive.")
        if leverage <= 0:
            raise ValueError("Leverage must be positive.")

        self.starting_balance = starting_balance
        self.leverage = leverage
        self.fee_rate = fee_rate
        self.current_price: float = 0.0

        # Using a list of tuples to store trades: (price, size)
        self.trades: list[tuple[float, float]] = []

    def update_price(self, price: float):
        """
        Updates the current market price.

        Args:
            price (float): The latest market price.
        """
        if price < 0:
            raise ValueError("Price cannot be negative.")
        self.current_price = price

    def add_trade(self, size: float):
        """
        Records a new trade.

        Args:
            size (float): The size of the trade. Positive for a long position,
                          negative for a short position.
        """
        if self.current_price == 0.0:
            raise RuntimeError("Cannot add a trade before the price is first updated.")

        self.trades.append((self.current_price, size))

    def get_metrics(self) -> dict[str, float] | None:
        """
        Calculates and returns the current portfolio metrics.

        Returns:
            A dictionary with the calculated metrics if there are trades,
            otherwise None.
        """
        if not self.trades:
            return None

        trade_prices, trade_sizes = np.array(self.trades).T

        position_size = np.sum(trade_sizes)
        entry_value = np.sum(trade_prices * trade_sizes)

        # Fee is paid on both entry and exit, so we multiply by 2
        total_fees = np.sum(np.abs(trade_prices * trade_sizes) * self.fee_rate * 2)

        unrealized_pnl = (position_size * self.current_price) - entry_value - total_fees

        account_balance = self.starting_balance + unrealized_pnl

        # Cross maintenance margin used
        margin_used = np.abs(entry_value) / self.leverage

        return {
            "position_size": position_size,
            "position_value": entry_value,
            "unrealized_pnl": unrealized_pnl,
            "account_balance": account_balance,
            "margin_used": margin_used,
            "current_price": self.current_price,
        }

    def __repr__(self) -> str:
        return (
            f"VOMS(start_balance={self.starting_balance}, "
            f"leverage={self.leverage}, trades={len(self.trades)}, "
            f"price={self.current_price})"
        )

    def to_dict(self) -> dict:
        """Serializes the VOMS state to a dictionary."""
        return {
            "starting_balance": self.starting_balance,
            "leverage": self.leverage,
            "fee_rate": self.fee_rate,
            "current_price": self.current_price,
            "trades": self.trades,
        }

    def from_dict(self, data: dict):
        """Restores the VOMS state from a dictionary."""
        self.starting_balance = data["starting_balance"]
        self.leverage = data["leverage"]
        self.fee_rate = data["fee_rate"]
        self.current_price = data["current_price"]
        # Convert list of lists back to list of tuples if necessary (JSON load produces lists)
        self.trades = [tuple(t) for t in data["trades"]]
