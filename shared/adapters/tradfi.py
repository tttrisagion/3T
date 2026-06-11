"""
TradFi exchange adapter.
Encapsulates Interactive Brokers, DTN IQFeed, US stock market hours,
whole-share integer rounding, and USD position value margin-clamping.
"""

import time
import requests
from datetime import datetime, timedelta, time as datetime_time
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from shared.adapters.base import BaseExchangeAdapter
from shared.config import config
from shared.ib_client import ib_client
from shared.iqfeed_client import iqfeed_client
from shared.database import get_db_connection, get_redis_connection

class TradfiAdapter(BaseExchangeAdapter):
    def fetch_ohlcv(self, symbol: str, timeframe: str, lookback: int) -> list[tuple]:
        """Fetch historical interval OHLCV data from IQFeed."""
        return iqfeed_client.fetch_ohlcv(symbol, timeframe, lookback)

    def fetch_and_store_balance(self, cursor, exchange_id: int) -> tuple[float, float]:
        """Fetch IB balance and positions, and store to Database."""
        balance_info, positions = ib_client.get_balance_and_positions()
        account_value = balance_info["account_value"]
        margin_used = balance_info["cross_maintenance_margin_used"]

        # Clean positions only for TradFi
        cursor.execute(
            "DELETE FROM positions WHERE product_id IN (SELECT id FROM products WHERE exchange_id = %s)",
            (exchange_id,)
        )

        cursor.execute(
            "SELECT id, symbol FROM products WHERE exchange_id = %s", (exchange_id,)
        )
        products = {row["symbol"]: row["id"] for row in cursor.fetchall()}

        for pos in positions:
            symbol = pos["symbol"]
            if symbol in products:
                query = "INSERT INTO positions (product_id, position_size, position_value, unrealized_pnl) VALUES (%s, %s, %s, %s)"
                cursor.execute(
                    query,
                    (
                        products[symbol],
                        pos["position_size"],
                        pos["position_value"],
                        pos["unrealized_pnl"],
                    ),
                )

        return account_value, margin_used

    def is_market_open(self, symbol: str) -> bool:
        """
        Check if the US stock market is open for active, safe reconciliation.
        Trading Hours: Monday - Friday, 9:30 AM to 3:55 PM Eastern Time (EST/EDT).
        Excludes weekends and US holidays.
        """
        if config.get("trading_hours.test_mode", False):
            return True

        try:
            tz = ZoneInfo("America/New_York")
        except Exception:
            tz = ZoneInfo("US/Eastern")

        now = datetime.now(tz)
        d = now.date()

        # Fixed holidays
        if (d.month == 1 and d.day == 1) or (d.month == 12 and d.day == 25):
            return False

        # Weekends
        if now.weekday() >= 5:
            return False

        current_time = now.time()
        start_time = datetime_time(9, 30, 0)
        end_time = datetime_time(15, 55, 0)

        return start_time <= current_time <= end_time

    def get_observer_state(self, symbol: str, wallet_id: str = None) -> tuple[float | None, str | None, float]:
        """Query our own local independent private observer node for TradFi positions."""
        observer_nodes = ["http://exchange-observer:8001/3T-observer.json"]
        max_heartbeat_age = timedelta(minutes=5)
        wallet_id = "tradfi"

        for observer_url in observer_nodes:
            try:
                response = requests.get(observer_url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    
                    # Validate heartbeat
                    hb_time_str = data.get("timestamp")
                    if not hb_time_str:
                        continue
                    hb_time = datetime.fromisoformat(hb_time_str.replace("Z", "+00:00"))
                    if datetime.now(hb_time.tzinfo) - hb_time > max_heartbeat_age:
                        continue

                    positions_dict = data.get("positions", {})
                    if wallet_id not in positions_dict:
                        continue

                    wallet_data = positions_dict.get(wallet_id, {})
                    asset_positions = wallet_data.get("assetPositions", [])

                    from worker.reconciliation_engine import get_api_coin
                    api_coin = get_api_coin(symbol)

                    for asset_pos in asset_positions:
                        pos_info = asset_pos.get("position", {})
                        if pos_info.get("coin", "").upper() == api_coin.upper():
                            pos_size = float(pos_info.get("szi", 0.0))
                            margin_used = float(pos_info.get("marginUsed", 0.0))
                            return pos_size, None, margin_used

                    return 0.0, None, 0.0
            except Exception as e:
                print(f"Warning: Failed to query observer node {observer_url}: {e}")

        return None, "All observer nodes failed or returned stale data", 0.0

    def apply_position_clamping_and_rounding(self, symbol: str, desired_position: float, symbol_cap: float, symbol_margin_used: float = 0.0) -> float:
        """Clamp TradFi position based on USD value (instead of margin) and round to whole shares."""
        if symbol_cap > 0 and abs(desired_position) > 1e-8:
            price = self._get_current_price(symbol)
            proposed_usd_value = abs(desired_position) * price
            
            if proposed_usd_value > symbol_cap:
                scale = symbol_cap / proposed_usd_value
                original = desired_position
                desired_position = desired_position * scale
                print(
                    f"TradFi Margin Clamp: Scaling desired position for {symbol} "
                    f"from {original:.4f} to {desired_position:.4f} to respect "
                    f"the ${symbol_cap:.2f} symbol budget (estimated price: ${price:.2f})."
                )

        # Enforce whole integer share quantities for TradFi stocks to prevent fractional execution rejection
        desired_position = float(round(desired_position))
        return desired_position

    def _get_current_price(self, symbol: str) -> float:
        """Helper to get current price from Redis or DB."""
        try:
            with get_redis_connection() as r:
                p_bytes = r.get(f"price:{symbol}")
                if p_bytes:
                    return float(p_bytes)
        except Exception:
            pass

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT close FROM market_data WHERE symbol = %s ORDER BY timestamp DESC LIMIT 1",
                    (symbol,)
                )
                row = cursor.fetchone()
                if row:
                    return float(row[0])
        except Exception:
            pass

        return 0.0

    def cancel_all_open_orders(self, symbols: list[str]) -> int:
        """Bypassed in production to prevent sequential rate-limiting floods."""
        return 0
