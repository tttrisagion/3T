"""
HyperLiquid exchange adapter.
Encapsulates all CCXT, public HIP-3 observer consensus, and DeFi-specific logic.
"""

import time
import requests
from datetime import datetime, timedelta
from shared.adapters.base import BaseExchangeAdapter
from shared.config import config
from shared.exchange_manager import exchange_manager
from shared.database import get_redis_connection

class HyperliquidAdapter(BaseExchangeAdapter):
    def fetch_ohlcv(self, symbol: str, timeframe: str, lookback: int) -> list[tuple]:
        """Fetch OHLCV data from CCXT."""
        exchange = exchange_manager.get_exchange("hyperliquid")
        
        # Parse timeframe to milliseconds
        timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
        since = int(time.time() * 1000) - (lookback * timeframe_ms)
        
        return exchange_manager.execute_with_retry(
            exchange.fetchOHLCV, symbol, timeframe, since
        )

    def fetch_and_store_balance(self, cursor, exchange_id: int) -> tuple[float, float]:
        """Fetch HyperLiquid balance and positions, and store to Database."""
        exchange = exchange_manager.get_exchange("hyperliquid")

        # Fetch native perp balance
        exchange_status = exchange_manager.execute_with_retry(
            exchange.fetch_balance
        )

        # Load markets for coin mapping
        if not exchange.markets:
            exchange_manager.execute_with_retry(exchange.load_markets)

        # Fetch HIP-3 dex states
        hip3_dexes = ["xyz"]
        hip3_states = []
        for dex in hip3_dexes:
            try:
                hip3_state = exchange_manager.execute_with_retry(
                    lambda d=dex: exchange.public_post_info(
                        {
                            "type": "clearinghouseState",
                            "user": exchange.walletAddress,
                            "dex": d,
                        }
                    )
                )
                if hip3_state:
                    hip3_states.append(hip3_state)
                    if hip3_state.get("assetPositions"):
                        exchange_status["info"].setdefault(
                            "assetPositions", []
                        ).extend(hip3_state["assetPositions"])
            except Exception as e:
                print(f"Error fetching HIP-3 dex '{dex}': {e}")

        # Compute aggregate account value and margin
        account_value = float(
            exchange_status["info"]["marginSummary"]["accountValue"]
        )
        margin_used = float(exchange_status["info"]["crossMaintenanceMarginUsed"])

        for hip3_state in hip3_states:
            account_value += float(
                hip3_state.get("marginSummary", {}).get("accountValue", 0)
            )
            margin_used += float(
                hip3_state.get("marginSummary", {}).get("totalMarginUsed", 0)
            )

        # Clean positions only for HyperLiquid
        cursor.execute(
            "DELETE FROM positions WHERE product_id IN (SELECT id FROM products WHERE exchange_id = %s)",
            (exchange_id,)
        )

        cursor.execute(
            "SELECT id, symbol FROM products WHERE exchange_id = %s", (exchange_id,)
        )
        products = {row["symbol"]: row["id"] for row in cursor.fetchall()}

        api_coin_to_symbol = {}
        for sym in products:
            if sym in exchange.markets:
                api_coin = exchange.markets[sym]["info"].get("name", "")
                if api_coin:
                    api_coin_to_symbol[api_coin.upper()] = sym

        if len(exchange_status["info"]["assetPositions"]) > 0:
            for pos in exchange_status["info"]["assetPositions"]:
                api_coin = pos["position"]["coin"]
                symbol = api_coin_to_symbol.get(api_coin.upper())
                if symbol is None:
                    coin_name = api_coin.upper().replace(":", "-")
                    symbol = coin_name + "/USDC:USDC"
                if symbol in products:
                    query = "INSERT INTO positions (product_id, position_size, position_value, unrealized_pnl) VALUES (%s, %s, %s, %s)"
                    cursor.execute(
                        query,
                        (
                            products[symbol],
                            pos["position"]["szi"],
                            pos["position"]["positionValue"],
                            float(pos["position"]["unrealizedPnl"]),
                        ),
                    )

        return account_value, margin_used

    def is_market_open(self, symbol: str) -> bool:
        """DeFi market is open 24/7."""
        return True

    def get_observer_state(self, symbol: str, wallet_id: str = None) -> tuple[float | None, str | None, float]:
        """Query decentralized observer nodes for consensus state."""
        if not wallet_id:
            wallet_id = config.get_secret("exchanges.hyperliquid.walletAddress")
            if not wallet_id:
                wallet_id = config.get("exchanges.hyperliquid.wallet_address")

        if not wallet_id:
            return None, "No wallet address configured", 0.0

        observer_nodes = config.get(
            "reconciliation_engine.observer_nodes",
            ["http://exchange-observer:8001/3T-observer.json"],
        )
        max_heartbeat_age = timedelta(minutes=5)

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
        """Clamp DeFi desired position to per-symbol margin cap if usage exceeds budget."""
        if symbol_cap > 0 and symbol_margin_used > symbol_cap and abs(desired_position) > 1e-8:
            scale = symbol_cap / symbol_margin_used
            original = desired_position
            desired_position = desired_position * scale
            print(
                f"DeFi Margin Clamp: {symbol} using ${symbol_margin_used:.2f} of "
                f"${symbol_cap:.2f} cap. Scaling desired position from {original:.4f} to {desired_position:.4f}."
            )
        return desired_position

    def cancel_all_open_orders(self, symbols: list[str]) -> int:
        """Bypassed in production to prevent sequential rate-limiting floods."""
        return 0
