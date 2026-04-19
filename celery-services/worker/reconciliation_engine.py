"""
Reconciliation Engine - The brain of the trading system.

This module continuously compares the Desired State of the portfolio with the
Actual State (determined by consensus) and generates orders to close any gap.
It operates without explicit locking, relying on idempotent execution and
state consensus for safety.
"""

import os
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import redis
import requests
from opentelemetry import trace

from shared.celery_app import app
from shared.config import config
from shared.database import get_db_connection, get_redis_connection
from shared.exchange_manager import exchange_manager
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


def is_market_open() -> bool:
    """
    Checks if the market is open based on Vatican City time (CET/CEST).
    Schedule: Continuous from 00:00 Monday to 23:00 Friday.
    Closed: Friday 23:00 - Monday 00:00.
    Holidays (Closed All Day): Jan 1, Dec 25, Good Friday.

    If trading_hours.test_mode is True in config.yml, always returns True.
    """
    from shared.config import config as _cfg

    if _cfg.get("trading_hours.test_mode", False):
        return True

    try:
        # Vatican uses Central European Time
        tz = ZoneInfo("Europe/Vatican")
    except Exception:
        tz = ZoneInfo("Europe/Rome")

    now = datetime.now(tz)
    d = now.date()

    # --- 1. Check Fixed Holidays ---
    if (d.month == 1 and d.day == 1) or (d.month == 12 and d.day == 25):
        return False

    # --- 2. Check Variable Holiday (Good Friday) ---
    # Anonymous Gregorian algorithm for Easter
    y = d.year
    a, b, c = y % 19, y // 100, y % 100
    d_val, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d_val - g + 15) % 30
    i, k = c // 4, c % 4
    l_val = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_val) // 451
    month = (h + l_val - 7 * m + 114) // 31
    day = ((h + l_val - 7 * m + 114) % 31) + 1

    easter = date(y, month, day)
    good_friday = easter - timedelta(days=2)

    if d == good_friday:
        return False

    # --- 3. Check Weekly Schedule ---
    # Weekdays: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    wd = now.weekday()

    # Closed Saturday (5) and Sunday (6)
    if wd >= 5:
        return False

    # Closed Friday (4) after 23:00
    if wd == 4 and now.hour >= 23:
        return False

    # Otherwise Open (Mon 00:00 -> Fri 22:59)
    return True


def _calculate_kelly_metrics(
    condition: str, symbol: str
) -> tuple[float | None, float | None, float | None]:
    """
    Calculate Kelly criterion metrics for runs matching a specific condition.
    Args:
        condition: A SQL WHERE clause condition (e.g., 'height IS NULL').
    Returns:
        Tuple of (win_rate, reward_ratio, kelly_percentage)
        Returns (None, None, None) if insufficient data.
    """
    with tracer.start_as_current_span(
        f"calculate_kelly_metrics_for_{condition}"
    ) as span:
        span.set_attribute("sql_condition", condition)
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Positive Expectancy Gate to filter out
                # entire heights/blocks that lose money on aggregate
                # preventing a bad null height cohort from looking
                # better than it should
                regime_filter = ""
                if "height IS NOT NULL" in condition:
                    regime_filter = f"""
                    AND height IN (
                        SELECT height
                        FROM runs
                        WHERE symbol = '{symbol}'
                          AND height IS NOT NULL
                        GROUP BY height
                        HAVING SUM(live_pnl) > 0
                    )
                    """

                query = f"""
                SELECT live_pnl
                FROM runs
                WHERE {condition}
                  AND live_pnl IS NOT NULL
                  AND live_pnl <> 0
                  AND symbol = '{symbol}'
                  {regime_filter}
                """

                cursor.execute(query)
                results = cursor.fetchall()

                if not results or len(results) < 10:  # Need minimum sample size
                    span.add_event(
                        "Insufficient data for Kelly calculation",
                        {
                            "condition": condition,
                            "sample_size": len(results) if results else 0,
                        },
                    )
                    return None, None, None

                # Separate wins and losses
                wins = [float(pnl[0]) for pnl in results if float(pnl[0]) > 0]
                losses = [abs(float(pnl[0])) for pnl in results if float(pnl[0]) < 0]

                if len(wins) == 0 or len(losses) == 0:
                    span.add_event(
                        "No wins or no losses found",
                        {"wins": len(wins), "losses": len(losses)},
                    )
                    return None, None, None

                # Calculate metrics
                total_trades = len(results)
                win_rate = len(wins) / total_trades
                avg_win = sum(wins) / len(wins)
                avg_loss = sum(losses) / len(losses)

                # Avoid division by zero
                if avg_loss == 0:
                    span.add_event(
                        "Average loss is zero - cannot calculate reward ratio"
                    )
                    return None, None, None

                reward_ratio = avg_win / avg_loss

                # Kelly formula: win_rate - ((1 - win_rate) / reward_ratio)
                kelly_percentage = win_rate - ((1 - win_rate) / reward_ratio)

                span.add_event(
                    "Kelly metrics calculated",
                    {
                        "total_trades": total_trades,
                        "wins": len(wins),
                        "losses": len(losses),
                        "win_rate": win_rate,
                        "avg_win": avg_win,
                        "avg_loss": avg_loss,
                        "reward_ratio": reward_ratio,
                        "kelly_percentage": kelly_percentage,
                    },
                )

                return win_rate, reward_ratio, kelly_percentage

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error calculating Kelly metrics: {e}")
            return None, None, None


def calculate_kelly_position_size(base_risk_pos_size: float, symbol: str) -> float:
    """
    Calculate Kelly-adjusted position size based on performance metrics.
    Args:
        base_risk_pos_size: Base position size before Kelly adjustment
    Returns:
        Kelly-adjusted position size
    """
    with tracer.start_as_current_span("calculate_kelly_position_size") as span:
        span.set_attribute("base_risk_pos_size", base_risk_pos_size)

        try:
            # 1. Get Kelly metric for current (null height) runs
            _, _, kelly_current = _calculate_kelly_metrics(
                "height IS NULL AND exit_run = 0", symbol
            )

            # 2. Get Kelly metric for historical (non-null height) runs
            _, _, kelly_historical = _calculate_kelly_metrics(
                "height IS NOT NULL", symbol
            )

            span.set_attributes(
                {
                    "kelly_current": kelly_current if kelly_current else 0,
                    "kelly_historical": kelly_historical if kelly_historical else 0,
                }
            )

            if kelly_current is None or kelly_historical is None:
                span.add_event("No valid Kelly data - using probationary base size")
                # we don't have enough data so treat symbol as "High Risk"
                # allocate reduced risk until we have more historical norming
                return base_risk_pos_size * 0.5

            if kelly_historical == 0:
                span.add_event(
                    "Historical Kelly is zero, cannot compute performance ratio. Using base size."
                )
                return base_risk_pos_size

            # Calculate performance based on the original formula
            kelly_performance = 1 - (kelly_current / kelly_historical)
            if kelly_current < 0 and kelly_historical > 0:
                kelly_performance = kelly_performance * -1
            if kelly_current > 0 and kelly_historical > 0:
                kelly_performance = kelly_performance * -1

            span.add_event(
                "Calculated Kelly performance",
                {"kelly_performance": kelly_performance},
            )

            # Get configuration for thresholding
            reconciliation_config = config.get("reconciliation_engine", {})
            kelly_threshold = reconciliation_config.get("kelly_threshold", 0.5)

            # Apply threshold cap
            if kelly_performance > kelly_threshold:
                kelly_performance = kelly_threshold
                span.add_event("Capping Kelly performance at threshold")

            # Add safety cap to prevent position flipping
            if kelly_performance < -1.0:
                kelly_performance = -0.98
                span.add_event(
                    "Flooring Kelly performance at -0.98 to prevent flip but allow pass through sampling"
                )

            # Calculate adjusted position size
            kelly_risk_pos_size = base_risk_pos_size + (
                base_risk_pos_size * kelly_performance
            )

            span.add_event(
                "Kelly position size calculated",
                {
                    "kelly_performance_adjustment": kelly_performance,
                    "final_kelly_risk_pos_size": kelly_risk_pos_size,
                },
            )

            return kelly_risk_pos_size

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error calculating Kelly position size: {e}")
            return base_risk_pos_size


def get_latest_margin_usage() -> float | None:
    """
    Get the latest cross maintenance margin used from the balance_history table.
    """
    with tracer.start_as_current_span("get_latest_margin_usage") as span:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                query = """
                SELECT cross_maintenance_margin_used FROM balance_history
                ORDER BY timestamp DESC
                LIMIT 1
                """
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    margin = float(result[0])
                    span.set_attribute("margin_usage", margin)
                    return margin
                span.add_event("No margin information found in balance_history.")
                return None
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error getting latest margin usage: {e}")
            return None


def get_latest_balance() -> float | None:
    """
    Get the latest total balance from the Redis stream.
    Returns:
        Latest total balance, or None if not available.
    """
    try:
        redis_host = config.get("redis.host", "redis")
        redis_port = config.get("redis.port", 6379)
        redis_db = config.get("redis.db", 0)
        stream_name = config.get("redis.streams.balance_updates", "balance:updated")

        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        # Get the last entry in the stream
        messages = r.xrevrange(stream_name, count=1)

        if messages:
            latest_message = messages[0][1]
            balance = float(latest_message.get(b"account_value", 0.0))
            return balance
        return None
    except (redis.exceptions.RedisError, ValueError) as e:
        print(f"Error getting latest balance from Redis: {e}")
        return None


def get_base_symbol(symbol: str) -> str:
    """
    Extract base symbol from trading pair symbol.
    Args:
        symbol: Symbol like 'BTC/USDC:USDC' or 'BTC'
    Returns:
        Base symbol like 'BTC'
    """
    return symbol.split("/")[0] if "/" in symbol else symbol


def get_api_coin(symbol: str) -> str:
    """Get the HyperLiquid API coin name for a trading symbol.
    Uses CCXT market info for native perps. For HIP-3 symbols (base contains
    a dash like XYZ-CL), converts to API format (xyz:CL) since CCXT doesn't
    load HIP-3 markets.
    """
    exchange = exchange_manager.get_exchange("hyperliquid")
    if exchange.markets and symbol in exchange.markets:
        return exchange.markets[symbol]["info"].get("name", get_base_symbol(symbol))
    # HIP-3 symbols: DB base is "XYZ-CL", API coin is "xyz:CL"
    base = get_base_symbol(symbol)
    if "-" in base:
        return base.replace("-", ":", 1)
    return base


def get_desired_state(symbol: str) -> float:
    """
    Calculate the desired position size based on active runs in the database.
    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')
    Returns:
        Target position size (positive for long, negative for short, 0 for flat)
    """
    with tracer.start_as_current_span("get_desired_state") as span:
        span.set_attribute("symbol", symbol)

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Query from the spec using full symbol format
                query = """
                SELECT symbol,
                       SUM(position_direction) as position,
                       SUM(live_pnl) as total_pnl,
                       COUNT(*) as runs
                FROM runs
                WHERE exit_run = 0
                  AND height IS NULL
                  AND end_time IS NULL
                  AND live_pnl > 0.2
                  AND abs(position_direction) > 0
                  AND symbol = %s
                  AND update_time >= NOW() - INTERVAL 10 MINUTE
                HAVING total_pnl > 0 AND ABS(position) >= 1
                """

                cursor.execute(query, (symbol,))
                result = cursor.fetchone()

                if result and result[1] is not None:  # position column
                    # Get risk position size from balance percentage
                    risk_pos_percentage = config.get(
                        "reconciliation_engine.risk_pos_percentage", 0.0016180339887
                    )
                    latest_balance = get_latest_balance()

                    if latest_balance:
                        base_risk_pos_size = latest_balance * risk_pos_percentage
                        # Apply Kelly criterion for position sizing
                        risk_pos_size = calculate_kelly_position_size(
                            base_risk_pos_size, symbol
                        )
                        span.add_event(
                            "Calculated Kelly-adjusted risk_pos_size from balance",
                            {
                                "latest_balance": latest_balance,
                                "risk_pos_percentage": risk_pos_percentage,
                                "base_risk_pos_size": base_risk_pos_size,
                                "kelly_adjusted_risk_pos_size": risk_pos_size,
                            },
                        )
                    else:
                        # Fallback to a small default value if balance is not available
                        base_risk_pos_size = 0.01
                        risk_pos_size = calculate_kelly_position_size(
                            base_risk_pos_size, symbol
                        )
                        span.add_event(
                            "Failed to get latest balance, using Kelly-adjusted fallback",
                            {
                                "fallback_base_risk_pos_size": base_risk_pos_size,
                                "kelly_adjusted_risk_pos_size": risk_pos_size,
                            },
                        )

                    position_direction = float(result[1])  # position from query

                    # Get current price for position sizing
                    instrument_price = get_current_price(symbol)
                    if instrument_price is None:
                        span.add_event(
                            "Failed to get current price", {"symbol": symbol}
                        )
                        return 0.0

                    # Calculate target position size in units (not USD)
                    target_position = (
                        position_direction * risk_pos_size / instrument_price
                    )

                    # Apply can_go_long and can_go_short flags
                    can_go_long = config.get("reconciliation_engine.can_go_long", True)
                    can_go_short = config.get(
                        "reconciliation_engine.can_go_short", True
                    )

                    if not can_go_long and target_position > 0:
                        span.add_event(
                            "Desired long position blocked by can_go_long=False",
                            {"original_target": target_position},
                        )
                        target_position = 0.0
                    elif not can_go_short and target_position < 0:
                        span.add_event(
                            "Desired short position blocked by can_go_short=False",
                            {"original_target": target_position},
                        )
                        target_position = 0.0

                    span.set_attribute("desired_position", target_position)
                    span.add_event(
                        "Calculated desired state",
                        {
                            "position_direction": position_direction,
                            "risk_pos_size": risk_pos_size,
                            "instrument_price": instrument_price,
                            "target_position": target_position,
                        },
                    )

                    return target_position

                span.add_event("No active runs found for symbol")
                return 0.0

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error getting desired state for {symbol}: {e}")
            return 0.0


def get_current_price(symbol: str) -> float | None:
    """
    Get current price for a symbol from market data table.
    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')
    Returns:
        Current price or None if unavailable
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Try with full symbol first
            query = """
            SELECT close FROM market_data
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """

            cursor.execute(query, (symbol,))
            result = cursor.fetchone()

            if result:
                return float(result[0])

            # Try with base symbol if full symbol doesn't work
            base_symbol = get_base_symbol(symbol)
            cursor.execute(query, (base_symbol,))
            result = cursor.fetchone()

            if result:
                return float(result[0])

            return None

    except Exception as e:
        print(f"Error getting current price for {symbol}: {e}")
        return None


def _get_symbol_margin_caps(symbols: list[str], max_margin: float) -> dict[str, float]:
    """
    Compute per-symbol margin caps using leverage-weighted fractions from the products table.

    Args:
        symbols: List of trading symbols
        max_margin: Total margin budget in USD (balance * max_margin_usage_percentage)

    Returns:
        Dict of {symbol: cap_in_usd}. Empty dict on failure (caller should skip per-symbol check).
    """
    if not symbols or max_margin <= 0:
        return {}

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            placeholders = ", ".join(["%s"] * len(symbols))
            cursor.execute(
                f"SELECT symbol, max_leverage FROM products WHERE symbol IN ({placeholders})",
                tuple(symbols),
            )
            leverage_map = {}
            for row in cursor.fetchall():
                lev = float(row[1]) if row[1] else 1.0
                leverage_map[str(row[0])] = max(lev, 1.0)

        # Fill missing symbols with leverage=1
        for s in symbols:
            if s not in leverage_map:
                leverage_map[s] = 1.0

        total_leverage = sum(leverage_map.values())
        if total_leverage <= 0:
            return {}

        return {
            symbol: (leverage_map[symbol] / total_leverage) * max_margin
            for symbol in symbols
        }

    except Exception as e:
        print(f"Error computing symbol margin caps: {e}")
        return {}


def get_observer_state(
    symbol: str,
) -> tuple[float | None, str | None, float]:
    """
    Get position from external observer node and perform safety checks.

    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')

    Returns:
        Tuple of (position_size, error_message, margin_used)
        - position_size: Position size, or None if validation fails.
        - error_message: A string describing the error, or None if successful.
        - margin_used: Margin used for this position (0.0 if unavailable).
    """
    observer_nodes = config.get(
        "reconciliation_engine.observer_nodes",
        ["http://exchange-observer:8001/3T-observer.json"],
    )
    wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")
    max_heartbeat_age = timedelta(minutes=5)

    if not wallet_address:
        return None, "No wallet address configured", 0.0

    for observer_url in observer_nodes:
        try:
            response = requests.get(observer_url, timeout=5)
            response.raise_for_status()
            observer_data = response.json()

            # Check heartbeat
            timestamp_str = observer_data.get("timestamp")
            if not timestamp_str:
                return None, f"Observer {observer_url} has no timestamp", 0.0

            timestamp = datetime.fromisoformat(timestamp_str)
            if datetime.now(UTC) - timestamp > max_heartbeat_age:
                return None, f"Observer {observer_url} data is stale", 0.0

            # Check for wallet presence
            positions = observer_data.get("positions", {})
            if wallet_address not in positions:
                return (
                    None,
                    f"Wallet {wallet_address} not found in observer {observer_url}",
                    0.0,
                )

            # Extract position
            wallet_data = positions.get(wallet_address, {})
            asset_positions = wallet_data.get("assetPositions", [])
            api_coin = get_api_coin(symbol)

            for asset_pos in asset_positions:
                position_data = asset_pos.get("position", {})
                if position_data.get("coin", "").upper() == api_coin.upper():
                    margin_used = float(position_data.get("marginUsed", 0))
                    return float(position_data.get("szi", 0)), None, margin_used

            # If no position found, it's a flat position
            return 0.0, None, 0.0

        except requests.RequestException as e:
            print(f"Could not connect to observer {observer_url}: {e}")
            continue  # Try next observer
        except (ValueError, KeyError) as e:
            return None, f"Invalid data from observer {observer_url}: {e}", 0.0

    return None, "All observers failed", 0.0


def get_actual_state(symbol: str) -> tuple[float | None, bool, float]:
    """
    Get the actual position state using consensus between local positions table
    and external observer node.
    Args:
        symbol: The trading symbol
    Returns:
        Tuple of (position_size, has_consensus, margin_used)
    """
    with tracer.start_as_current_span("get_actual_state") as span:
        span.set_attribute("symbol", symbol)

        try:
            # Get position from local database (last 15 seconds)
            local_position = get_local_position(symbol)
            span.set_attribute(
                "local_position", local_position if local_position else 0.0
            )

            # Get position from observer node
            observer_position, error_message, margin_used = get_observer_state(symbol)

            if error_message:
                span.add_event("Observer validation failed", {"error": error_message})
                print(f"Observer validation failed for {symbol}: {error_message}")
                return None, False, 0.0

            span.set_attribute(
                "observer_position", observer_position if observer_position else 0.0
            )

            # Check for consensus (identical positions)
            # Convert None to 0.0 for comparison (no position = 0 position)
            local_pos = local_position if local_position is not None else 0.0
            observer_pos = observer_position if observer_position is not None else 0.0

            # Check if positions agree (with small tolerance for floating point)
            if abs(local_pos - observer_pos) < 1e-8:  # 1e-8 tolerance
                consensus_position = local_pos  # Use local position as canonical
                span.set_attribute("consensus_position", consensus_position)
                span.add_event(
                    "Consensus achieved",
                    {
                        "position": consensus_position,
                        "local_position": local_pos,
                        "observer_position": observer_pos,
                    },
                )
                return consensus_position, True, margin_used

            span.add_event(
                "No consensus",
                {
                    "local_position": local_pos,
                    "observer_position": observer_pos,
                    "difference": abs(local_pos - observer_pos),
                },
            )
            return None, False, 0.0

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error getting actual state for {symbol}: {e}")
            return None, False, 0.0


def get_local_position(symbol: str) -> float | None:
    """
    Get position from local positions table (last 15 seconds).
    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')
    Returns:
        Position size or None if not found
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Get the product_id for this symbol (use full symbol)
            query = """
            SELECT id FROM products
            WHERE symbol = %s
            """
            cursor.execute(query, (symbol,))
            product_result = cursor.fetchone()

            if not product_result:
                return None

            product_id = product_result[0]

            # Get latest position within configured staleness timeout
            staleness_timeout = config.get(
                "reconciliation_engine.position_staleness_timeout", 300
            )
            cutoff_time = datetime.now(UTC) - timedelta(seconds=staleness_timeout)
            query = """
            SELECT position_size FROM positions
            WHERE product_id = %s
              AND timestamp > %s
            ORDER BY timestamp DESC
            LIMIT 1
            """

            cursor.execute(query, (product_id, cutoff_time))
            result = cursor.fetchone()

            if result:
                return float(result[0])

            return None

    except Exception as e:
        print(f"Error getting local position for {symbol}: {e}")
        return None


def calculate_reconciliation_action(
    actual_position: float, desired_position: float, symbol: str
) -> tuple[bool, str | None, float | None]:
    """
    Calculate what action is needed to reconcile actual vs desired position.
    Based on the extensively tested Python logic from the original portfolio system.
    Args:
        actual_position: Current actual position size
        desired_position: Target desired position size
        symbol: Trading symbol
    Returns:
        Tuple of (execute_trade, side, position_delta)
        Note: position_delta uses sign convention: positive=buy, negative=sell
    """
    with tracer.start_as_current_span("calculate_reconciliation_action") as span:
        span.set_attributes(
            {
                "symbol": symbol,
                "actual_position": actual_position,
                "desired_position": desired_position,
            }
        )

        try:
            instrument_price = get_current_price(symbol)
            if instrument_price is None:
                span.add_event("No price available")
                return False, None, None

            min_trade_threshold = config.get(
                "reconciliation_engine.minimum_trade_threshold", 20.0
            )
            position_exists = abs(actual_position) > 1e-8
            open_risk = abs(desired_position) > 1e-8

            execute_trade = False
            side = None
            position_delta = 0.0

            if open_risk and position_exists:
                if actual_position < 0 and desired_position < 0:  # Both short
                    position_now = abs(actual_position)
                    position_target = abs(desired_position)

                    if position_now > position_target:
                        position_delta = (abs(position_target) - abs(position_now)) * -1
                        if (
                            abs(position_delta * instrument_price)
                            >= min_trade_threshold
                        ):
                            execute_trade = True
                            side = "buy"
                    elif position_now < position_target:
                        position_delta = abs(abs(position_now) - abs(position_target))
                        if position_delta * instrument_price >= min_trade_threshold:
                            execute_trade = True
                            side = "sell"

                elif actual_position < 0 and desired_position > 0:  # From short to long
                    position_delta = abs(actual_position) + desired_position
                    if abs(position_delta * instrument_price) >= min_trade_threshold:
                        execute_trade = True
                        side = "buy"

                elif actual_position > 0 and desired_position < 0:  # From long to short
                    position_delta = (actual_position + abs(desired_position)) * -1
                    if abs(position_delta * instrument_price) >= min_trade_threshold:
                        execute_trade = True
                        side = "sell"

                elif actual_position > 0 and desired_position > 0:  # Both long
                    position_now = abs(actual_position)
                    position_target = abs(desired_position)

                    if position_now < position_target:
                        position_delta = position_target - position_now
                        if (
                            abs(position_delta * instrument_price)
                            >= min_trade_threshold
                        ):
                            execute_trade = True
                            side = "buy"
                    elif position_now > position_target:
                        position_delta = position_now - position_target
                        if (
                            abs(position_delta * instrument_price)
                            >= min_trade_threshold
                        ):
                            execute_trade = True
                            side = "sell"

            elif open_risk:  # Create a new position
                position_delta = desired_position
                if abs(position_delta * instrument_price) >= min_trade_threshold:
                    execute_trade = True
                    if desired_position > 0:
                        side = "buy"
                    else:
                        side = "sell"

            elif position_exists:  # Liquidate position
                position_delta = actual_position * -1
                if abs(position_delta * instrument_price) >= min_trade_threshold:
                    execute_trade = True
                    if actual_position > 0:
                        side = "sell"
                    else:
                        side = "buy"

            span.set_attributes(
                {
                    "execute_trade": execute_trade,
                    "side": side or "none",
                    "position_delta": position_delta or 0,
                    "instrument_price": instrument_price,
                }
            )

            return execute_trade, side, position_delta

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error calculating reconciliation action for {symbol}: {e}")
            return False, None, None


def send_order_to_gateway(symbol: str, side: str, size: float) -> bool:
    """
    Send order to the Order Execution Gateway.
    Args:
        symbol: Trading symbol
        side: 'buy' or 'sell' (derived from position_delta sign)
        size: Order size (signed: positive=buy, negative=sell)
    Returns:
        True if order sent successfully
    """
    with tracer.start_as_current_span("send_order_to_gateway") as span:
        span.set_attributes({"symbol": symbol, "side": side, "size": size})

        try:
            gateway_url = config.get(
                "reconciliation_engine.order_gateway_url", "http://order-gateway:8002"
            )

            # Convert signed size to positive size and verify side matches sign
            order_size = abs(size)
            expected_side = "buy" if size > 0 else "sell"

            if side != expected_side:
                print(
                    f"Warning: side mismatch - got {side} but size {size} suggests {expected_side}"
                )

            order_data = {
                "symbol": symbol,
                "side": side,
                "size": order_size,
                "type": "market",
            }

            timeout = config.get("reconciliation_engine.order_timeout", 30)

            response = requests.post(
                f"{gateway_url}/execute_order", json=order_data, timeout=timeout
            )

            response.raise_for_status()

            span.add_event("Order sent successfully", order_data)
            print(f"Order sent to gateway: {order_data}")
            return True

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error sending order to gateway: {e}")
            return False


def cancel_all_open_orders(symbols):
    """
    Cancel all open orders across configured symbols.

    Called at the start of each reconciliation cycle to ensure a clean slate.
    Uses fetch_open_orders + cancel_orders per symbol since cancel_all_orders
    is not supported on HyperLiquid via ccxt.

    Non-fatal: logs warnings on failure without blocking reconciliation.
    """
    with tracer.start_as_current_span("cancel_all_open_orders") as span:
        span.set_attribute("symbols_count", len(symbols))
        total_cancelled = 0

        try:
            exchange = exchange_manager.get_exchange()
        except Exception as e:
            print(f"Warning: could not get exchange for order cancellation: {e}")
            span.record_exception(e)
            return

        for symbol in symbols:
            try:
                open_orders = exchange.fetch_open_orders(symbol)
                if not open_orders:
                    continue

                order_ids = [order["id"] for order in open_orders]
                span.add_event(
                    f"Cancelling {len(order_ids)} open orders for {symbol}",
                    {"symbol": symbol, "order_count": len(order_ids)},
                )

                exchange.cancel_orders(order_ids, symbol)
                total_cancelled += len(order_ids)
                print(
                    f"Cancelled {len(order_ids)} open orders for {symbol}: {order_ids}"
                )

            except Exception as e:
                print(f"Warning: failed to cancel orders for {symbol}: {e}")
                span.add_event(
                    f"Failed to cancel orders for {symbol}",
                    {"symbol": symbol, "error": str(e)},
                )

        span.set_attribute("total_cancelled", total_cancelled)


@app.task(bind=True)
def reconcile_positions(self):
    """
    Main reconciliation engine task.
    Runs the reconciliation loop for all configured symbols.
    Uses a distributed lock to prevent concurrent execution (e.g. Beat + startup overlap).
    """
    with tracer.start_as_current_span("reconcile_positions") as span:
        # If market is closed, do not perform reconciliation.
        # This prevents liquidating positions during weekends/holidays.
        if not is_market_open():
            span.add_event("Market is closed, skipping reconciliation cycle.")
            print("Market is closed. Skipping reconciliation cycle.")
            return

        # Distributed lock prevents concurrent reconciliation runs
        # (e.g. Beat schedule + worker startup hook firing simultaneously)
        # Lock TTL of 600s is generous — typical run takes ~200s
        with get_redis_connection(decode_responses=False) as r:
            acquired = r.set("reconciliation:lock", "1", nx=True, ex=600)
            if not acquired:
                span.add_event("Another reconciliation is already running, skipping.")
                print("Reconciliation already in progress, skipping this invocation.")
                return

        try:
            symbols = config.get("reconciliation_engine.symbols", ["BTC", "ETH"])
            span.set_attribute("symbols_count", len(symbols))

            # Cancel all open orders before reconciling to ensure a clean slate
            cancel_all_open_orders(symbols)

            # Pre-compute per-symbol margin caps using leverage weights
            latest_balance_for_caps = get_latest_balance()
            max_margin_pct = config.get(
                "reconciliation_engine.max_margin_usage_percentage", 0.01
            )
            if latest_balance_for_caps:
                total_margin_budget = latest_balance_for_caps * max_margin_pct
                symbol_margin_caps = _get_symbol_margin_caps(
                    symbols, total_margin_budget
                )
            else:
                symbol_margin_caps = {}

            for symbol in symbols:
                with tracer.start_as_current_span("reconcile_symbol") as symbol_span:
                    symbol_span.set_attribute("symbol", symbol)

                    try:
                        # Get desired state
                        desired_position = get_desired_state(symbol)

                        # Get actual state with consensus
                        actual_position, has_consensus, symbol_margin_used = (
                            get_actual_state(symbol)
                        )

                        if not has_consensus:
                            print(f"No consensus for {symbol} - skipping trade")
                            symbol_span.add_event(
                                "No consensus - skipping", {"symbol": symbol}
                            )
                            continue

                        if actual_position is None:
                            print(f"Could not determine actual position for {symbol}")
                            symbol_span.add_event("Could not determine actual position")
                            continue

                        # Clamp desired position to per-symbol margin cap
                        margin_cap_multiplier = config.get(
                            "reconciliation_engine.margin_cap_multiplier", 1
                        )
                        if (
                            symbol_margin_caps
                            and symbol in symbol_margin_caps
                            and symbol_margin_used > 0
                            and abs(desired_position) > 1e-8
                        ):
                            symbol_cap = (
                                symbol_margin_caps[symbol] * margin_cap_multiplier
                            )
                            if symbol_margin_used > symbol_cap:
                                scale = symbol_cap / symbol_margin_used
                                original = desired_position
                                desired_position = desired_position * scale
                                symbol_span.add_event(
                                    "Desired position clamped by per-symbol margin cap",
                                    {
                                        "symbol": symbol,
                                        "original_desired": original,
                                        "clamped_desired": desired_position,
                                        "symbol_margin_used": symbol_margin_used,
                                        "symbol_margin_cap": symbol_cap,
                                        "scale_factor": scale,
                                    },
                                )
                                print(
                                    f"Per-symbol margin cap: {symbol} using "
                                    f"${symbol_margin_used:.2f} of "
                                    f"${symbol_cap:.2f} cap. Scaling desired "
                                    f"position from {original} to {desired_position}."
                                )

                        # Calculate reconciliation action
                        (
                            execute_trade,
                            side,
                            position_delta,
                        ) = calculate_reconciliation_action(
                            actual_position, desired_position, symbol
                        )

                        if execute_trade and side and position_delta:
                            trade_allowed = True
                            # Check if the trade increases exposure
                            is_increasing_exposure = abs(desired_position) > abs(
                                actual_position
                            )

                            if is_increasing_exposure:
                                with tracer.start_as_current_span(
                                    "margin_check"
                                ) as margin_span:
                                    latest_balance = get_latest_balance()
                                    max_margin = latest_balance * config.get(
                                        "reconciliation_engine.max_margin_usage_percentage",
                                        0.01,
                                    )
                                    current_margin = get_latest_margin_usage()

                                    margin_span.set_attribute(
                                        "max_margin_allowed", max_margin
                                    )

                                    if current_margin is not None:
                                        margin_span.set_attribute(
                                            "current_margin_usage", current_margin
                                        )
                                        if current_margin >= max_margin:
                                            trade_allowed = False
                                            margin_span.add_event(
                                                "Margin limit reached. Trade blocked.",
                                                {
                                                    "current_margin": current_margin,
                                                    "max_margin": max_margin,
                                                },
                                            )
                                            print(
                                                f"Margin limit of ${max_margin} reached. Current margin: ${current_margin}. Skipping trade for {symbol}."
                                            )
                                    else:
                                        # Block trade for safety if margin is unknown
                                        trade_allowed = False
                                        margin_span.add_event(
                                            "Could not determine current margin. Trade blocked for safety."
                                        )
                                        print(
                                            f"Could not determine current margin for {symbol}. Skipping trade for safety."
                                        )

                            if trade_allowed:
                                print(
                                    "Reconciliation needed for "
                                    f"{symbol}: {side} {abs(position_delta)}"
                                )

                                # Send order to gateway
                                success = send_order_to_gateway(
                                    symbol, side, abs(position_delta)
                                )

                                if success:
                                    symbol_span.add_event(
                                        "Order executed",
                                        {"side": side, "size": abs(position_delta)},
                                    )
                                else:
                                    symbol_span.add_event("Order failed")
                            else:
                                symbol_span.add_event(
                                    "Trade skipped due to margin limit"
                                )
                        else:
                            symbol_span.add_event("No action needed")
                    except Exception as e:
                        symbol_span.record_exception(e)
                        symbol_span.set_status(
                            trace.Status(trace.StatusCode.ERROR, str(e))
                        )
                        print(f"Error reconciling {symbol}: {e}")

            span.add_event("Reconciliation cycle completed")

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error in reconciliation engine: {e}")
        finally:
            # Release the distributed lock so the next scheduled run can proceed
            try:
                with get_redis_connection(decode_responses=False) as r:
                    r.delete("reconciliation:lock")
            except Exception:
                pass  # Lock will expire via TTL if delete fails
