"""
Reconciliation Engine - The brain of the trading system.

This module continuously compares the Desired State of the portfolio with the
Actual State (determined by consensus) and generates orders to close any gap.
It operates without explicit locking, relying on idempotent execution and
state consensus for safety.
"""

import os
from datetime import UTC, datetime, timedelta

import redis
import requests
from opentelemetry import trace

from shared.celery_app import app
from shared.config import config
from shared.database import get_db_connection
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


def _calculate_kelly_metrics(
    condition: str,
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

                query = f"""
                SELECT live_pnl
                FROM runs
                WHERE {condition}
                  AND live_pnl IS NOT NULL
                  AND live_pnl <> 0
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


def calculate_kelly_position_size(base_risk_pos_size: float) -> float:
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
            _, _, kelly_current = _calculate_kelly_metrics("height IS NULL")

            # 2. Get Kelly metric for historical (non-null height) runs
            _, _, kelly_historical = _calculate_kelly_metrics("height IS NOT NULL")

            span.set_attributes(
                {
                    "kelly_current": kelly_current if kelly_current else 0,
                    "kelly_historical": kelly_historical if kelly_historical else 0,
                }
            )

            # If no historical data, we can't compare. Use current, or default to base size.
            if kelly_historical is None or kelly_historical <= 0:
                if kelly_current and kelly_current > 0:
                    # No benchmark, but current performance is positive. Use it directly.
                    kelly_performance = kelly_current
                    span.add_event(
                        "Using current Kelly score due to no historical baseline.",
                        {"kelly_performance_adjustment": kelly_performance},
                    )
                else:
                    # No data at all, or current is negative. Neutral position size.
                    span.add_event("No valid Kelly data - using base size")
                    return base_risk_pos_size
            # If no current data, but we have historical, use a neutral multiplier.
            elif kelly_current is None or kelly_current <= 0:
                span.add_event(
                    "Current performance is negative or has insufficient data. Using neutral multiplier."
                )
                return base_risk_pos_size
            else:
                # 3. Calculate performance multiplier
                performance_ratio = kelly_current / kelly_historical
                # The adjustment is how much better/worse we are than the baseline
                kelly_performance = performance_ratio - 1

                span.add_event(
                    "Calculated Kelly performance adjustment",
                    {
                        "performance_ratio": performance_ratio,
                        "kelly_performance_adjustment": kelly_performance,
                    },
                )

            # Get configuration for thresholding
            reconciliation_config = config.get("reconciliation_engine", {})
            kelly_max_increase = reconciliation_config.get(
                "kelly_max_increase", 1.0
            )  # Cap increase to 100%
            kelly_max_decrease = reconciliation_config.get(
                "kelly_max_decrease", -0.5
            )  # Cap decrease to -50%

            # 4. Apply threshold cap/floor
            if kelly_performance > kelly_max_increase:
                kelly_performance = kelly_max_increase
                span.add_event("Capping performance adjustment at max increase")
            elif kelly_performance < kelly_max_decrease:
                kelly_performance = kelly_max_decrease
                span.add_event("Flooring performance adjustment at max decrease")

            # 5. Calculate adjusted position size
            # Multiplier is 1 + adjustment. E.g., 20% better -> 1.2x size
            kelly_multiplier = 1 + kelly_performance
            kelly_risk_pos_size = base_risk_pos_size * kelly_multiplier

            span.add_event(
                "Kelly position size calculated",
                {
                    "kelly_performance_adjustment": kelly_performance,
                    "kelly_multiplier": kelly_multiplier,
                    "final_kelly_risk_pos_size": kelly_risk_pos_size,
                },
            )

            return kelly_risk_pos_size

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error calculating Kelly position size: {e}")
            return base_risk_pos_size


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
                       COUNT(*) as runs,
                       SUM(position_direction)/COUNT(*) as size_per_run
                FROM runs
                WHERE exit_run = 0
                  AND end_time IS NULL
                  AND live_pnl > 0
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
                            base_risk_pos_size
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
                        # Fallback to a default value if balance is not available
                        base_risk_pos_size = 20.25
                        risk_pos_size = calculate_kelly_position_size(
                            base_risk_pos_size
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


def get_observer_state(symbol: str) -> tuple[float | None, str | None]:
    """
    Get position from external observer node and perform safety checks.

    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')

    Returns:
        Tuple of (position_size, error_message)
        - position_size: Position size, or None if validation fails.
        - error_message: A string describing the error, or None if successful.
    """
    observer_nodes = config.get(
        "reconciliation_engine.observer_nodes",
        ["http://localhost:8001/3T-observer.json"],
    )
    wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")
    max_heartbeat_age = timedelta(minutes=5)

    if not wallet_address:
        return None, "No wallet address configured"

    for observer_url in observer_nodes:
        try:
            response = requests.get(observer_url, timeout=5)
            response.raise_for_status()
            observer_data = response.json()

            # Check heartbeat
            timestamp_str = observer_data.get("timestamp")
            if not timestamp_str:
                return None, f"Observer {observer_url} has no timestamp"

            timestamp = datetime.fromisoformat(timestamp_str)
            if datetime.now(UTC) - timestamp > max_heartbeat_age:
                return None, f"Observer {observer_url} data is stale"

            # Check for wallet presence
            positions = observer_data.get("positions", {})
            if wallet_address not in positions:
                return (
                    None,
                    f"Wallet {wallet_address} not found in observer {observer_url}",
                )

            # Extract position
            wallet_data = positions.get(wallet_address, {})
            asset_positions = wallet_data.get("assetPositions", [])
            base_symbol = get_base_symbol(symbol)

            for asset_pos in asset_positions:
                position_data = asset_pos.get("position", {})
                if position_data.get("coin") == base_symbol:
                    return float(position_data.get("szi", 0)), None

            # If no position found, it's a flat position
            return 0.0, None

        except requests.RequestException as e:
            print(f"Could not connect to observer {observer_url}: {e}")
            continue  # Try next observer
        except (ValueError, KeyError) as e:
            return None, f"Invalid data from observer {observer_url}: {e}"

    return None, "All observers failed"


def get_actual_state(symbol: str) -> tuple[float | None, bool]:
    """
    Get the actual position state using consensus between local positions table
    and external observer node.
    Args:
        symbol: The trading symbol
    Returns:
        Tuple of (position_size, has_consensus)
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
            observer_position, error_message = get_observer_state(symbol)

            if error_message:
                span.add_event("Observer validation failed", {"error": error_message})
                print(f"Observer validation failed for {symbol}: {error_message}")
                return None, False

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
                return consensus_position, True

            span.add_event(
                "No consensus",
                {
                    "local_position": local_pos,
                    "observer_position": observer_pos,
                    "difference": abs(local_pos - observer_pos),
                },
            )
            return None, False

        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            print(f"Error getting actual state for {symbol}: {e}")
            return None, False


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


@app.task(bind=True)
def reconcile_positions(self):
    """
    Main reconciliation engine task.
    Runs the reconciliation loop for all configured symbols.
    """
    with tracer.start_as_current_span("reconcile_positions") as span:
        try:
            symbols = config.get("reconciliation_engine.symbols", ["BTC", "ETH"])
            span.set_attribute("symbols_count", len(symbols))

            for symbol in symbols:
                with tracer.start_as_current_span("reconcile_symbol") as symbol_span:
                    symbol_span.set_attribute("symbol", symbol)

                    try:
                        # Get desired state
                        desired_position = get_desired_state(symbol)

                        # Get actual state with consensus
                        actual_position, has_consensus = get_actual_state(symbol)

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

                        # Calculate reconciliation action
                        (
                            execute_trade,
                            side,
                            position_delta,
                        ) = calculate_reconciliation_action(
                            actual_position, desired_position, symbol
                        )

                        if execute_trade and side and position_delta:
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
