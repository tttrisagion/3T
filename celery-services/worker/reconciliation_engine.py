"""
Reconciliation Engine - The brain of the trading system.

This module continuously compares the Desired State of the portfolio with the
Actual State (determined by consensus) and generates orders to close any gap.
It operates without explicit locking, relying on idempotent execution and
state consensus for safety.
"""

import os
from datetime import datetime, timedelta

import requests
from opentelemetry import trace

from shared.celery_app import app
from shared.config import config
from shared.database import get_db_connection
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


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
                HAVING total_pnl > 0 AND ABS(position) >= 1
                """

                cursor.execute(query, (symbol,))
                result = cursor.fetchone()

                if result and result[1] is not None:  # position column
                    risk_pos_size = config.get(
                        "reconciliation_engine.risk_pos_size", 20.25
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
            observer_position, observer_available = get_observer_position(symbol)
            span.set_attribute(
                "observer_position", observer_position if observer_position else 0.0
            )
            span.set_attribute("observer_available", observer_available)

            # Require successful observer validation for consensus
            if not observer_available:
                span.add_event("Observer nodes unavailable - cannot achieve consensus")
                return None, False

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
            cutoff_time = datetime.utcnow() - timedelta(seconds=staleness_timeout)
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


def get_observer_position(symbol: str) -> tuple[float | None, bool]:
    """
    Get position from external observer node.
    Args:
        symbol: The trading symbol (e.g., 'BTC/USDC:USDC')
    Returns:
        Tuple of (position_size, observer_available)
        - position_size: Position size or None if observer unavailable
        - observer_available: True if observer responded successfully, False if all failed
    """
    try:
        observer_nodes = config.get(
            "reconciliation_engine.observer_nodes",
            ["http://localhost:8001/3T-observer.json"],
        )
        wallet_address = config.get_secret("exchanges.hyperliquid.walletAddress")

        if not wallet_address:
            print("No wallet address configured")
            return None, False

        # Extract base symbol for observer lookup (observer uses coin field like 'BTC')
        base_symbol = get_base_symbol(symbol)

        for observer_url in observer_nodes:
            try:
                response = requests.get(observer_url, timeout=5)
                response.raise_for_status()

                observer_data = response.json()
                positions = observer_data.get("positions", {})
                wallet_data = positions.get(wallet_address, {})
                asset_positions = wallet_data.get("assetPositions", [])

                # Find position for this base symbol
                for asset_pos in asset_positions:
                    position_data = asset_pos.get("position", {})
                    coin = position_data.get("coin")

                    if coin == base_symbol:
                        szi = float(position_data.get("szi", 0))
                        return szi, True

                # If no position found, return 0 (flat position) with successful observer
                return 0.0, True

            except requests.RequestException as e:
                print(f"Error getting position from observer {observer_url}: {e}")
                continue

        # All observers failed - return None with failed status
        return None, False

    except Exception as e:
        print(f"Error getting observer position for {symbol}: {e}")
        return None, False


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
