import hashlib
import json
import logging
import os
import secrets

# Import shared modules
import sys
from datetime import datetime
from enum import Enum

import mysql.connector
from fastapi import FastAPI, HTTPException, status
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, Field

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from shared.config import config
from shared.database import get_db_connection
from shared.exchange_manager import ExchangeManager
from shared.opentelemetry_config import get_tracer

from shared.opentelemetry_config import get_tracer

# Setup OpenTelemetry
tracer = get_tracer("order-gateway")

exchange_manager = ExchangeManager()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Order Execution Gateway",
    description="Secure, idempotent order execution service",
    version="1.0.0",
)

# Instrument FastAPI with OpenTelemetry
FastAPIInstrumentor.instrument_app(app)


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


class OrderRequest(BaseModel):
    symbol: str = Field(..., description="Exchange symbol (e.g., BTC/USDC:USDC)")
    side: OrderSide = Field(..., description="Order side (buy/sell)")
    size: float = Field(..., gt=0, description="Order size")
    type: OrderType = Field(..., description="Order type (market/limit)")
    price: float | None = Field(None, gt=0, description="Price for limit orders")

    def model_post_init(self, __context):
        if self.type == OrderType.LIMIT and self.price is None:
            raise ValueError("Price is required for limit orders")


class OrderResponse(BaseModel):
    client_order_id: str
    status: str
    message: str
    exchange_order_id: str | None = None


class OrderExecutionError(Exception):
    def __init__(self, message: str, retry_possible: bool = False):
        self.message = message
        self.retry_possible = retry_possible
        super().__init__(message)


def generate_request_hash(order_request: OrderRequest) -> str:
    """
    Generate a deterministic hash of the order request for deduplication.
    Includes a timestamp component rounded to 30-second windows to allow
    multiple identical orders over time while preventing rapid duplicates.
    """
    import time

    # Round timestamp to 30-second windows for short-term deduplication
    timestamp_window = int(time.time() // 30) * 30

    request_data = {
        "symbol": order_request.symbol,
        "side": order_request.side.value,
        "size": str(order_request.size),
        "type": order_request.type.value,
        "price": str(order_request.price) if order_request.price else None,
        "timestamp_window": timestamp_window,
    }
    request_json = json.dumps(request_data, sort_keys=True)
    return hashlib.sha256(request_json.encode()).hexdigest()


def generate_hyperliquid_client_order_id() -> str:
    """
    Generate a HyperLiquid-compatible client order ID.
    Format: 128-bit hex string with 0x prefix (e.g., 0x1234567890abcdef1234567890abcdef)
    """
    # Generate 128 bits (16 bytes) of random data
    random_bytes = secrets.token_bytes(16)
    # Convert to hex string with 0x prefix
    return "0x" + random_bytes.hex()


def get_or_create_order_log(order_request: OrderRequest) -> tuple[str, bool]:
    """
    Get existing order log or create new one. Returns (client_order_id, is_new).
    If an identical request exists, returns existing client_order_id.
    """
    request_hash = generate_request_hash(order_request)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Check for existing request with same hash
        cursor.execute(
            "SELECT client_order_id, status FROM order_execution_log WHERE request_hash = %s",
            (request_hash,),
        )
        existing = cursor.fetchone()

        if existing:
            logger.info(
                f"Found existing order with hash {request_hash}, client_order_id: {existing['client_order_id']}"
            )
            return existing["client_order_id"], False

        # Create new order log entry with HyperLiquid-compatible client order ID
        client_order_id = generate_hyperliquid_client_order_id()
        cursor.execute(
            """
            INSERT INTO order_execution_log
            (client_order_id, request_hash, product_id, side, size, order_type, price, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                client_order_id,
                request_hash,
                order_request.symbol,
                order_request.side.value,
                order_request.size,
                order_request.type.value,
                order_request.price,
                OrderStatus.PENDING.value,
            ),
        )
        conn.commit()

        logger.info(
            f"Created new order log entry with client_order_id: {client_order_id}"
        )
        return client_order_id, True

    finally:
        cursor.close()
        conn.close()


def update_order_status(
    client_order_id: str,
    status: OrderStatus,
    exchange_order_id: str = None,
    error_message: str = None,
    increment_retry: bool = False,
):
    """Update order status in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if increment_retry:
            cursor.execute(
                """
                UPDATE order_execution_log
                SET status = %s, exchange_order_id = %s, error_message = %s,
                    retry_count = retry_count + 1, updated_at = NOW()
                WHERE client_order_id = %s
            """,
                (status.value, exchange_order_id, error_message, client_order_id),
            )
        else:
            cursor.execute(
                """
                UPDATE order_execution_log
                SET status = %s, exchange_order_id = %s, error_message = %s, updated_at = NOW()
                WHERE client_order_id = %s
            """,
                (status.value, exchange_order_id, error_message, client_order_id),
            )

        conn.commit()
        logger.info(f"Updated order {client_order_id} status to {status.value}")

    finally:
        cursor.close()
        conn.close()


def get_latest_price_from_stream(symbol: str) -> float:
    """Get latest price from Redis price stream."""
    try:
        from shared.database import get_redis_connection

        redis_conn = get_redis_connection()

        # Get latest price from the unified price stream
        stream_key = config.get("redis.streams.price_updates")  # "prices:updated"

        # Search through recent entries to find the latest price for this symbol
        latest_entries = redis_conn.xrevrange(stream_key, count=500)

        for _, fields in latest_entries:
            if fields.get("symbol") == symbol:
                price = float(fields.get("price", 0))
                logger.info(f"Got latest price for {symbol}: {price}")
                return price

        logger.warning(f"No price data found for {symbol} in recent stream entries")
        return None
    except Exception as e:
        logger.error(f"Failed to get price from stream for {symbol}: {e}")
        return None


def send_to_exchange(order_details: dict, client_order_id: str) -> dict:
    """
    Send market order to HyperLiquid exchange using the exchange manager.
    Gets latest price from price stream and executes as market order.

    Args:
        order_details: Dict with symbol, side, size, type
        client_order_id: HyperLiquid-compatible client order ID

    Returns:
        dict with 'success', 'exchange_order_id', 'error_message', 'retry_possible'
    """
    try:
        # Get latest price from price stream for logging/validation
        current_price = get_latest_price_from_stream(order_details["symbol"])
        if current_price is None:
            return {
                "success": False,
                "exchange_order_id": None,
                "error_message": f"No current price available for {order_details['symbol']}",
                "retry_possible": True,
            }

        logger.info(
            f"Executing order: {order_details['side']} {order_details['size']} {order_details['symbol']} at ~{current_price}"
        )

        # Get exchange instance
        exchange = exchange_manager.get_exchange("hyperliquid")

        # Prepare order parameters for CCXT
        order_params = {"clientOrderId": client_order_id, "loadMarkets": False}

        # Execute the market order with retry logic
        def place_order():
            return exchange.create_order(
                symbol=order_details["symbol"],
                type="market",  # Always market order
                side=order_details["side"],
                amount=order_details["size"],
                price=current_price,  # HyperLiquid needs price for slippage calculation
                params=order_params,
            )

        result = exchange_manager.execute_with_retry(place_order)

        logger.info(
            f"Order successful: {result.get('id', 'unknown')} - {order_details['side']} {order_details['size']} {order_details['symbol']}"
        )

        return {
            "success": True,
            "exchange_order_id": result.get("id", "unknown"),
            "error_message": None,
            "retry_possible": False,
        }

    except Exception as e:
        error_msg = str(e).lower()
        logger.error(f"Order execution failed: {str(e)}")

        # Check for duplicate order (idempotency)
        if (
            "already exists" in error_msg
            or "duplicate" in error_msg
            or "clientorderid" in error_msg
        ):
            logger.info(
                f"Duplicate order detected for client_order_id: {client_order_id}"
            )
            return {
                "success": False,
                "exchange_order_id": None,
                "error_message": f"Order with clientOrderId {client_order_id} already exists",
                "retry_possible": False,
            }

        # Check for client errors (non-retryable)
        elif any(
            phrase in error_msg
            for phrase in [
                "insufficient",
                "invalid",
                "bad request",
                "unauthorized",
                "permission denied",
            ]
        ):
            return {
                "success": False,
                "exchange_order_id": None,
                "error_message": str(e),
                "retry_possible": False,
            }

        # Network/timeout errors are retryable
        elif any(
            phrase in error_msg
            for phrase in ["timeout", "network", "connection", "502", "503", "504"]
        ):
            return {
                "success": False,
                "exchange_order_id": None,
                "error_message": str(e),
                "retry_possible": True,
            }

        # Everything else is potentially retryable
        else:
            return {
                "success": False,
                "exchange_order_id": None,
                "error_message": str(e),
                "retry_possible": True,
            }


@app.on_event("startup")
async def startup_event():
    """Load markets on startup to handle connection issues early."""
    try:
        logger.info("Loading exchange markets on startup...")
        exchange = exchange_manager.get_exchange("hyperliquid")
        exchange.load_markets()
        logger.info("Exchange markets loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load exchange markets on startup: {e}")
        # Depending on the desired behavior, you might want to exit the application
        # if it can't connect to the exchange on startup.
        # For now, we'll just log the error and continue.


@app.post("/execute_order", response_model=OrderResponse)
async def execute_order(order_request: OrderRequest):
    """
    Execute a trade order with idempotency guarantees.
    """
    with tracer.start_as_current_span("execute_order") as span:
        span.set_attribute("symbol", order_request.symbol)
        span.set_attribute("side", order_request.side.value)
        span.set_attribute("size", order_request.size)
        span.set_attribute("type", order_request.type.value)

        try:
            # Get or create order log entry
            client_order_id, is_new = get_or_create_order_log(order_request)
            span.set_attribute("client_order_id", client_order_id)
            span.set_attribute("is_new_order", is_new)

            # If not a new order, check existing status
            if not is_new:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                try:
                    cursor.execute(
                        "SELECT status, exchange_order_id, error_message FROM order_execution_log WHERE client_order_id = %s",
                        (client_order_id,),
                    )
                    existing_order = cursor.fetchone()

                    if existing_order["status"] == OrderStatus.CONFIRMED.value:
                        return OrderResponse(
                            client_order_id=client_order_id,
                            status="CONFIRMED",
                            message="Order already confirmed",
                            exchange_order_id=existing_order["exchange_order_id"],
                        )
                    elif existing_order["status"] == OrderStatus.FAILED.value:
                        return OrderResponse(
                            client_order_id=client_order_id,
                            status="FAILED",
                            message=f"Order already failed: {existing_order['error_message']}",
                        )
                finally:
                    cursor.close()
                    conn.close()

            # Attempt to send order to exchange with retry logic
            max_retries = 3
            retry_count = 0

            while retry_count <= max_retries:
                try:
                    order_details = {
                        "symbol": order_request.symbol,
                        "side": order_request.side.value,
                        "size": order_request.size,
                        "type": order_request.type.value,
                        "price": order_request.price,
                    }

                    result = send_to_exchange(order_details, client_order_id)

                    if result["success"]:
                        update_order_status(
                            client_order_id,
                            OrderStatus.CONFIRMED,
                            exchange_order_id=result["exchange_order_id"],
                        )
                        return OrderResponse(
                            client_order_id=client_order_id,
                            status="CONFIRMED",
                            message="Order executed successfully",
                            exchange_order_id=result["exchange_order_id"],
                        )

                    # Check if error indicates duplicate (safe success)
                    if "already exists" in result["error_message"].lower():
                        update_order_status(
                            client_order_id,
                            OrderStatus.CONFIRMED,
                            error_message="Duplicate order - treating as success",
                        )
                        return OrderResponse(
                            client_order_id=client_order_id,
                            status="CONFIRMED",
                            message="Order was already processed (duplicate detection)",
                        )

                    # Check if error is retryable
                    if not result["retry_possible"] or retry_count >= max_retries:
                        update_order_status(
                            client_order_id,
                            OrderStatus.FAILED,
                            error_message=result["error_message"],
                        )
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Order execution failed: {result['error_message']}",
                        )

                    # Increment retry count and continue
                    retry_count += 1
                    update_order_status(
                        client_order_id,
                        OrderStatus.PENDING,
                        error_message=f"Retry {retry_count}: {result['error_message']}",
                        increment_retry=True,
                    )

                    # Exponential backoff
                    import time

                    time.sleep(2**retry_count)

                except Exception as e:
                    import traceback

                    logger.error(f"Unexpected error during order execution: {str(e)}")
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    update_order_status(
                        client_order_id,
                        OrderStatus.FAILED,
                        error_message=f"Internal error: {str(e)}",
                    )
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Internal server error during order execution: {str(e)}",
                    ) from e

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in execute_order: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error",
            ) from e


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        conn = mysql.connector.connect(
            host=config.get("database.host"),
            user=config.get("database.user"),
            password=config.get_secret("database.password"),
            database=config.get("database.database"),
        )
        conn.close()

        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service unhealthy"
        ) from e


@app.get("/orders/{client_order_id}")
async def get_order_status(client_order_id: str):
    """Get the status of an order by client_order_id."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(
            """
            SELECT client_order_id, product_id as symbol, side, size, order_type, price,
                   status, exchange_order_id, error_message, retry_count,
                   created_at, updated_at
            FROM order_execution_log
            WHERE client_order_id = %s
        """,
            (client_order_id,),
        )

        order = cursor.fetchone()
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
            )

        return order

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)
