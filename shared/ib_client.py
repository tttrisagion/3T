"""
Stateless, thread-safe, and Eventlet-resilient Interactive Brokers TWS/Gateway client.
Creates and binds a private asyncio event loop explicitly inside each public call,
and generates a random, unique clientId on every single connection to completely prevent
TWS clientId conflicts across Celery replicas and threads.
"""

import logging
import asyncio
import random
import nest_asyncio

# Apply nest_asyncio globally to support nested event loop runs inside Uvicorn/FastAPI
nest_asyncio.apply()

# --- Pre-Import Safe Event Loop Policy ---
# eventkit (imported by ib_insync) attempts to fetch the event loop during package import.
# Setting a custom loop policy here ensures the import succeeds on both MainThread and GreenThreads.
class SafeEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    def get_event_loop(self):
        try:
            loop = super().get_event_loop()
            if loop is None or loop.is_closed():
                raise RuntimeError()
            return loop
        except RuntimeError:
            loop = asyncio.new_event_loop()
            self.set_event_loop(loop)
            return loop

asyncio.set_event_loop_policy(SafeEventLoopPolicy())

from ib_insync import IB, Stock, MarketOrder, LimitOrder

from shared.config import config

logger = logging.getLogger(__name__)


class IBClient:
    """
    Stateless, Eventlet-resilient Interactive Brokers client.
    Each method executes its asyncio commands explicitly on a private event loop,
    utilizing fresh randomized clientIds on every connection to prevent collisions.
    """

    def __init__(self):
        # Load configs
        self.host = config.get("exchanges.tradfi.ib.host", "127.0.0.1")
        self.port = int(config.get("exchanges.tradfi.ib.port", 7497))

    def get_balance_and_positions(self) -> tuple[dict, list[dict]]:
        """
        Fetch real-time account balance and portfolio positions synchronously
        using a private, isolated asyncio event loop and a unique connection clientId.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Generate random unique clientId per connection to avoid TWS collisions
        client_id = random.randint(100, 999999)
        ib = IB()
        try:
            logger.warning(f"Establishing private loop IB connection to {self.host}:{self.port} (clientId: {client_id})...")
            # Connect asynchronously on our private loop
            loop.run_until_complete(ib.connectAsync(self.host, self.port, clientId=client_id))
            
            # 1. Fetch account summary asynchronously
            summary = loop.run_until_complete(ib.accountSummaryAsync())
            account_value = 0.0
            cross_margin_used = 0.0
            
            for item in summary:
                if item.tag == "NetLiquidation":
                    account_value = float(item.value)
                elif item.tag == "MaintMarginReq":
                    cross_margin_used = float(item.value)
            
            # 2. Fetch portfolio positions
            # Let the loop process incoming socket updates for portfolio data
            loop.run_until_complete(asyncio.sleep(0.5))
            
            portfolio_items = ib.portfolio()
            positions = []
            
            for item in portfolio_items:
                if item.contract.secType == "STK":
                    positions.append({
                        "symbol": item.contract.symbol,
                        "position_size": float(item.position),
                        "position_value": float(item.marketValue),
                        "unrealized_pnl": float(item.unrealizedPNL)
                    })
            
            logger.warning(f"Successfully retrieved IB state: Value=${account_value}, Positions={len(positions)}")
            return {
                "account_value": account_value,
                "cross_maintenance_margin_used": cross_margin_used
            }, positions
            
        except Exception as e:
            logger.error(f"Failed to fetch IB account state: {e}")
            raise
        finally:
            if ib.isConnected():
                try:
                    loop.run_until_complete(ib.disconnectAsync() if hasattr(ib, 'disconnectAsync') else asyncio.sleep(0.1))
                except Exception:
                    pass
            try:
                loop.close()
            except Exception:
                pass

    def place_order(self, symbol: str, side: str, size: float, order_type: str, price: float = None) -> dict:
        """
        Place a limit or market order for a SMART routed stock inside our private event loop.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        client_id = random.randint(100, 999999)
        ib = IB()
        try:
            logger.warning(f"Establishing private loop IB connection for execution to {self.host}:{self.port} (clientId: {client_id})...")
            loop.run_until_complete(ib.connectAsync(self.host, self.port, clientId=client_id))
            
            # Define stock contract
            contract = Stock(symbol, "SMART", "USD")
            loop.run_until_complete(ib.qualifyContractsAsync(contract))
            
            action = side.upper()
            if order_type.lower() == "limit":
                if price is None:
                    raise ValueError("Price is required for IB Limit orders.")
                order = LimitOrder(action, size, price)
            else:
                order = MarketOrder(action, size)
            
            logger.info(f"Submitting IB order: {action} {size} {symbol} ({order_type.upper()})")
            trade = ib.placeOrder(contract, order)
            
            # Wait for order propagation
            for _ in range(30):  # 3-second timeout limit
                if trade.isDone():
                    break
                loop.run_until_complete(asyncio.sleep(0.1))
            
            order_id = str(trade.order.orderId or trade.order.permId)
            status = trade.orderStatus.status
            
            logger.info(f"IB Order placed. ID: {order_id}, Status: {status}")
            return {
                "id": order_id,
                "status": status
            }
        except Exception as e:
            logger.error(f"Failed to place IB order for {symbol}: {e}")
            raise
        finally:
            if ib.isConnected():
                try:
                    loop.run_until_complete(ib.disconnectAsync() if hasattr(ib, 'disconnectAsync') else asyncio.sleep(0.1))
                except Exception:
                    pass
            try:
                loop.close()
            except Exception:
                pass

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an active IB order by order_id.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        client_id = random.randint(100, 999999)
        ib = IB()
        try:
            logger.warning(f"Establishing private loop IB connection for cancellation to {self.host}:{self.port} (clientId: {client_id})...")
            loop.run_until_complete(ib.connectAsync(self.host, self.port, clientId=client_id))
            
            # Request all open orders across all clientIds to find and cancel our order
            ib.reqAllOpenOrders()
            trades = ib.trades()
            
            for trade in trades:
                id_to_check = str(trade.order.orderId or trade.order.permId)
                if id_to_check == str(order_id):
                    logger.info(f"Cancelling active IB order {order_id}...")
                    ib.cancelOrder(trade.order)
                    loop.run_until_complete(asyncio.sleep(0.5))
                    return True
            
            logger.warning(f"Could not find active IB trade matching order ID: {order_id}")
            return False
        except Exception as e:
            logger.error(f"Failed to cancel IB order {order_id}: {e}")
            raise
        finally:
            if ib.isConnected():
                try:
                    loop.run_until_complete(ib.disconnectAsync() if hasattr(ib, 'disconnectAsync') else asyncio.sleep(0.1))
                except Exception:
                    pass
            try:
                loop.close()
            except Exception:
                pass


# Global singleton instance
ib_client = IBClient()
