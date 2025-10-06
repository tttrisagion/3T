#!/usr/bin/env python3
"""
Flippycoin: A "Hello World" example for the 3T system.

This script demonstrates a complete, albeit simple, trading strategy loop.
It forks multiple child processes, each running an independent "coin flip" strategy.
Each child attempts to claim a symbol based on margin availability before running.
"""

import logging
import os
import random
import struct
import sys
import time
from multiprocessing import resource_tracker, shared_memory
from pathlib import Path

import numpy as np
from celery import Celery

# --- Project Root Setup ---
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from shared.config import config
from shared.voms import VOMS

# --- Constants ---
NUM_CHILDREN = 300
DECISION_SLEEP_SECONDS = 30
STARTING_BALANCE = 10000
LEVERAGE = 10
MARGIN_THRESHOLD = 70

SHM_PRICE_NAME = "price_data_shm"

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(process)d - %(levelname)s - %(message)s",
)


# --- Symbol and SHM Functions ---
def get_all_products():
    """
    Fetches product symbols from config.yml and assigns leverage based on a
    static map, with a fallback to a default value.
    """
    leverage_map = {
        "BTC/USDC:USDC": 40,
        "ETH/USDC:USDC": 25,
    }
    default_leverage = 3
    try:
        symbols = config.get("reconciliation_engine.symbols")
        if not symbols:
            logging.error(
                "No symbols found in config.yml under reconciliation_engine.symbols"
            )
            return []

        products = []
        for s in symbols:
            leverage = leverage_map.get(s, default_leverage)
            products.append({"symbol": s, "max_leverage": leverage})
        return products
    except Exception as e:
        logging.error(f"Could not fetch products from config: {e}")
        return []


def get_price_from_shm(symbol: str):
    """Gets the latest price for a symbol from shared memory."""
    try:
        shm = shared_memory.SharedMemory(name=SHM_PRICE_NAME)
        resource_tracker.unregister(shm._name, "shared_memory")
        header_size, entry_size = 8, 24
        max_symbols = (shm.size - header_size) // entry_size
        buffer = np.ndarray((shm.size,), dtype=np.uint8, buffer=shm.buf)
        num_symbols = buffer[:header_size].view(np.uint64)[0]

        import hashlib

        hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
        symbol_hash = (
            int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF
        )

        for i in range(min(num_symbols, max_symbols)):
            offset = header_size + (i * entry_size)
            entry_data = buffer[offset : offset + entry_size].tobytes()
            stored_hash, price, _ = struct.unpack("<Qdd", entry_data)
            if stored_hash == symbol_hash:
                return price
    except FileNotFoundError:
        return None
    finally:
        if "shm" in locals() and shm:
            shm.close()
    return None


# --- Child Process Logic ---
def child_task():
    POSITION_SIZE = 20  # Fixed dollar amount for each trade
    start_time = time.time()
    max_duration = random.randint(300, 259200)  # 5 minutes to 72 hours

    logging.info(
        f"CHILD: New child process started. Max duration: {max_duration / 3600:.2f} hours."
    )

    app = Celery(
        "flippycoin_child",
        broker="redis://localhost:6379/0",
        backend="redis://localhost:6379/0",
    )

    # 1. Get all available products
    products = get_all_products()
    if not products:
        logging.error("CHILD: No products found in config. Exiting.")
        return

    # 2. Margin-based Symbol Selection
    selected_product = None
    while not selected_product:
        product = random.choice(products)
        symbol = product["symbol"]
        leverage = product["max_leverage"]

        try:
            # Check current margin allocation for this symbol
            result = app.send_task("worker.tasks.get_active_run_count", args=[symbol])
            active_runs = result.get(timeout=DECISION_SLEEP_SECONDS)

            # Normalize run count by leverage
            margin_val = float(active_runs / leverage)

            if margin_val > MARGIN_THRESHOLD:
                logging.warning(
                    f"CHILD: {symbol} exceeds margin allocation ({margin_val:.2f} > {MARGIN_THRESHOLD}). Trying another symbol."
                )
                time.sleep(1)  # Avoid busy-looping
            else:
                logging.info(
                    f"CHILD: {symbol} qualifies for margin ({margin_val:.2f} <= {MARGIN_THRESHOLD})."
                )
                selected_product = product
        except Exception as e:
            logging.error(
                f"CHILD: Could not get active run count for {symbol}. Error: {e}"
            )
            time.sleep(5)

    # 3. Create a new run
    run_id = None
    try:
        run_params = {
            "start_balance": STARTING_BALANCE,
            "symbol": selected_product["symbol"],
            "max_duration": max_duration,
            "controller_seed": random.random(),
            "pid": os.getpid(),
            "host": "flippycoin",
        }
        result = app.send_task("worker.tasks.create_run", kwargs=run_params)
        run_id = result.get(timeout=DECISION_SLEEP_SECONDS)
        logging.info(
            f"CHILD: Created run_id: {run_id} for symbol {selected_product['symbol']}"
        )
    except Exception as e:
        logging.error(f"CHILD: Could not create run. Exiting. Error: {e}")
        return

    # 4. Initialize VOMS and trading loop
    voms = VOMS(starting_balance=STARTING_BALANCE, leverage=LEVERAGE)
    logging.info(f"CHILD: VOMS object created. Attributes: {dir(voms)}")
    risk_percentage = config.get("reconciliation_engine.risk_pos_percentage", 0.01)

    def update_run_position(pos_direction: int):
        # apply technique to mean-revision only
        app.send_task(
            "worker.tasks.update_run_position", args=[run_id, pos_direction * -1]
        )

    def update_pnl(pnl: float):
        app.send_task("worker.tasks.update_pnl", args=[run_id, pnl])

    def get_exit():
        """Checks if the run has been flagged to exit."""
        try:
            result = app.send_task("worker.tasks.get_exit_status", args=[run_id])
            return result.get(
                timeout=DECISION_SLEEP_SECONDS
            )  # Using 10s timeout for responsiveness
        except Exception as e:
            logging.error(
                f"CHILD: Could not get exit status for run {run_id}. Error: {e}"
            )
            return False  # Default to not exiting if check fails

    while True:
        try:
            if time.time() - start_time > max_duration:
                logging.info("CHILD: Max duration reached. Exiting.")
                break

            # Check for external exit signal
            if get_exit():
                logging.info(f"CHILD: Exit signal received for run {run_id}. Exiting.")
                break

            time.sleep(DECISION_SLEEP_SECONDS)

            symbol = selected_product["symbol"]
            price = get_price_from_shm(symbol)
            logging.info(f"CHILD: Checking price for {symbol}. Found: {price}")
            if price is None:
                logging.warning(
                    f"CHILD: No price found for {symbol} in shared memory. Skipping cycle."
                )
                continue

            voms.update_price(price)
            metrics = voms.get_metrics()

            # Get state from metrics dict; default to 0 if no position yet
            current_pos_size = metrics["position_size"] if metrics else 0
            is_winning = metrics is not None and metrics["unrealized_pnl"] > 0

            # Decide trade direction
            go_long = random.choice([True, False])
            logging.info(
                f"CHILD: Coin flip result: {'GO LONG' if go_long else 'GO SHORT'}. Current P&L: {metrics['unrealized_pnl'] if metrics else 0:.4f}"
            )

            # --- New Trading Strategy ---
            if (
                current_pos_size != 0
            ):  # Only apply special logic if there is an open position
                if is_winning:
                    is_currently_long = current_pos_size > 0
                    is_currently_short = current_pos_size < 0

                    # If coin flip opposes winning position, hold.
                    if (is_currently_long and not go_long) or (
                        is_currently_short and go_long
                    ):
                        logging.info(
                            f"CHILD: Holding winning {'LONG' if is_currently_long else 'SHORT'} position. P&L: {metrics['unrealized_pnl']:.2f}"
                        )
                        update_pnl(metrics["unrealized_pnl"])
                        continue

                    logging.info(
                        f"CHILD: Adding to winning {'LONG' if is_currently_long else 'SHORT'} position."
                    )
                else:  # Position is losing
                    # "when losing just wait"
                    logging.info(
                        f"CHILD: Position is losing. Waiting and not trading. P&L: {metrics['unrealized_pnl']:.2f}"
                    )
                    update_pnl(metrics["unrealized_pnl"])
                    continue

            # Execute trade: if no position, or if winning and coin flip agrees
            # Calculate the quantity for a single trade unit based on fixed dollar amount
            trade_quantity_unit = POSITION_SIZE / price

            if go_long:
                logging.info(
                    f"CHILD: Executing LONG trade. Quantity: {trade_quantity_unit:.8f} (${POSITION_SIZE})"
                )
                voms.add_trade(trade_quantity_unit)
            else:  # Go short
                logging.info(
                    f"CHILD: Executing SHORT trade. Quantity: {-trade_quantity_unit:.8f} (${POSITION_SIZE})"
                )
                voms.add_trade(-trade_quantity_unit)

            # Update run state in DB
            final_metrics = voms.get_metrics()
            if final_metrics:
                update_pnl(final_metrics["unrealized_pnl"])
                final_pos_size = final_metrics.get("position_size", 0)

                # Calculate position magnitude in integer units of POSITION_SIZE
                position_magnitude = 0
                if trade_quantity_unit > 0:
                    position_magnitude = round(final_pos_size / trade_quantity_unit)

                update_run_position(position_magnitude)
                logging.info(
                    f"CHILD: Position updated to {final_pos_size:.4f} ({position_magnitude}). P&L: {final_metrics['unrealized_pnl']:.2f}"
                )
            else:
                logging.info("CHILD: No metrics to update in DB yet.")

        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"CHILD: Error in trading loop: {e}", exc_info=True)
            time.sleep(5)


# --- Parent Process Logic ---
if __name__ == "__main__":
    child_pids = []
    logging.info(f"PARENT: Starting with PID {os.getpid()}")

    parent_app = Celery(
        "flippycoin_parent",
        broker="redis://localhost:6379/0",
        backend="redis://localhost:6379/0",
    )

    def get_height():
        result = parent_app.send_task("worker.tasks.get_max_run_height")
        return result.get(timeout=DECISION_SLEEP_SECONDS)

    def exit_runs_by_height(height):
        parent_app.send_task("worker.tasks.set_exit_for_runs_by_height", args=[height])

    current_height = get_height()
    logging.info(f"PARENT: Initial height is {current_height}")

    for i in range(NUM_CHILDREN):
        pid = os.fork()
        if pid == 0:
            import multiprocessing.util

            multiprocessing.util._exit_function()
            child_task()
            os._exit(0)
        else:
            child_pids.append(pid)

    logging.info("PARENT: Initial pool launched. Entering management loop.")
    while True:
        try:
            finished_pid, _ = os.wait()
            logging.warning(f"PARENT: Child with PID {finished_pid} has exited.")
            child_pids.remove(finished_pid)

            next_height = get_height()
            if next_height > current_height:
                logging.info(f"PARENT: Target height {next_height} reached. Exiting.")
                for pid in child_pids:
                    try:
                        os.kill(pid, 9)
                    except ProcessLookupError:
                        pass
                exit_runs_by_height(current_height + 1)
                break

            new_pid = os.fork()
            if new_pid == 0:
                child_task()
                os._exit(0)
            else:
                logging.info(f"PARENT: Launched new child with PID {new_pid}.")
                child_pids.append(new_pid)

        except OSError as e:
            logging.error(f"PARENT: Error forking new child: {e}. Retrying.")
            time.sleep(1)
        except ChildProcessError:
            logging.info("PARENT: No more children to wait for. Exiting.")
            break
        except KeyboardInterrupt:
            logging.info("PARENT: Shutdown signal received. Terminating children...")
            for pid in child_pids:
                try:
                    os.kill(pid, 9)
                except ProcessLookupError:
                    pass
            break
