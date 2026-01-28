import asyncio
import glob
import json
import logging
import math
import os
import random
import sys
import time
import traceback
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

from celery import Celery

# --- Project Setup ---
project_root = "/opt/3T"
sys.path.append(str(project_root))
from shared.config import config
from shared.providence import (
    calculate_permutation_entropy,
    calculate_volatility_goal,
    get_price_from_shm,
    get_volatility,
)
from shared.voms import VOMS

# --- Constants and Globals ---
ORANGE = "\033[38;5;208m"
RESET = "\033[0m"
print(f"""
{ORANGE}
              P
           /  .  \\
       S  /  -|-  \\  S
         /`•. | .•`\\
        /    `.`    \\
              |
         _ _ _|_ _ _
              F
  DOCILIS - MITIS - HUMILIS
{RESET}
Starting: 3T Providence Strategy (asyncio v2)...
""")
filename = os.path.basename(__file__)
print(f"{filename} - Tactical Trend Trader")

NUM_CHILDREN = 5000
DECISION_SLEEP = 0
CELERY_GET_TIMEOUT = 30
NUM_RESULT_FETCHERS = 50

# --- Persistence Setup ---
SAVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_runs")
if not os.path.exists(SAVE_DIR): os.makedirs(SAVE_DIR)
COMPLETED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "completed_runs")
if not os.path.exists(COMPLETED_DIR): os.makedirs(COMPLETED_DIR)

print(f"MANAGER: Starting with PID {os.getpid()}")

# --- Architecture Components ---
fire_and_forget_app = Celery("showcase_client_producer", broker="redis://192.168.2.157:6379/0", backend=None)

class ResultFetcher:
    def __init__(self, loop, executor, num_fetchers):
        self._loop = loop
        self._executor = executor
        self._request_queue = asyncio.Queue()
        self._future_map = {}
        self._celery_app = threading.local()
        self._fetchers = [asyncio.create_task(self._fetcher_worker()) for _ in range(num_fetchers)]
        print(f"MANAGER: Launched {num_fetchers} result fetcher workers.")

    def _get_celery_app(self):
        if not hasattr(self._celery_app, "app"):
            self._celery_app.app = Celery("showcase_client_rpc", broker="redis://192.168.2.157:6379/0", backend="redis://192.168.2.157:6379/0")
        return self._celery_app.app

    def _sync_get_result(self, task_id):
        app = self._get_celery_app()
        return app.AsyncResult(task_id).get(timeout=CELERY_GET_TIMEOUT)

    async def _fetcher_worker(self):
        while True:
            task_id = await self._request_queue.get()
            future = self._future_map.get(task_id)
            if not future or future.done():
                continue
            try:
                result = await self._loop.run_in_executor(self._executor, self._sync_get_result, task_id)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                if task_id in self._future_map:
                    del self._future_map[task_id]

    async def fetch_result(self, task_id):
        future = self._loop.create_future()
        self._future_map[task_id] = future
        await self._request_queue.put(task_id)
        return await future

    async def shutdown(self):
        for fetcher in self._fetchers:
            fetcher.cancel()
        await asyncio.gather(*self._fetchers, return_exceptions=True)
        if hasattr(self._celery_app, "app"):
            self._celery_app.app.close()

# --- Main Trading Logic ---
async def trading_task(result_fetcher, resume_state=None):
    loop = asyncio.get_running_loop()
    # --- Local State ---
    system_swing = bool(random.randint(0, 1))
    max_pos_limit = 1000
    weights, weights_timestamp = [], []
    aprs, aprs_timestamp = [], []
    position_direction, apr_change, apr_last = 0, 0, 0
    run_id, start_time, start_balance = 0, 0, 0
    symb = config.get("reconciliation_engine.symbols")
    symb_leverage = [80] * 4 + [2] * 54
    pos = [0] * len(symb)
    virtual_balance = 7000
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # --- Parameter and VOMS Initialization ---
    if resume_state:
        ann_params = resume_state["ann_params"]
        choose = ann_params["choose"]
        weights = resume_state.get("weights", [])
        weights_timestamp = resume_state.get("weights_timestamp", [])
        aprs = resume_state.get("aprs", [])
        aprs_timestamp = resume_state.get("aprs_timestamp", [])
        position_direction = resume_state.get("position_direction", 0)
        start_time = resume_state.get("start_time", 0)
        start_balance = resume_state.get("start_balance", 0)
        run_id = resume_state.get("run_id", 0)
        voms = VOMS(starting_balance=virtual_balance, leverage=symb_leverage[choose])
        if "voms_state" in resume_state: voms.from_dict(resume_state["voms_state"])
        max_duration = ann_params["max_duration"]
        max_direction_reversal = ann_params["max_direction_reversal"]
        balance_divisor = ann_params["balance_divisor"]
        pos_scaler = ann_params["pos_scaler"]
        max_cross_margin_ratio = ann_params["max_cross_margin_ratio"]
        apr_target = ann_params["apr_target"]
        pos_weight = ann_params["pos_weight"]
        min_goal, max_goal = ann_params["min_goal"], ann_params["max_goal"]
        min_goal_weight, max_goal_weight = ann_params["min_goal_weight"], ann_params["max_goal_weight"]
        volatility_entropy_window_minutes = ann_params["volatility_entropy_window_minutes"]
        volatility_entropy_window_samples = ann_params["volatility_entropy_window_samples"]
        machine_vision_entropy_max = ann_params["machine_vision_entropy_max"]
        rolling_apr_minutes = ann_params["rolling_apr_minutes"]
        decision_distance_seconds = ann_params["decision_distance_seconds"]
        logging.info(f"RESUMED RUN {run_id} for symbol {symb[choose]}")
    else:
        choose = random.randint(0, len(symb) - 1)
        voms = VOMS(starting_balance=virtual_balance, leverage=symb_leverage[choose])
        max_duration = random.randint(300, 86400 * 5)
        max_direction_reversal = int(max_duration / random.randint(1, 10))
        balance_divisor, pos_scaler = 5, random.uniform(1.618, 3.236) / 1000
        apr_target = random.randint(20, 100) / balance_divisor / 1.618
        min_goal, max_goal = random.randint(1, 9) / 10, random.randint(1, 10)
        min_goal_weight, max_goal_weight = random.randint(1, 38) / 10000, random.randint(69, 200) / 10000
        volatility_entropy_window_minutes, volatility_entropy_window_samples = random.randint(1, 15), random.randint(5, 20)
        machine_vision_entropy_max = random.randint(5, 15) / 100
        rolling_apr_minutes, decision_distance_seconds = random.randint(1, 30), 0
        max_cross_margin_ratio, pos_weight = 0.05 / balance_divisor, 0.032732855219630354 / 3
        ann_params = {
            "virtual_balance": virtual_balance, "choose": choose, "max_duration": max_duration,
            "max_direction_reversal": max_direction_reversal, "balance_divisor": balance_divisor, "pos_scaler": pos_scaler,
            "max_cross_margin_ratio": max_cross_margin_ratio, "apr_target": apr_target, "pos_weight": pos_weight,
            "min_goal": min_goal, "max_goal": max_goal, "min_goal_weight": min_goal_weight, "max_goal_weight": max_goal_weight,
            "volatility_entropy_window_minutes": volatility_entropy_window_minutes, "volatility_entropy_window_samples": volatility_entropy_window_samples,
            "machine_vision_entropy_max": machine_vision_entropy_max, "rolling_apr_minutes": rolling_apr_minutes,
            "decision_distance_seconds": decision_distance_seconds, "system_swing": system_swing,
        }

    # --- Helpers ---
    async def get_max_pos():
        task = fire_and_forget_app.send_task("worker.tasks.get_active_run_count", args=[symb[choose]])
        result = await result_fetcher.fetch_result(task.id)
        return float(result / symb_leverage[choose])

    async def create_new_run():
        run_params = {"start_balance": virtual_balance, "max_duration": max_duration, "symbol": symb[choose], "ann_params": json.dumps(ann_params), "pid": os.getpid(), "host": hex(uuid.getnode())}
        task = fire_and_forget_app.send_task("worker.tasks.create_run", kwargs=run_params)
        new_run_id = await result_fetcher.fetch_result(task.id)
        return virtual_balance, new_run_id

    async def get_weight():
        task = fire_and_forget_app.send_task("worker.tasks.get_market_weight", args=[symb[choose]])
        return await result_fetcher.fetch_result(task.id)

    async def get_exit():
        task = fire_and_forget_app.send_task("worker.tasks.get_exit_status", args=[run_id])
        return await result_fetcher.fetch_result(task.id)

    update_run = lambda balance: fire_and_forget_app.send_task("worker.tasks.end_run", args=[run_id, balance])
    update_run_position = lambda pos: fire_and_forget_app.send_task("worker.tasks.update_run_position", args=[run_id, pos])
    update_pnl = lambda pnl: fire_and_forget_app.send_task("worker.tasks.update_pnl", args=[run_id, pnl])

    def format_log(_, __, red_level, red_value):
        log_entry = f"{run_id} {asyncio.current_task().get_name()} {filename} {symb[choose]} {red_value}"
        if red_level == "I": logging.info(log_entry)
        else: logging.warning(log_entry)

    def save_state():
        state = {"run_id": run_id, "start_time": start_time, "start_balance": start_balance, "ann_params": ann_params, "weights": weights, "weights_timestamp": weights_timestamp, "aprs": aprs, "aprs_timestamp": aprs_timestamp, "position_direction": position_direction, "apr_change": apr_change, "apr_last": apr_last, "voms_state": voms.to_dict(), "pid": os.getpid(), "last_update": time.time()}
        file_path = os.path.join(SAVE_DIR, f"run_{run_id}.json")
        temp_file_path = os.path.join(SAVE_DIR, f"run_{run_id}.tmp")
        try:
            with open(temp_file_path, "w") as f: json.dump(state, f, indent=4); f.flush(); os.fsync(f.fileno())
            os.replace(temp_file_path, file_path)
        except Exception as e:
            logging.error(f"Failed to save state for run {run_id}: {e}")
            if os.path.exists(temp_file_path):
                try: os.remove(temp_file_path)
                except: pass

    def cleanup_state():
        file_path = os.path.join(SAVE_DIR, f"run_{run_id}.json")
        if os.path.exists(file_path):
            try:
                completed_path = os.path.join(COMPLETED_DIR, f"run_{run_id}_completed_{int(time.time())}.json")
                os.replace(file_path, completed_path)
                logging.info(f"Archived run {run_id} to {completed_path}")
            except OSError as e: logging.error(f"Error archiving state file {file_path}: {e}")

    def get_voms_values():
        metrics = voms.get_metrics()
        if metrics: return (metrics["position_size"], metrics["position_value"], metrics["unrealized_pnl"], metrics["account_balance"], metrics["margin_used"])
        else: return (0.0, 0.0, 0.0, virtual_balance, 0.0)

    get_position = lambda: get_voms_values()[0] if get_voms_values() else None
    get_balance = lambda: float(get_voms_values()[3])
    get_cross_maintenance_margin_used = lambda: float(get_voms_values()[4])

    # --- Main Loop ---
    try:
        if not resume_state:
            max_pos_val = await get_max_pos()
            if max_pos_val > 45:
                print(f"{symb[choose]} exceeds max margin allocation for symbol based on {max_pos_val}. Exiting")
                return
            start_balance, run_id = await create_new_run()
            start_time = time.time()

        if start_balance <= 0:
            logging.warning(f"Run {run_id} has invalid start_balance {start_balance}. Resetting.")
            start_balance = virtual_balance

        await loop.run_in_executor(None, save_state)
        last_save_time = time.time()
        
        # Add Jitter to prevent thundering herd
        await asyncio.sleep(random.uniform(1, 15))

        while True:
            await asyncio.sleep(decision_distance_seconds * random.random())
            instrument_price = get_price_from_shm(symb[choose])
            if instrument_price is None:
                format_log("E", False, "W", "Price not available in SHM, waiting..."); await asyncio.sleep(5); continue

            pos[choose] = (start_balance * (pos_weight + (random.random() / 100)) / instrument_price * pos_scaler / balance_divisor)
            if pos[choose] * instrument_price < 10.50: pos[choose] = 10.50 / instrument_price

            latest_weight = await get_weight()
            weights.append(latest_weight); weights_timestamp.append(time.time())
            side = "sell" if latest_weight <= 0 else "buy"
            
            # --- Inlined `manage_orders` logic ---
            balance = get_balance()
            duration = time.time() - start_time
            apr = ((balance - start_balance) / (duration / 3600) * 24 * 365.24 / start_balance) if start_balance > 0 and duration > 0 else 0
            if apr_last != 0: apr_change = (apr - apr_last) / apr_last
            else: apr_change = 0
            
            # Robust rolling APR calculation
            recent_aprs = [a for i, a in enumerate(aprs) if (time.time() - aprs_timestamp[i]) / 60 < rolling_apr_minutes]
            apr_last = sum(recent_aprs) / len(recent_aprs) if recent_aprs else 0
            
            update_pnl(balance - virtual_balance)
            aprs.append(apr); aprs_timestamp.append(time.time())
            voms.update_price(instrument_price)

            exit_trade = False
            if await get_exit():
                format_log("E", False, "I", "EXIT ON GLOBAL EXTERNAL EXIT SIGNAL"); await asyncio.sleep(random.random() * 100); await loop.run_in_executor(None, cleanup_state); break
            elif apr > (float(apr_target) * float(calculate_volatility_goal(get_volatility(symb[choose]), instrument_price, min_goal_weight, max_goal_weight, min_goal, max_goal))):
                format_log("E", False, "I", "EXIT APR TARGET"); exit_trade = True
            elif duration > max_duration:
                format_log("E", False, "I", "EXIT TIME STOP"); exit_trade = True

            if exit_trade:
                if get_position():
                    position_size = get_position()
                    exit_side = "sell" if position_size > 0 else "buy"
                    format_log("E", False, "I", f"BOOK EXIT ORDER {exit_side}, {abs(position_size)}")
                    if (abs(position_size) * instrument_price) > 10: await asyncio.sleep(DECISION_SLEEP)
                update_run(balance)
                format_log("E", False, "I", "EXIT TRADE SIGNAL"); await loop.run_in_executor(None, cleanup_state); break
            
            machine_vision_entropy = -1 # Placeholder, get_close_entropy needs to be defined/restored
            if machine_vision_entropy is not None and machine_vision_entropy < machine_vision_entropy_max:
                try:
                    if system_swing: side = "buy" if side == "sell" else "sell"
                    approve_trade = (position_direction == 0) or \
                                    (apr_change > 0 and ((position_direction > 0 and side == "buy") or (position_direction < 0 and side == "sell"))) or \
                                    (apr_change <= 0 and ((position_direction > 0 and side == "sell") or (position_direction < 0 and side == "buy")))
                    if approve_trade:
                        if position_direction == 0 and duration > max_direction_reversal:
                            format_log("E", False, "I", f"EXIT DUE TO REVERSAL {position_direction}, {duration}, {balance}")
                            if get_position():
                                p_size = get_position()
                                format_log("E", False, "I", f"BOOK EXIT ORDER {'sell' if p_size > 0 else 'buy'}, {abs(p_size)}, {instrument_price}")
                                if (abs(p_size) * instrument_price) > 10: await asyncio.sleep(DECISION_SLEEP)
                            update_run(balance); await loop.run_in_executor(None, cleanup_state); break
                        
                        cross_margin_ratio = get_cross_maintenance_margin_used() / balance if balance > 0 else 0
                        if not (cross_margin_ratio > max_cross_margin_ratio and ((position_direction > 0 and side == "buy") or (position_direction < 0 and side == "sell"))):
                            trade_condition = ((apr >= 0 and position_direction <= 0 and side == "sell") or (apr <= 0 and position_direction < 0 and side == "buy") or (apr >= 0 and position_direction >= 0 and side == "buy") or (apr <= 0 and position_direction > 0 and side == "sell"))
                            if trade_condition and abs(position_direction) <= max_pos_limit:
                                await asyncio.sleep(DECISION_SLEEP)
                                format_log("E", False, "I", f"BOOK NORMAL ORDER {side}, {pos[choose]}, {instrument_price} based on {position_direction} {apr_change}")
                                position_direction += 1 if side == "buy" else -1
                                update_run_position(position_direction)
                                voms.add_trade(pos[choose] * (1 if side == "buy" else -1))
                except Exception as e:
                    format_log("E", False, "W", f"Trade execution failed: {e}")

            if time.time() - last_save_time > 5:
                await loop.run_in_executor(None, save_state)
                last_save_time = time.time()
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        logging.info(f"Task for run {run_id} cancelled.")
    except Exception as e:
        format_log("E", False, "C", f"TASK EXCEPTION {traceback.format_exc()}" + str(e))
    finally:
        logging.info(f"Task for run {run_id} is shutting down.")

async def main():
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=NUM_RESULT_FETCHERS + 10) 
    result_fetcher = ResultFetcher(loop, executor, num_fetchers=NUM_RESULT_FETCHERS)

    def get_height():
        local_app = Celery("showcase_client_main", broker="redis://192.168.2.157:6379/0", backend="redis://192.168.2.157:6379/0")
        try:
            task = local_app.send_task("worker.tasks.get_max_run_height")
            res = local_app.AsyncResult(task.id).get(timeout=CELERY_GET_TIMEOUT)
            return res
        finally:
            local_app.close()

    def exit_runs_by_height(next_height):
        fire_and_forget_app.send_task("worker.tasks.set_exit_for_runs_by_height", args=[next_height])

    try:
        current_height = await loop.run_in_executor(executor, get_height)
        print(f"MANAGER: Current height is {current_height}")

        saved_runs = []
        try:
            run_files = glob.glob(os.path.join(SAVE_DIR, "run_*.json"))
            for run_file in run_files:
                with open(run_file) as f: saved_runs.append(json.load(f))
        except Exception as e:
            print(f"MANAGER: Failed to load saved runs: {e}")

        tasks = set()
        for state in saved_runs:
            if len(tasks) >= NUM_CHILDREN: break
            task = asyncio.create_task(trading_task(result_fetcher, resume_state=state))
            tasks.add(task)
        
        remaining_slots = NUM_CHILDREN - len(tasks)
        for _ in range(remaining_slots):
            task = asyncio.create_task(trading_task(result_fetcher))
            tasks.add(task)

        print(f"\nMANAGER: Initial pool of {len(tasks)} tasks launched. Entering management loop. 🔄")

        while tasks:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            for task in done:
                tasks.remove(task)
                new_task = asyncio.create_task(trading_task(result_fetcher))
                tasks.add(new_task)

            if loop.time() % 60 < 1: 
                try:
                    next_height = await loop.run_in_executor(executor, get_height)
                    if next_height > current_height:
                        print("MANAGER: TARGET HEIGHT REACHED, SHUTTING DOWN ALL TASKS.")
                        for task in pending: task.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        exit_runs_by_height(current_height + 1)
                        break
                except Exception as e:
                    print(f"MANAGER: Error checking height: {e}")
        
        print("MANAGER: All tasks have finished. Exiting.")

    finally:
        await result_fetcher.shutdown()
        executor.shutdown(wait=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMANAGER: Shutting down on user request...")
