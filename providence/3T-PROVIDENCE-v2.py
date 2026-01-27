import asyncio
import glob
import json
import logging
import math
import os
import random
import signal
import sys
import time
import traceback
import uuid
from functools import partial

from celery import Celery

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

# Define the ANSI escape codes
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
Starting: 3T RC3 (asyncio v2)...
""")
filename = os.path.basename(__file__)
print(f"{filename} - Tactical Trend Trader")

num_children = 100
decision_sleep = 0

# Ensure persistence directory exists
SAVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "saved_runs")
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# Ensure completed runs directory exists
COMPLETED_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "completed_runs"
)
if not os.path.exists(COMPLETED_DIR):
    os.makedirs(COMPLETED_DIR)

print(f"MANAGER: Starting with PID {os.getpid()}")


async def trading_task(resume_state=None):
    # This code is ONLY executed by the child task.
    # Configure Celery client to connect to the same broker
    app = Celery(
        "showcase_client",
        broker="redis://192.168.2.157:6379/0",
        backend="redis://192.168.2.157:6379/0",
    )

    loop = asyncio.get_running_loop()

    # -----------
    # IMPORTANT >~~~~~.
    #                  `.
    system_swing = bool(random.randint(0, 1))
    max_pos_limit = 1000
    # ___________

    # SIGNALS
    def _timeout_handler(_, __):
        raise TimeoutError

    # signal.signal(signal.SIGALRM, _timeout_handler) # Cannot set signal in non-main thread
    # signal.signal(signal.SIGINT, signal.SIG_IGN)

    # GLOBALS converted to LOCAL for asyncio
    weights = []
    weights_timestamp = []
    aprs = []
    aprs_timestamp = []

    system_debug = False
    controller_seed = 0
    # position_direction  # sum buy(pos) sell(neg) or zero flat
    # apr_change  # determine if APR has gone up or down since last iteration
    # if up can add if down can remove
    # apr_last  # for calculating change based on current apr value
    position_direction = 0
    apr_change = 0
    apr_last = 0

    symb = config.get("reconciliation_engine.symbols")
    symb_leverage = [
        80,
        80,
        80,
        80,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
        2,
    ]
    pos = [0] * len(symb)
    
    run_id = 0
    start_time = 0
    start_balance = 0
    # max_duration defined later via assignment

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    ######
    # ANN PARAMS
    golden_ratio = (math.sqrt(5) + 1) / 2  # Pacioli's divine proportion
    virtual_balance = 7000  # for trade simulation

    if resume_state:
        # Load params from saved state
        ann_params = resume_state["ann_params"]
        choose = ann_params["choose"]

        # Restore Globals
        weights = resume_state.get("weights", [])
        weights_timestamp = resume_state.get("weights_timestamp", [])
        aprs = resume_state.get("aprs", [])
        aprs_timestamp = resume_state.get("aprs_timestamp", [])
        position_direction = resume_state.get("position_direction", 0)
        apr_change = resume_state.get("apr_change", 0)
        apr_last = resume_state.get("apr_last", 0)
        start_time = resume_state.get("start_time", 0)
        start_balance = resume_state.get("start_balance", 0)
        run_id = resume_state.get("run_id", 0)
        controller_seed = resume_state.get("controller_seed", 0)
        system_swing = ann_params.get("system_swing", False)  # Restore swing setting

        # Initialize VOMS and restore state
        voms = VOMS(starting_balance=virtual_balance, leverage=symb_leverage[choose])
        if "voms_state" in resume_state:
            voms.from_dict(resume_state["voms_state"])

        # Unpack params for local scope usage
        max_duration = ann_params["max_duration"]
        max_direction_reversal = ann_params["max_direction_reversal"]
        balance_divisor = ann_params["balance_divisor"]
        pos_scaler = ann_params["pos_scaler"]
        max_cross_margin_ratio = ann_params["max_cross_margin_ratio"]
        apr_target = ann_params["apr_target"]
        pos_weight = ann_params["pos_weight"]
        min_goal = ann_params["min_goal"]
        max_goal = ann_params["max_goal"]
        min_goal_weight = ann_params["min_goal_weight"]
        max_goal_weight = ann_params["max_goal_weight"]
        volatility_entropy_window_minutes = ann_params[
            "volatility_entropy_window_minutes"
        ]
        volatility_entropy_window_samples = ann_params[
            "volatility_entropy_window_samples"
        ]
        machine_vision_entropy_max = ann_params["machine_vision_entropy_max"]
        rolling_apr_minutes = ann_params["rolling_apr_minutes"]
        decision_distance_seconds = ann_params["decision_distance_seconds"]

        logging.info(f"RESUMED RUN {run_id} for symbol {symb[choose]}")
    else:
        # Standard Random Initialization
        if len(sys.argv) < 2 or sys.argv[1] == "" or int(sys.argv[1]) > len(symb) - 1:
            choose = random.randint(0, len(symb) - 1)
        else:
            choose = int(sys.argv[1])

        voms = VOMS(starting_balance=virtual_balance, leverage=symb_leverage[choose])

        max_duration = random.randint(300, 86400 * 5)
        max_direction_reversal = int(max_duration / random.randint(1, 10))
        balance_divisor = 5  # allows normalization of risk/reward/position size values as account balance is increased to reduce margin call risk without causing the other metrics to increase as a ratio of starting balance
        pos_scaler = random.uniform(golden_ratio, golden_ratio * 2) / 1000
        max_cross_margin_ratio = random.randint(25, 100) / 100 / balance_divisor
        apr_target = random.randint(20, 100) / balance_divisor / golden_ratio
        pos_weight = 0.032732855219630354 / 3
        # volatility bands
        min_goal = random.randint(1, 9) / 10
        max_goal = random.randint(1, 10)
        min_goal_weight = random.randint(1, 38) / 10000
        max_goal_weight = random.randint(69, 200) / 10000
        volatility_entropy_window_minutes = random.randint(1, 15)
        volatility_entropy_window_samples = random.randint(5, 20)
        machine_vision_entropy_max = random.randint(5, 15) / 100
        # apr
        rolling_apr_minutes = random.randint(1, 30)
        # space between trades
        decision_distance_seconds = 0
        # decision_distance_seconds = random.randint( 1, 10000 ) / 100  # (minimum) can be floating point fractional
        max_cross_margin_ratio = 0.05 / balance_divisor
        pos_weight = 0.032732855219630354 / 3

        # store the params
        ann_params = {
            "virtual_balance": virtual_balance,
            "choose": choose,
            "max_duration": max_duration,
            "max_direction_reversal": max_direction_reversal,
            "balance_divisor": balance_divisor,
            "pos_scaler": pos_scaler,
            "max_cross_margin_ratio": max_cross_margin_ratio,
            "apr_target": apr_target,
            "pos_weight": pos_weight,
            "min_goal": min_goal,
            "max_goal": max_goal,
            "min_goal_weight": min_goal_weight,
            "max_goal_weight": max_goal_weight,
            "volatility_entropy_window_minutes": volatility_entropy_window_minutes,
            "volatility_entropy_window_samples": volatility_entropy_window_samples,
            "machine_vision_entropy_max": machine_vision_entropy_max,
            "rolling_apr_minutes": rolling_apr_minutes,
            "decision_distance_seconds": decision_distance_seconds,
            "system_swing": system_swing,
        }

    #####
    # MARGIN NORMALIZED SYMBOL PREFILTER
    async def get_max_pos():
        task = app.send_task("worker.tasks.get_active_run_count", args=[symb[choose]])
        task_result = await loop.run_in_executor(
            None, partial(task.get, timeout=decision_sleep)
        )
        return float(task_result / symb_leverage[choose])

    # Only check margin limit for NEW runs. Resumed runs are already active.
    if not resume_state:
        max_pos_val = await get_max_pos()

        if max_pos_val > 45:
            print(
                f"{symb[choose]} exceeds max margin allocation for symbol based on {max_pos_val}. Exiting"
            )
            return
        else:
            print(f"{symb[choose]} qualifies for margin based on {max_pos_val}")

    ###
    # Functional Units

    def format_log(_, __, red_level, red_value):
        log_entry = f"{run_id} {asyncio.current_task().get_name()} {filename} {symb[choose]} {red_value}"
        if red_level == "I":
            logging.info(log_entry)
        else:
            logging.warning(log_entry)

    def save_state():
        """Continuously save state to JSON for persistence and telemetry."""
        # This function remains synchronous as file I/O is fast enough here
        # and using aiofiles would add a dependency. For extreme performance,
        # this could be moved to an executor.
        current_time = time.time()

        entropy_cutoff = current_time - (volatility_entropy_window_minutes * 60) - 60
        weight_slice_idx = 0
        for i, ts in enumerate(weights_timestamp):
            if ts > entropy_cutoff:
                weight_slice_idx = i
                break
        if weight_slice_idx > 0:
            weights[:] = weights[weight_slice_idx:]
            weights_timestamp[:] = weights_timestamp[weight_slice_idx:]

        apr_cutoff = current_time - (rolling_apr_minutes * 60) - 60
        apr_slice_idx = 0
        for i, ts in enumerate(aprs_timestamp):
            if ts > apr_cutoff:
                apr_slice_idx = i
                break
        if apr_slice_idx > 0:
            aprs[:] = aprs[apr_slice_idx:]
            aprs_timestamp[:] = aprs_timestamp[apr_slice_idx:]

        state = {
            "run_id": run_id,
            "start_time": start_time,
            "start_balance": start_balance,
            "controller_seed": controller_seed,
            "ann_params": ann_params,
            "weights": weights,
            "weights_timestamp": weights_timestamp,
            "aprs": aprs,
            "aprs_timestamp": aprs_timestamp,
            "position_direction": position_direction,
            "apr_change": apr_change,
            "apr_last": apr_last,
            "voms_state": voms.to_dict(),
            "pid": os.getpid(),  # Note: PID is now the same for all tasks
            "last_update": current_time,
        }

        file_path = os.path.join(SAVE_DIR, f"run_{run_id}.json")
        temp_file_path = os.path.join(SAVE_DIR, f"run_{run_id}.tmp")
        try:
            with open(temp_file_path, "w") as f:
                json.dump(state, f, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file_path, file_path)
        except Exception as e:
            logging.error(f"Failed to save state for run {run_id}: {e}")
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass

    def cleanup_state():
        """Move state file to completed_runs on clean exit."""
        file_path = os.path.join(SAVE_DIR, f"run_{run_id}.json")
        if os.path.exists(file_path):
            try:
                timestamp = int(time.time())
                completed_filename = f"run_{run_id}_completed_{timestamp}.json"
                completed_path = os.path.join(COMPLETED_DIR, completed_filename)
                os.replace(file_path, completed_path)
                logging.info(f"Archived run {run_id} to {completed_path}")
            except OSError as e:
                logging.error(f"Error archiving state file {file_path}: {e}")

    def get_voms_values():
        metrics = voms.get_metrics()
        if metrics:
            return (
                metrics["position_size"],
                metrics["position_value"],
                metrics["unrealized_pnl"],
                metrics["account_balance"],
                metrics["margin_used"],
            )
        else:
            return (0.0, 0.0, 0.0, 0.0, 0.0)

    async def get_weight():
        task = app.send_task("worker.tasks.get_market_weight", args=[symb[choose]])
        task_result = await loop.run_in_executor(
            None, partial(task.get, timeout=decision_sleep)
        )
        return task_result

    def get_position():
        result = get_voms_values()
        return result if result[0] else None

    def get_balance():
        result = get_voms_values()
        return float(result[3]) if result[3] else virtual_balance

    def get_cross_maintenance_margin_used():
        result = get_voms_values()
        return float(result[4]) if result[4] else 0

    async def create_new_run():
        start_balance = virtual_balance
        run_params = {
            "start_balance": start_balance,
            "max_duration": max_duration,
            "symbol": symb[choose],
            "ann_params": json.dumps(ann_params),
            "controller_seed": controller_seed,
            "pid": os.getpid(),
            "host": hex(uuid.getnode()),
        }
        task = app.send_task("worker.tasks.create_run", kwargs=run_params)
        run_id = await loop.run_in_executor(
            None, partial(task.get, timeout=decision_sleep)
        )
        return start_balance, run_id

    def update_run(balance):
        app.send_task("worker.tasks.end_run", args=[run_id, balance])

    def update_run_position(pos):
        app.send_task("worker.tasks.update_run_position", args=[run_id, pos])

    def update_pnl(pnl):
        app.send_task("worker.tasks.update_pnl", args=[run_id, pnl])

    async def get_exit():
        task = app.send_task("worker.tasks.get_exit_status", args=[run_id])
        task_result = await loop.run_in_executor(
            None, partial(task.get, timeout=decision_sleep)
        )
        return task_result

    def get_rolling_apr():
        x = [
            apr
            for i, apr in enumerate(aprs)
            if (time.time() - aprs_timestamp[i]) / 60 < rolling_apr_minutes
        ]
        return sum(x) / len(x) if x else 0

    def get_close_entropy():
        x = []
        for i, weight in enumerate(weights):
            calc_minutes = (time.time() - weights_timestamp[i]) / 60
            if calc_minutes < volatility_entropy_window_minutes:
                if weight > 0:
                    x.append(1)
                elif weight < 0:
                    x.append(-1)
                else:
                    x.append(0)
        x.reverse()
        if len(x) > volatility_entropy_window_samples:
            return calculate_permutation_entropy(x)
        else:
            return 1

    ###
    # Main
    try:
        if not resume_state:
            start_balance, run_id = await create_new_run()
            start_time = time.time()

        if start_balance <= 0:
            logging.warning(f"Run {run_id} has invalid start_balance {start_balance}. Resetting to {virtual_balance}")
            start_balance = virtual_balance

        save_state()
        last_save_time = time.time()

        while True:
            # The original signal.alarm is not compatible with asyncio.
            # Using asyncio.wait_for for timeout handling if needed,
            # but the logic seems to rely on celery timeouts more.

            await asyncio.sleep(decision_distance_seconds * random.random())
            # Removed global instrument_price to prevent state bleeding between async tasks
            instrument_price = get_price_from_shm(symb[choose])

            if instrument_price is None:
                format_log("E", False, "W", "Price not available in SHM, waiting...")
                await asyncio.sleep(5)
                continue

            pos[choose] = (
                start_balance
                * (pos_weight + (random.random() / 100))
                / instrument_price
                * pos_scaler
                / balance_divisor
            )

            if pos[choose] * instrument_price < 10.50:
                pos[choose] = 10.50 / instrument_price

            latest_weight = await get_weight()
            weights.append(latest_weight)
            weights_timestamp.append(time.time())
            side = "sell" if latest_weight <= 0 else "buy"

            async def manage_orders(side, price, symbol, amount):
                nonlocal position_direction  # sum buy(pos) sell(neg) or zero flat
                nonlocal apr_change  # determine if APR has gone up or down since last iteration
                # if up can add if down can remove
                nonlocal apr_last  # for calculating change based on current apr value

                symbol_position = get_position()
                balance = get_balance()
                cross_maintenance_margin_used = get_cross_maintenance_margin_used()
                duration = time.time() - start_time
                
                if start_balance > 0 and duration > 0:
                    apr = (
                        (balance - start_balance)
                        / (duration / 60 / 60)
                        * 24
                        * 365.24
                        / start_balance
                    )
                else:
                    apr = 0

                if apr_last != 0:
                    apr_change = (apr - apr_last) / apr_last

                    # ensure negative aprs are compared correctly
                    if apr_last < 0:
                        apr_change = apr_change * -1
                else:
                    apr_change = 0
                apr_last = get_rolling_apr()
                cross_margin_ratio = cross_maintenance_margin_used / balance

                update_pnl(balance - virtual_balance)
                aprs.append(apr)
                aprs_timestamp.append(time.time())

                voms.update_price(instrument_price)

                # EXIT
                if system_debug:
                    format_log(
                        "E",
                        False,
                        "I",
                        f"TIME REMAINING {abs(int(duration - max_duration))}",
                    )
                exit_trade = False
                if await get_exit():
                    format_log("E", False, "I", "EXIT ON GLOBAL EXTERNAL EXIT SIGNAL")
                    await asyncio.sleep(random.random() * 100)
                    cleanup_state()  # CLEAN EXIT
                    return False
                elif apr > (
                    float(apr_target)
                    * float(
                        calculate_volatility_goal(
                            get_volatility(symb[choose]),
                            instrument_price,
                            min_goal_weight,
                            max_goal_weight,
                            min_goal,
                            max_goal,
                        )
                    )
                ):
                    format_log("E", False, "I", "EXIT APR TARGET")
                    exit_trade = True
                elif duration > max_duration:
                    format_log("E", False, "I", "EXIT TIME STOP")
                    exit_trade = True

                if exit_trade:
                    symbol_position = get_position()

                    if symbol_position:
                        position_size = symbol_position[0]
                        side = "sell"
                        if position_size < 0:
                            side = "buy"
                            position_size = position_size * -1
                        format_log(
                            "E", False, "I", f"BOOK EXIT ORDER {side}, {position_size}"
                        )
                        # anything smaller is considered dust for different cleanup
                        if (float(position_size) * instrument_price) > 10:
                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                            # entry style - gives APR time to settle
                            # exit style  - prevents entering new run too quickly
                            await asyncio.sleep(decision_sleep)
                    update_run(balance)
                    format_log("E", False, "I", "EXIT TRADE SIGNAL")
                    cleanup_state()  # CLEAN EXIT
                    return False

                # TRADE
                # ensure minimal direction entropy 0 is perfect 1 is totally random
                machine_vision_entropy = get_close_entropy()
                if machine_vision_entropy is not None:
                    if machine_vision_entropy < machine_vision_entropy_max:
                        # HANDLE NORMAL ORDER FLOW
                        try:
                            if system_swing:
                                if side == "sell":
                                    side = "buy"
                                else:
                                    side = "sell"

                            approve_trade = False
                            if position_direction == 0:
                                approve_trade = True
                            elif apr_change > 0:
                                if position_direction > 0 and side == "buy":
                                    approve_trade = True
                                elif position_direction < 0 and side == "sell":
                                    approve_trade = True
                            else:
                                if position_direction > 0 and side == "sell":
                                    approve_trade = True
                                elif position_direction < 0 and side == "buy":
                                    approve_trade = True

                            if approve_trade:
                                if (
                                    position_direction == 0
                                    and duration > max_direction_reversal
                                ):
                                    # strategy has flattened itself, reset run with fresh APR calculations
                                    format_log(
                                        "E",
                                        False,
                                        "I",
                                        f"EXIT DUE TO REVERSAL {position_direction}, {duration}, {balance}",
                                    )
                                    symbol_position = get_position()
                                    if symbol_position:
                                        format_log(
                                            "E",
                                            False,
                                            "I",
                                            "POSITION EXISTS POST REVERSAL - FULLY EXIT",
                                        )
                                        position_size = symbol_position[0]
                                        side = "sell"
                                        if position_size < 0:
                                            position_size = position_size * -1
                                            side = "buy"
                                        format_log(
                                            "E",
                                            False,
                                            "I",
                                            f"BOOK EXIT ORDER {side}, {position_size}, {instrument_price}",
                                        )
                                        if (
                                            float(position_size) * instrument_price
                                        ) > 10:
                                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                                            # entry style - gives APR time to settle
                                            # exit style  - prevents entering new run too quickly
                                            await asyncio.sleep(decision_sleep)
                                    update_run(balance)
                                    cleanup_state()  # CLEAN EXIT
                                    return False

                                if (
                                    cross_margin_ratio > max_cross_margin_ratio
                                    and (position_direction > 0 and side == "buy")
                                ) or (
                                    cross_margin_ratio > max_cross_margin_ratio
                                    and (position_direction < 0 and side == "sell")
                                ):
                                    # format_log( 'E', False, 'I', f'SKIP ENTRY: CANNOT ADD DUE TO MARGIN BUFFER {cross_margin_ratio}' )
                                    return True
                                else:
                                    if (
                                        (
                                            apr >= 0
                                            and position_direction <= 0
                                            and side == "sell"
                                        )
                                        or (
                                            apr <= 0
                                            and position_direction < 0
                                            and side == "buy"
                                        )
                                        or (
                                            apr >= 0
                                            and position_direction >= 0
                                            and side == "buy"
                                        )
                                        or (
                                            apr <= 0
                                            and position_direction > 0
                                            and side == "sell"
                                        )
                                    ):
                                        if abs(position_direction) <= max_pos_limit:
                                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                                            # entry style - gives APR time to settle
                                            # exit style  - prevents entering new run too quickly
                                            await asyncio.sleep(decision_sleep)
                                            format_log(
                                                "E",
                                                False,
                                                "I",
                                                f"BOOK NORMAL ORDER {side}, {amount}, {instrument_price} based on {position_direction} {apr_change}",
                                            )
                                            position_size_metric = 1
                                            if side == "sell":
                                                position_direction += -1
                                                position_size_metric = -1
                                            else:
                                                position_direction += 1
                                            update_run_position(position_direction)
                                            voms.add_trade(
                                                pos[choose] * position_size_metric
                                            )
                                        # else:
                                        #    format_log( 'E', False, 'I', f"SKIP ENTRY: MAX POS {max_pos_limit} REACHED {side}, {amount}, {instrument_price} based on {apr}" )
                                    # else:
                                    #    format_log( 'E', False, 'I', f"SKIP ENTRY: NEGATIVE APR {side}, {amount}, {instrument_price} based on {apr}" )

                            # else:
                            #    format_log( 'E', False, 'I', f"REJECT NORMAL ORDER {side}, {amount}, {instrument_price} based on {position_direction} {apr_change}" )

                        except Exception as e:
                            format_log("E", False, "W", "create_order() failed")
                            format_log("E", False, "W", str(e))
                return True # Signal to continue

            should_continue = await manage_orders(side, instrument_price, symb[choose], pos[choose])
            if not should_continue:
                break  # Exit the while loop

            if time.time() - last_save_time > 5:
                save_state()
                last_save_time = time.time()

            await asyncio.sleep(1)

    except asyncio.CancelledError:
        logging.info(f"Task for run {run_id} cancelled.")
    except Exception as e:
        format_log("E", False, "C", f"TASK EXCEPTION {traceback.format_exc()}" + str(e))
    finally:
        logging.info(f"Task for run {run_id} is shutting down.")
        # Final state save? Might not be necessary if cleanup_state was called.


async def main():
    """The main entry point for the asyncio-based manager."""
    parent_app = Celery(
        "showcase_client",
        broker="redis://192.168.2.157:6379/0",
        backend="redis://192.168.2.157:6379/0",
    )
    loop = asyncio.get_running_loop()

    def get_height():
        task = parent_app.send_task("worker.tasks.get_max_run_height")
        return task.get(timeout=decision_sleep)

    def exit_runs_by_height(next_height):
        parent_app.send_task("worker.tasks.set_exit_for_runs_by_height", args=[next_height])

    current_height = await loop.run_in_executor(None, get_height)
    print(f"MANAGER: Current height is {current_height}")

    saved_runs = []
    run_files = glob.glob(os.path.join(SAVE_DIR, "run_*.json"))
    for run_file in run_files:
        try:
            with open(run_file) as f:
                saved_runs.append(json.load(f))
        except Exception as e:
            print(f"MANAGER: Failed to load saved run {run_file}: {e}")

    tasks = set()
    # Resume saved runs first
    for state in saved_runs:
        if len(tasks) >= num_children:
            break
        task = asyncio.create_task(trading_task(resume_state=state))
        tasks.add(task)
        print(f"MANAGER: Resumed run {state.get('run_id')}")

    # Spawn fresh children if quota allows
    remaining_slots = num_children - len(tasks)
    for _ in range(remaining_slots):
        task = asyncio.create_task(trading_task())
        tasks.add(task)

    print(f"\nMANAGER: Initial pool of {len(tasks)} tasks launched. Entering management loop. 🔄")

    while tasks:
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            # Check for height changes to coordinate shutdown across hosts
            next_height = await loop.run_in_executor(None, get_height)
            if next_height > current_height:
                print("MANAGER: TARGET REACHED, SHUTTING DOWN ALL TASKS.")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                await loop.run_in_executor(None, exit_runs_by_height, current_height + 1)
                break

            # Respawn tasks that have finished
            for task in done:
                print("MANAGER: A task has finished. Respawning a new one.")
                tasks.remove(task)
                new_task = asyncio.create_task(trading_task())
                tasks.add(new_task)

        except asyncio.CancelledError:
            print("MANAGER: Main loop cancelled, shutting down.")
            break

    print("MANAGER: All tasks have finished. Exiting.")


if __name__ == "__main__":
    main_task = None
    try:
        main_task = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMANAGER: Shutting down on user request...")
        if main_task and not main_task.done():
            main_task.cancel()
            asyncio.get_event_loop().run_until_complete(main_task)
