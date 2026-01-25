import math
import ctypes
import struct
import uuid
import traceback
import signal
import random
import time
import json
import asyncio
import os
import sys
import logging
from multiprocessing import resource_tracker, shared_memory
import numpy as np
from celery import Celery
project_root = '/opt/3T'
sys.path.append(str(project_root))
from shared.voms import VOMS
from shared.config import config

# Define the ANSI escape codes
ORANGE = '\033[38;5;202m'
RESET = '\033[0m'

print( f"""
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
Starting: 3T RC3...
""")
filename = os.path.basename(__file__)
print( f"{filename} - Tactical Trend Trader" )

num_children = 2000
decision_sleep = 0
child_pids = []

SHM_PRICE_NAME = "price_data_shm"

print( f"PARENT: Starting with PID {os.getpid()}" )

def load_perm_entropy_library():
    """Load the permutation entropy C++ library."""
    try:
        lib_path = "/opt/3T/celery-services/cpp/bin/libperm_entropy_cpu.so"
        lib = ctypes.CDLL(lib_path)

        func = lib.calculate_cpu_perm_entropy
        func.argtypes = [
            np.ctypeslib.ndpointer(dtype=np.float64, flags="C_CONTIGUOUS"),
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int,
        ]
        func.restype = ctypes.c_double
        print("✅ Successfully loaded permutation entropy library")
        return func
    except (OSError, AttributeError) as e:
        print(f"⚠️ WARNING: Could not load permutation entropy library. Error: {e}")
        return None

def calculate_permutation_entropy(data):
    order = 3
    delay = 1
    try:
        if not calculate_cpu_entropy:
            error_msg = "Permutation entropy library not loaded"
            span.set_attribute("error", True)
            span.add_event(error_msg)
            return {"error": error_msg, "result": None}

        # Convert data to numpy array
        x_np = np.array(data, dtype=np.float64)
        n = len(x_np)

        # Validate inputs
        if n < order:
            error_msg = f"Data length ({n}) must be >= order ({order})"
            span.set_attribute("error", True)
            span.add_event(error_msg)
            return {"error": error_msg, "result": None}

        result = calculate_cpu_entropy(x_np, n, order, delay)

        return result

    except Exception as e:
        print(f"Error in calculate_permutation_entropy task: {e}")
        return None


# Load the library on module import
calculate_cpu_entropy = load_perm_entropy_library()

def child_task():
    # This code is ONLY executed by the child.
    # Configure Celery client to connect to the same broker
    app = Celery(
        "showcase_client",
        broker="redis://192.168.2.157:6379/0",
        backend="redis://192.168.2.157:6379/0",
    )


    #-----------
    # IMPORTANT >~~~~~.
    #                  `.
    global system_swing
    system_swing = bool(random.randint(0, 1))
    max_pos_limit = 1000
    #___________

    # SIGNALS
    def _timeout_handler( _, __ ):
        raise TimeoutError

    signal.signal( signal.SIGALRM, _timeout_handler )

    # GLOBALS
    global weights
    weights = []
    global weights_timestamp
    weights_timestamp = []
    global aprs
    aprs = []
    global aprs_timestamp
    aprs_timestamp = []

    system_debug = False
    controller_seed = 0
    global position_direction # sum buy(pos) sell(neg) or zero flat
    global apr_change # determine if APR has gone up or down since last iteration
                      # if up can add if down can remove
    global apr_last   # for calculating change based on current apr value
    position_direction = 0
    apr_change = 0
    apr_last = 0

    symb = config.get("reconciliation_engine.symbols")
    symb_leverage = [ 80,80,80,80,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2 ]
    pos = [0] * len(symb)
    global run_id
    global start_time
    global start_balance
    global max_duration
    run_id = 0
    start_time = 0
    start_balance = 0

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    ######
    # ANN PARAMS
    golden_ratio = (math.sqrt(5)+1)/2 # Pacioli's divine proportion
    virtual_balance = 7000 # for trade simulation
    if len(sys.argv) < 2 or sys.argv[1] == "" or int(sys.argv[1]) > len(symb)-1:
        choose = random.randint( 0, len( symb )-1 )
    else:
        choose = int( sys.argv[1] )

    voms = VOMS( starting_balance=virtual_balance, leverage=symb_leverage[choose] )

    max_duration = random.randint( 300, 86400*5 )
    max_direction_reversal = int( max_duration / random.randint(1,10) )
    balance_divisor = 5 # allows normalization of risk/reward/position size values as account balance is increased to reduce margin call risk without causing the other metrics to increase as a ratio of starting balance
    pos_scaler = random.uniform( golden_ratio, golden_ratio*2 ) / 1000
    max_cross_margin_ratio = random.randint( 25, 100 ) / 100 / balance_divisor
    apr_target = random.randint(20,100) / balance_divisor / golden_ratio
    pos_weight = 0.032732855219630354 / 3
    # volatility bands
    min_goal = random.randint( 1, 9 ) / 10
    max_goal = random.randint( 1, 10 )
    min_goal_weight = random.randint( 1, 38 ) / 10000
    max_goal_weight = random.randint( 69, 200 ) / 10000
    volatility_entropy_window_minutes = random.randint(1,15)
    volatility_entropy_window_samples = random.randint(5,20)
    machine_vision_entropy_max = random.randint(5,15) / 100
    # apr
    rolling_apr_minutes = random.randint( 1, 30 )
    # space between trades
    decision_distance_seconds = 0
    #decision_distance_seconds = random.randint( 1, 10000 ) / 100  # (minimum) can be floating point fractional
    max_cross_margin_ratio = 0.05 / balance_divisor
    pos_weight = 0.032732855219630354 / 3

    #####
    # MARGIN NORMALIZED SYMBOL PREFILTER
    def get_max_pos():
        result = app.send_task( "worker.tasks.get_active_run_count", args=[symb[choose]] )
        task_result = result.get(timeout=decision_sleep)
        return float( task_result/symb_leverage[choose] )

    max_pos_val = get_max_pos()

    if max_pos_val > 45:
        print( f"{symb[choose]} exceeds max margin allocation for symbol based on {max_pos_val}. Exiting" )
        quit()
    else:
        print( f"{symb[choose]} qualifies for margin based on {max_pos_val}" )

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
            "system_swing": system_swing
        }

    ###
    # Functional Units
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

    def format_log( _, __, red_level, red_value ):
        log_entry = f"{run_id} {os.getpid()} {filename} {symb[choose]} {red_value}"
        if red_level == "I":
            logging.info( log_entry )
        else:
            logging.warning( log_entry )

    MAX_SYMBOLS = 1000
    ENTRY_SIZE = 24
    HEADER_SIZE = 8
    TOTAL_SIZE = HEADER_SIZE + (MAX_SYMBOLS * ENTRY_SIZE)

    # Staleness threshold
    MAX_AGE_SECONDS = 5

    def symbol_to_hash(symbol):
        """Convert symbol string to 64-bit hash for fast lookup"""
        import hashlib

        # Use SHA256 for deterministic hashing across processes
        hash_bytes = hashlib.sha256(symbol.encode("utf-8")).digest()
        # Take first 8 bytes as 64-bit integer
        return int.from_bytes(hash_bytes[:8], byteorder="little") & 0x7FFFFFFFFFFFFFFF

    def get_voms_values_OLD():
        if len(voms.trades) > 0:
            metrics = voms.get_metrics( )
            position_size = metrics['position_size']
            position_value = metrics['position_value']
            unrealized_pnl = metrics['unrealized_pnl']
            account_balance = metrics['account_balance']
            cross_maintenance_margin_used = metrics['margin_used']
            return (
                    position_size, position_value, unrealized_pnl, account_balance, cross_maintenance_margin_used
                    )
        else:
            return ( False, False, False, False, False )

    def get_voms_values():
        """
        Safely retrieves the latest metrics from the VOMS object.
        """
        metrics = voms.get_metrics()

        if metrics:
            # If metrics is not None, unpack the values
            position_size = metrics['position_size']
            position_value = metrics['position_value']
            unrealized_pnl = metrics['unrealized_pnl']
            account_balance = metrics['account_balance']
            cross_maintenance_margin_used = metrics['margin_used']
            return (
                position_size, position_value, unrealized_pnl,
                account_balance, cross_maintenance_margin_used
            )
        else:
            # If metrics is None (no trades yet), return default numeric value
            return (0.0, 0.0, 0.0, 0.0, 0.0)


    def calculate_volatility_goal(current_volatility):
        min_volatility = instrument_price * min_goal_weight
        max_volatility = instrument_price * max_goal_weight
        
        # Scale the current volatility to a value between 0 and 1
        scaled_volatility = (current_volatility - min_volatility) / (max_volatility - min_volatility)
        
        # Apply an exponential function to the scaled volatility
        goal = np.exp(scaled_volatility * np.log(max_goal / min_goal)) * min_goal
        
        if goal < min_goal:
            return min_goal
        elif goal > max_goal:
            return max_goal
        return goal

    def get_volatility(symbol, max_retries=None):
        SHM_NAME = "market_data_shm"
        """
        Get volatility for a symbol from shared memory.
        Returns (volatility, timestamp) if found and fresh, otherwise retries.
        """
        symbol_hash = symbol_to_hash(symbol)
        retry_count = 0

        while max_retries is None or retry_count < max_retries:
            try:
                # Connect to existing shared memory
                shm = shared_memory.SharedMemory(name=SHM_NAME)

                # CRITICAL: Unregister from resource tracker to prevent cleanup
                # This prevents the shared memory from being destroyed when getter exits
                resource_tracker.unregister(shm._name, "shared_memory")

                try:
                    # Create numpy array view
                    buffer = np.ndarray((TOTAL_SIZE,), dtype=np.uint8, buffer=shm.buf)

                    # Read number of symbols
                    num_symbols = buffer[:HEADER_SIZE].view(np.uint64)[0]

                    # Search for our symbol
                    current_time = time.time()

                    for i in range(min(num_symbols, MAX_SYMBOLS)):
                        offset = HEADER_SIZE + (i * ENTRY_SIZE)

                        # Unpack entry
                        entry_data = buffer[offset : offset + ENTRY_SIZE].tobytes()
                        stored_hash, volatility, timestamp = struct.unpack("<Qdd", entry_data)

                        if stored_hash == symbol_hash:
                            # Check staleness
                            age = current_time - timestamp
                            if age <= MAX_AGE_SECONDS:
                                return volatility
                            else:
                                print(
                                    f"Volatility for {symbol} is stale (age: {age:.1f}s), retrying..."
                                )
                                break

                    print(f"Symbol {symbol} not found in shared memory, retrying...")

                finally:
                    shm.close()

            except FileNotFoundError:
                print("Shared memory not found, setter may not be running. Retrying...")
            except Exception as e:
                print(f"Error reading shared memory: {e}")

            retry_count += 1
            if max_retries is None or retry_count < max_retries:
                print(f"Sleeping 30 seconds before retry {retry_count}...")
                time.sleep(30)

        raise TimeoutError(
            f"Could not get fresh volatility for {symbol} after {retry_count} attempts"
        )

    def get_weight():
        result = app.send_task( "worker.tasks.get_market_weight", args=[symb[choose]] )
        task_result = result.get(timeout=decision_sleep)
        return task_result

    def get_position():
        result = get_voms_values()
        if result[0]:
            return result
        else:
            return None

    def get_balance():
        result = get_voms_values()
        if result[3]:
            return float( result[3] )
        else:
            return virtual_balance


    def get_cross_maintenance_margin_used():
        result = get_voms_values()
        if result[4]:
            return float( result[4] )
        else:
            return 0

    def create_new_run():
        start_balance = virtual_balance
        run_params = {
            "start_balance": start_balance,
            "max_duration": max_duration,
            "symbol": symb[choose],
            "ann_params": json.dumps( ann_params ),
            "controller_seed": controller_seed,
            "pid": os.getpid(),
            "host": hex(uuid.getnode())
        }
        result = app.send_task(
            "worker.tasks.create_run",
            kwargs=run_params,
        )
        run_id = result.get(timeout=decision_sleep)
        
        return start_balance, run_id

    def update_run( balance ):
        app.send_task( "worker.tasks.end_run", args=[run_id, balance] )

    def update_run_position( pos ):
        app.send_task( "worker.tasks.update_run_position", args=[run_id, pos ] )

    def update_pnl( pnl ):
        app.send_task( "worker.tasks.update_pnl", args=[run_id, pnl] )

    def get_exit( ):
        result = app.send_task( "worker.tasks.get_exit_status", args=[run_id] )
        task_result = result.get(timeout=decision_sleep)
        return task_result 

    def get_rolling_apr():
        x = []
        for i in range(0, len(aprs) ):
            calc_minutes = ( time.time() - aprs_timestamp[i] ) / 60
            if calc_minutes < rolling_apr_minutes:
                x.append( aprs[i] )
        if len( x ) > 0:
            return sum( x ) / len( x )
        else:
            return 0

    def get_close_entropy():
        x = []
        for i in range(0, len(weights) ):
            calc_minutes = ( time.time() - weights_timestamp[i] ) / 60
            if calc_minutes < volatility_entropy_window_minutes:
                if weights[i] > 0:
                    x.append( 1 )
                elif weights[i] < 0:
                    x.append( -1 )
                elif weights[i] == 0:
                    x.append( 0 )

        x.reverse()
        if len( x ) > volatility_entropy_window_samples:
            result = calculate_permutation_entropy( x )
            return result
        else:
            return 1

    ###
    # Main
    start_balance, run_id = create_new_run()

    start_time = time.time()


    while True:
        try:
            signal.alarm( decision_sleep + int( decision_distance_seconds * 2 ) + 60 )

            time.sleep( decision_distance_seconds * random.random() )
            global instrument_price
            instrument_price = get_price_from_shm( symb[choose] )

            pos[choose] = start_balance * (pos_weight + (random.random()/100)) / instrument_price * pos_scaler / balance_divisor

            # assume this will meet the minimum order size for the exchange
            if pos[choose] * instrument_price < 10.50:
                pos[choose] = 10.50 / instrument_price

            latest_weight = get_weight()
            weights.append( latest_weight )
            weights_timestamp.append( time.time() )
            side = 'sell'
            if latest_weight > 0:
                side = 'buy'


            async def manage_orders(side, price, symbol,amount):
                global position_direction # sum buy(pos) sell(neg) or zero flat
                global apr_change # determine if APR has gone up or down since last iteration
                                  # if up can add if down can remove
                global apr_last   # for calculating change based on current apr value

                symbol_position = get_position()
                balance = get_balance()
                cross_maintenance_margin_used = get_cross_maintenance_margin_used()
                duration = time.time() - start_time
                apr = (balance-start_balance) / (duration/60/60)*24*365.24 / start_balance
                if apr_last != 0:
                    apr_change = (apr - apr_last)/apr_last  

                    # ensure negative aprs are compared correctly
                    if apr_last < 0:
                        apr_change = apr_change * -1
                else:
                    apr_change = 0
                apr_last = get_rolling_apr()
                cross_margin_ratio = cross_maintenance_margin_used/balance

                update_pnl( balance - virtual_balance )
                aprs.append( apr )
                aprs_timestamp.append( time.time() )

                voms.update_price( instrument_price )

                # EXIT
                if system_debug:
                    format_log( 'E', False, 'I', f"TIME REMAINING {abs(int(duration-max_duration))}" )
                exit_trade = False
                if get_exit():
                    format_log( 'E', False, 'I', "EXIT ON GLOBAL EXTERNAL EXIT SIGNAL")
                    time.sleep(random.random()*100)
                    quit()
                elif apr > ( float( apr_target ) * float( calculate_volatility_goal( get_volatility( symb[choose] ) ) ) ):
                    format_log( 'E', False, 'I', "EXIT APR TARGET")
                    exit_trade = True
                elif duration > max_duration:
                    format_log( 'E', False, 'I', "EXIT TIME STOP")
                    exit_trade = True

                if exit_trade:
                    symbol_position = get_position()

                    if symbol_position: 
                        position_size = symbol_position[0]
                        side = 'sell'
                        if position_size < 0:
                            side = 'buy'
                            position_size = position_size * -1
                        format_log( 'E', False, 'I', f"BOOK EXIT ORDER {side}, {position_size}" )
                        # anything smaller is considered dust for different cleanup
                        if ( float( position_size ) * instrument_price ) > 10:
                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                            # entry style - gives APR time to settle
                            # exit style  - prevents entering new run too quickly
                            time.sleep( decision_sleep )
                    update_run( balance )
                    format_log( 'E', False, 'I', "EXIT TRADE SIGNAL" )
                    quit()

                # TRADE 
                # ensure minimal direction entropy 0 is perfect 1 is totally random
                machine_vision_entropy = get_close_entropy( ) 
                if machine_vision_entropy is not None:
                    if machine_vision_entropy < machine_vision_entropy_max:
                        # HANDLE NORMAL ORDER FLOW
                        try:
                            if system_swing:
                                if side == 'sell':
                                    side = 'buy'
                                else:
                                    side = 'sell'

                            approve_trade = False
                            if position_direction == 0:
                                approve_trade = True
                            elif apr_change > 0:
                                if position_direction > 0 and side =="buy":
                                    approve_trade = True
                                elif position_direction < 0 and side == "sell":
                                    approve_trade = True
                            else:
                                if position_direction > 0 and side =="sell":
                                    approve_trade = True
                                elif position_direction < 0 and side == "buy":
                                    approve_trade = True

                            if approve_trade:

                                if position_direction == 0 and duration > max_direction_reversal:
                                    # strategy has flattened itself, reset run with fresh APR calculations
                                    format_log( 'E', False, 'I', f"EXIT DUE TO REVERSAL {position_direction}, {duration}, {balance}" )
                                    symbol_position = get_position()
                                    if symbol_position:
                                        format_log( 'E', False, 'I', "POSITION EXISTS POST REVERSAL - FULLY EXIT")
                                        position_size = symbol_position[0] 
                                        side = 'sell'
                                        if position_size < 0:
                                            position_size = position_size * -1
                                            side = 'buy'
                                        format_log( 'E', False, 'I', f"BOOK EXIT ORDER {side}, {position_size}, {instrument_price}" )
                                        if ( float( position_size ) * instrument_price ) > 10:
                                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                                            # entry style - gives APR time to settle
                                            # exit style  - prevents entering new run too quickly
                                            time.sleep( decision_sleep )
                                    update_run( balance )
                                    quit()

                                if (cross_margin_ratio > max_cross_margin_ratio and (position_direction > 0 and side == "buy")) or (cross_margin_ratio > max_cross_margin_ratio and (position_direction < 0 and side == "sell")):
                                    #format_log( 'E', False, 'I', f'SKIP ENTRY: CANNOT ADD DUE TO MARGIN BUFFER {cross_margin_ratio}' )
                                    return True
                                else:
                                    if (
                                            ( apr >= 0 and position_direction <= 0 and side == 'sell' ) or
                                            ( apr <= 0 and position_direction < 0 and side == 'buy' ) or
                                            ( apr >= 0 and position_direction >= 0 and side == 'buy' ) or
                                            ( apr <= 0 and position_direction > 0 and side == 'sell' ) 
                                            ):
                                        if abs( position_direction ) <= max_pos_limit:
                                            # MANDITORY TIME BURN COOL DOWN FOR EVERY POST TRADE
                                            # entry style - gives APR time to settle
                                            # exit style  - prevents entering new run too quickly
                                            time.sleep( decision_sleep )
                                            format_log( 'E', False, 'I', f"BOOK NORMAL ORDER {side}, {amount}, {instrument_price} based on {position_direction} {apr_change}" )
                                            position_size_metric = 1
                                            if side == "sell":
                                                position_direction += -1
                                                position_size_metric = -1
                                            else:
                                                position_direction += 1
                                            update_run_position( position_direction )
                                            voms.add_trade( pos[choose]*position_size_metric )
                                        #else:
                                        #    format_log( 'E', False, 'I', f"SKIP ENTRY: MAX POS {max_pos_limit} REACHED {side}, {amount}, {instrument_price} based on {apr}" )
                                    #else:
                                    #    format_log( 'E', False, 'I', f"SKIP ENTRY: NEGATIVE APR {side}, {amount}, {instrument_price} based on {apr}" )

                            #else:
                            #    format_log( 'E', False, 'I', f"REJECT NORMAL ORDER {side}, {amount}, {instrument_price} based on {position_direction} {apr_change}" )

                        except Exception as e:
                            format_log( 'E', False, 'W', 'create_order() failed')
                            format_log( 'E', False, 'W', str(e))
                return True
            
            asyncio.run(manage_orders(side, instrument_price, symb[choose],pos[choose] ))

        except TimeoutError:
            format_log( 'E', False, 'C', 'ITERATION TIMEOUT ERROR')
        except Exception as e:
            format_log( 'E', False, 'C', f'ITERATION EXCEPTION {traceback.format_exc()}'+ str(e) )

if __name__ == "__main__":
    # --- 1. Initial Forking Loop ---
    parent_app = Celery(
        "showcase_client",
        broker="redis://192.168.2.157:6379/0",
        backend="redis://192.168.2.157:6379/0",
    )
    # Determine max height to know whether it's time to exit or not
    def get_height( ):
        result = parent_app.send_task( "worker.tasks.get_max_run_height" )
        task_result = result.get(timeout=decision_sleep)
        return task_result

    def exit_runs_by_height( next_height ):
        parent_app.send_task( "worker.tasks.set_exit_for_runs_by_height", args=[ next_height ] )

    current_height = get_height()
    print( f"Current height is {current_height}" )

    # The parent will run this loop to create children
    for i in range(num_children):
        time.sleep(0.1)
        pid = os.fork()

        if pid == 0:
            # --- CHILD PROCESS ---
            import multiprocessing.util
            multiprocessing.util._exit_function()
            sys.stdout.flush()
            sys.stderr.flush()

            child_task()
            os._exit(0) # IMPORTANT: Child exits so it doesn't continue the loop.

        else:
            # --- PARENT PROCESS ---
            # The parent gets the child's PID and continues the loop.
            #print(f"PARENT: I just created child {i+1} with PID {pid}.")
            child_pids.append(pid)


    # --- 2. The Management Loop ---
    print("\nPARENT: Initial pool launched. Entering management loop to maintain the pool. 🔄")
    while True:
        try:
            # os.wait() blocks here until one of the children exits.
            # It returns the PID of the finished child and its exit status.
            finished_pid, _ = os.wait()
            print(f"\nPARENT: Child with PID {finished_pid} has exited.")
            child_pids.remove(finished_pid)

            next_height = get_height()
            if next_height > current_height:
                print( f"TARGET REACHED, PARENT EXITS" )
                for pid in child_pids:
                    try:
                        os.kill(pid, 9) # Forcefully kill any remaining children
                    except:
                        print( f"Skipping pid {pid}" )
                exit_runs_by_height( current_height + 1 ) # needs to increment by one otherwise causes clash across hosts
                break
            
            # Respawn a new child to replace the one that finished.
            new_pid = os.fork()
            
            if new_pid == 0:
                # New child's code.
                child_task()
            else:
                # Parent's code.
                print(f"PARENT: Launched new child with PID {new_pid}.")
                child_pids.append(new_pid)

        except OSError as e:
            print(f"PARENT: Error forking new child: {e}. Will retry.")
            time.sleep(1)
        except ChildProcessError:
            print("PARENT: No children to wait for. Exiting management loop.")
            break
        except KeyboardInterrupt:
            # Handle Ctrl+C to gracefully shut down.
            print("\nPARENT: Shutting down. Terminating remaining children...")
            for pid in child_pids:
                os.kill(pid, 9) # Forcefully kill any remaining children
            break
