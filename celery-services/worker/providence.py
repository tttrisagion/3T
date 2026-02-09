"""
Providence Trading Engine - Celery Worker Implementation
"""

import json
import os
import socket
import time

from celery.utils.log import get_task_logger
from worker.tasks import (
    _get_market_weight_impl,
    update_pnl,
    update_run_position,
)

from shared.celery_app import app
from shared.config import config
from shared.database import (
    check_exit_signal_redis,
    delete_state_from_redis,
    get_db_connection,
    is_run_completed_redis,
    load_state_from_redis,
    mark_run_completed_redis,
    save_state_to_redis,
)
from shared.opentelemetry_config import get_tracer
from shared.providence import (
    calculate_permutation_entropy,
    get_price_from_redis,
    get_volatility_from_redis,
)
from shared.providence.math_utils import calculate_volatility_goal
from shared.providence.trading_logic import generate_ann_params, should_approve_trade
from shared.voms import VOMS

logger = get_task_logger(__name__)
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))


def get_close_entropy(state):
    """Helper to calculate entropy from the current run state."""
    ann_params = state["ann_params"]
    weights = state["weights"]
    weights_timestamp = state["weights_timestamp"]

    x = []
    now = time.time()
    for i in range(len(weights)):
        calc_minutes = (now - weights_timestamp[i]) / 60
        if calc_minutes < ann_params["volatility_entropy_window_minutes"]:
            if weights[i] > 0:
                x.append(1)
            elif weights[i] < 0:
                x.append(-1)
            else:
                x.append(0)

    x.reverse()

    if len(x) > ann_params["volatility_entropy_window_samples"]:
        return calculate_permutation_entropy(x)
    return 1.0  # High entropy if not enough data


@app.task(bind=True)
def providence_trading_iteration(self, run_id):
    """
    Single iteration of a trading run. Loads state from Redis (fast), performs one trading cycle,
    saves state back to Redis. ZERO MySQL queries during normal operation - fully Redis-based.
    This is called repeatedly by Celery Beat (every 2 seconds per run).
    """
    get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))

    # Quick check: is this run already completed?
    if is_run_completed_redis(run_id):
        return {"status": "completed", "run_id": run_id}

    # Quick check: exit signal?
    if check_exit_signal_redis(run_id):
        logger.info(f"Run {run_id}: Exit signal received. Ending run.")
        mark_run_completed_redis(run_id)
        delete_state_from_redis(run_id)

        # Update MySQL asynchronously (don't block iteration)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            cursor.execute("UPDATE runs SET end_time = NOW() WHERE id = %s", (run_id,))
            db_cnx.commit()
        except Exception as e:
            logger.error(f"Run {run_id}: Failed to update MySQL on exit: {e}")
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()

        return {"status": "exited", "run_id": run_id}

    # Try loading state from Redis first (fast!)
    state = load_state_from_redis(run_id)

    if state is None:
        # State not in Redis - load from MySQL and cache it (only happens once per run)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor(dictionary=True)
            cursor.execute(
                "SELECT run_state, exit_run, end_time FROM runs WHERE id = %s",
                (run_id,),
            )
            run_data = cursor.fetchone()

            if not run_data or run_data["end_time"] is not None:
                # Run completed or doesn't exist
                mark_run_completed_redis(run_id)
                delete_state_from_redis(run_id)
                return {"status": "completed", "run_id": run_id}

            if run_data["exit_run"]:
                logger.info(f"Run {run_id}: Exit flag set in MySQL. Ending run.")
                mark_run_completed_redis(run_id)
                delete_state_from_redis(run_id)
                return {"status": "exited", "run_id": run_id}

            if not run_data["run_state"]:
                logger.warning(f"Run {run_id}: No state found. Skipping iteration.")
                return {"status": "no_state", "run_id": run_id}

            state = json.loads(run_data["run_state"])
            # Cache to Redis for next iteration
            save_state_to_redis(run_id, state)

        except Exception as e:
            logger.error(f"Run {run_id}: Failed to load state: {e}")
            return {"status": "error", "run_id": run_id, "error": str(e)}
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()

    # Perform one trading iteration (pure Redis operations)
    try:
        result = _perform_trading_iteration(run_id, state)
        return result
    except Exception as e:
        logger.error(f"Run {run_id}: Iteration failed: {e}", exc_info=True)
        return {"status": "error", "run_id": run_id, "error": str(e)}


def _perform_trading_iteration(run_id, state):
    """
    Internal function that performs one trading cycle.
    Returns status dict indicating whether run should continue.
    """
    ann_params = state["ann_params"]
    symb = config.get("reconciliation_engine.symbols")
    symb_leverage = [80] * 4 + [2] * 54
    choose = ann_params["choose"]

    # Reconstruct VOMS
    voms = VOMS(starting_balance=state["start_balance"], leverage=symb_leverage[choose])
    if state.get("voms_state"):
        voms.from_dict(state["voms_state"])

    # Calculate duration and increment iteration count
    duration = time.time() - state["start_time"]
    state["iteration_count"] = state.get("iteration_count", 0) + 1

    # --- Check Exit Conditions First ---
    if duration > ann_params["max_duration"]:
        logger.info(f"Run {run_id}: Max duration reached.")
        metrics = voms.get_metrics()
        balance = metrics["account_balance"] if metrics else state["start_balance"]
        state["voms_state"] = voms.to_dict()
        _end_run(run_id, balance, state)
        return {"status": "completed", "reason": "max_duration", "run_id": run_id}

    # --- Get Market Data ---
    instrument_price = get_price_from_redis(symb[choose])
    if instrument_price is None:
        return {"status": "continue", "run_id": run_id, "info": "price_unavailable"}

    volatility = get_volatility_from_redis(symb[choose])
    if volatility is None:
        return {
            "status": "continue",
            "run_id": run_id,
            "info": "volatility_unavailable",
        }

    # --- Update VOMS and Calculate Metrics ---
    voms.update_price(instrument_price)
    latest_weight = _get_market_weight_impl(symb[choose])
    state["weights"].append(latest_weight)
    state["weights_timestamp"].append(time.time())

    metrics = voms.get_metrics()
    balance = metrics["account_balance"] if metrics else state["start_balance"]
    apr = (
        (
            (balance - state["start_balance"])
            / (duration / 3600)
            * 24
            * 365.24
            / state["start_balance"]
        )
        if state["start_balance"] > 0 and duration > 0
        else 0
    )

    # Calculate apr_change
    if state["apr_last"] != 0:
        state["apr_change"] = (apr - state["apr_last"]) / state["apr_last"]
        if state["apr_last"] < 0:
            state["apr_change"] = state["apr_change"] * -1
    else:
        state["apr_change"] = 0

    state["aprs"].append(apr)
    state["aprs_timestamp"].append(time.time())

    # Calculate rolling APR
    recent_aprs = [
        a
        for i, a in enumerate(state["aprs"])
        if (time.time() - state["aprs_timestamp"][i]) / 60
        < ann_params["rolling_apr_minutes"]
    ]
    state["apr_last"] = sum(recent_aprs) / len(recent_aprs) if recent_aprs else 0

    # Update PnL in MySQL at configured frequency (throttled to reduce DB load)
    mysql_update_freq = config.get("providence.mysql_update_frequency", 50)
    if state["iteration_count"] % mysql_update_freq == 0:
        update_pnl.delay(run_id, balance - state["start_balance"])

    # --- Data Pruning ---
    now = time.time()
    weights_cutoff = now - (ann_params["volatility_entropy_window_minutes"] + 1) * 60
    if state["weights_timestamp"] and state["weights_timestamp"][0] < weights_cutoff:
        state["weights"] = [
            w
            for i, w in enumerate(state["weights"])
            if state["weights_timestamp"][i] >= weights_cutoff
        ]
        state["weights_timestamp"] = [
            ts for ts in state["weights_timestamp"] if ts >= weights_cutoff
        ]

    aprs_cutoff = now - (ann_params["rolling_apr_minutes"] + 1) * 60
    if state["aprs_timestamp"] and state["aprs_timestamp"][0] < aprs_cutoff:
        state["aprs"] = [
            a
            for i, a in enumerate(state["aprs"])
            if state["aprs_timestamp"][i] >= aprs_cutoff
        ]
        state["aprs_timestamp"] = [
            ts for ts in state["aprs_timestamp"] if ts >= aprs_cutoff
        ]

    # --- Check APR Target Exit ---
    volatility_goal = calculate_volatility_goal(
        volatility,
        instrument_price,
        ann_params["min_goal_weight"],
        ann_params["max_goal_weight"],
        ann_params["min_goal"],
        ann_params["max_goal"],
    )
    if apr > (ann_params["apr_target"] * volatility_goal):
        logger.info(f"Run {run_id}: APR target reached.")
        state["voms_state"] = voms.to_dict()
        _end_run(run_id, balance, state)
        return {"status": "completed", "reason": "apr_target", "run_id": run_id}

    # --- Trading Logic ---
    machine_vision_entropy = get_close_entropy(state)
    if machine_vision_entropy < ann_params["machine_vision_entropy_max"]:
        side = "sell" if latest_weight <= 0 else "buy"
        approve, new_side = should_approve_trade(
            state["position_direction"],
            state["apr_change"],
            side,
            ann_params["system_swing"],
        )

        if approve:
            # Check reversal exit
            if (
                state["position_direction"] == 0
                and duration > ann_params["max_direction_reversal"]
            ):
                logger.info(f"Run {run_id}: Reversal exit.")
                state["voms_state"] = voms.to_dict()
                _end_run(run_id, balance, state)
                return {"status": "completed", "reason": "reversal", "run_id": run_id}

            # Check margin ratio
            cross_margin_ratio = (
                (metrics["margin_used"] / balance) if metrics and balance > 0 else 0
            )
            if cross_margin_ratio > ann_params["max_cross_margin_ratio"]:
                if (state["position_direction"] > 0 and new_side == "buy") or (
                    state["position_direction"] < 0 and new_side == "sell"
                ):
                    # Skip this iteration
                    _save_state(run_id, state)
                    return {
                        "status": "continue",
                        "run_id": run_id,
                        "info": "margin_limit",
                    }

            # Check trade conditions
            trade_condition = (
                (apr >= 0 and state["position_direction"] <= 0 and new_side == "sell")
                or (apr <= 0 and state["position_direction"] < 0 and new_side == "buy")
                or (apr >= 0 and state["position_direction"] >= 0 and new_side == "buy")
                or (apr <= 0 and state["position_direction"] > 0 and new_side == "sell")
            )

            max_pos_limit = config.get("providence.max_position_direction", 1024)
            if trade_condition and abs(state["position_direction"]) < max_pos_limit:
                pos_size = (
                    state["start_balance"]
                    * ann_params["pos_weight"]
                    / instrument_price
                    * ann_params["pos_scaler"]
                    / ann_params["balance_divisor"]
                )
                trade_size = pos_size * (1 if new_side == "buy" else -1)
                voms.add_trade(trade_size)

                state["position_direction"] += 1 if new_side == "buy" else -1

                # Update position in MySQL at configured frequency (throttled to reduce DB load)
                if state["iteration_count"] % mysql_update_freq == 0:
                    update_run_position.delay(run_id, state["position_direction"])

                logger.info(
                    f"Run {run_id}: Trade - {new_side} {pos_size:.6f} @ {instrument_price:.2f}, pos_dir: {state['position_direction']}"
                )

    # --- Save State ---
    state["voms_state"] = voms.to_dict()
    _save_state(run_id, state)

    return {"status": "continue", "run_id": run_id}


def _end_run(run_id, balance, state=None):
    """
    Helper to end a run.
    Marks as completed in Redis immediately, then persists to MySQL.
    """
    # Mark as completed in Redis first (immediate effect)
    mark_run_completed_redis(run_id)
    delete_state_from_redis(run_id)

    # Persist to MySQL
    db_cnx = None
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()

        # Save final state to MySQL if provided
        if state:
            cursor.execute(
                "UPDATE runs SET end_balance = %s, end_time = NOW(), run_state = %s WHERE id = %s",
                (balance, json.dumps(state), run_id),
            )
        else:
            cursor.execute(
                "UPDATE runs SET end_balance = %s, end_time = NOW() WHERE id = %s",
                (balance, run_id),
            )
        db_cnx.commit()

    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


def _save_state(run_id, state):
    """
    Save state to Redis for fast access.
    State is only persisted to MySQL on run completion or periodically.
    """
    try:
        save_state_to_redis(run_id, state)

        # Optionally persist to MySQL every N iterations for durability
        # (in case Redis fails or server restarts)
        iteration_count = state.get("iteration_count", 0)
        mysql_update_freq = config.get("providence.mysql_update_frequency", 50)
        if iteration_count % mysql_update_freq == 0:  # Persist at configured frequency
            db_cnx = None
            try:
                # Calculate current PnL
                current_pnl = 0
                if state.get("voms_state") and state.get("voms_state", {}).get(
                    "trades"
                ):
                    from shared.voms import VOMS

                    ann_params = state.get("ann_params", {})
                    choose = ann_params.get("choose", 0)
                    symb_leverage = [80] * 4 + [2] * 54
                    voms = VOMS(
                        starting_balance=state["start_balance"],
                        leverage=symb_leverage[choose],
                    )
                    voms.from_dict(state["voms_state"])
                    metrics = voms.get_metrics()
                    if metrics:
                        current_pnl = (
                            metrics["account_balance"] - state["start_balance"]
                        )

                db_cnx = get_db_connection()
                cursor = db_cnx.cursor()
                cursor.execute(
                    """
                    UPDATE runs
                    SET run_state = %s, position_direction = %s, live_pnl = %s
                    WHERE id = %s
                """,
                    (
                        json.dumps(state),
                        state.get("position_direction", 0),
                        current_pnl,
                        run_id,
                    ),
                )
                db_cnx.commit()
                logger.debug(
                    f"Run {run_id}: Persisted state to MySQL at iteration {iteration_count} (pos: {state.get('position_direction', 0)}, pnl: {current_pnl:.2f})"
                )
            except Exception as e:
                logger.error(f"Run {run_id}: Failed to persist state to MySQL: {e}")
            finally:
                if db_cnx and db_cnx.is_connected():
                    cursor.close()
                    db_cnx.close()

    except Exception as e:
        logger.error(f"Run {run_id}: Failed to save state to Redis: {e}")


@app.task(bind=True)
def providence_iteration_scheduler(self):
    """
    Periodically spawns trading iterations for all active runs.
    This runs every 2 seconds and kicks off an iteration task for each active run.
    """
    db_cnx = None
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor(dictionary=True)

        # Get all active runs
        cursor.execute("""
            SELECT id FROM runs
            WHERE end_time IS NULL AND exit_run = 0
        """)
        active_runs = cursor.fetchall()

        # Spawn an iteration task for each active run
        for run in active_runs:
            providence_trading_iteration.delay(run["id"])

        logger.info(f"Iteration scheduler: Spawned {len(active_runs)} iteration tasks")

    except Exception as e:
        logger.error(f"Iteration scheduler failed: {e}", exc_info=True)
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(bind=True)
def providence_supervisor(self):
    """
    Ensures the desired number of trading runs exist in the database.
    The iteration scheduler handles executing them.
    """
    with tracer.start_as_current_span("providence_supervisor"):
        desired_run_count = config.get("providence.desired_run_count", 100)
        symb = config.get("reconciliation_engine.symbols", [])
        symb_leverage = [80] * 4 + [2] * 54
        virtual_balance = 7000
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor(dictionary=True)

            # Query DB for active runs (runs without end_time)
            cursor.execute(
                "SELECT id FROM runs WHERE end_time IS NULL AND exit_run = 0"
            )
            active_db_runs = cursor.fetchall()
            current_total_runs = len(active_db_runs)

            # Create new runs if needed
            needed_runs = desired_run_count - current_total_runs
            spawned_count = 0
            if needed_runs > 0:
                logger.info(f"Supervisor: Creating {needed_runs} new runs.")
                for _ in range(needed_runs):
                    ann_params = generate_ann_params(
                        symb, symb_leverage, virtual_balance
                    )
                    choose = ann_params["choose"]

                    # Create initial state
                    initial_state = {
                        "start_time": time.time(),
                        "start_balance": virtual_balance,
                        "ann_params": ann_params,
                        "weights": [],
                        "weights_timestamp": [],
                        "aprs": [],
                        "aprs_timestamp": [],
                        "position_direction": 0,
                        "apr_change": 0,
                        "apr_last": 0,
                        "voms_state": None,
                        "iteration_count": 0,
                    }

                    # Create run in database with initial state
                    # Use stable hostname (Docker container name) instead of volatile MAC address
                    stable_host = os.environ.get("HOSTNAME", socket.gethostname())

                    cursor.execute(
                        """
                        INSERT INTO runs (start_balance, max_duration, symbol, ann_params, run_state, start_time, host)
                        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    """,
                        (
                            virtual_balance,
                            ann_params["max_duration"],
                            symb[choose],
                            json.dumps(ann_params),
                            json.dumps(initial_state),
                            stable_host,
                        ),
                    )
                    db_cnx.commit()
                    spawned_count += 1

                    logger.info(
                        f"Supervisor: Created run {cursor.lastrowid} for {symb[choose]}"
                    )

            logger.info(
                f"Supervisor: Cycle complete. Desired: {desired_run_count}, Active: {current_total_runs}, Spawned: {spawned_count}"
            )

        except Exception as e:
            logger.error(f"Supervisor failed: {e}", exc_info=True)
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()
