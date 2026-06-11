# fmt: off
# fmt: on
import shared.eventlet_patch  # Must be first for greening
import ctypes
import json
import os
import time
from datetime import UTC, datetime

import ccxt
import numpy as np
from ccxt.base.errors import RateLimitExceeded
from celery.signals import beat_init
from eventlet.greenpool import GreenPool
from opentelemetry import context as opentelemetry_context
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from worker.reconciliation_engine import reconcile_positions
from worker.trading_range import update_trading_range

from shared.celery_app import app
from shared.config import config
from shared.database import get_db_connection, get_redis_connection, get_exchange_name_for_symbol
from shared.exchange_manager import exchange_manager
from shared.adapters import get_adapter, get_adapter_for_symbol
from shared.opentelemetry_config import get_tracer, setup_log_sampling, setup_telemetry

# --- OTel Setup ---
# It's crucial to set up the tracer provider BEFORE instrumenting.
# This ensures the instrumentor uses our custom sampler configuration.
provider = setup_telemetry(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))
CeleryInstrumentor().instrument(tracer_provider=provider)

# Get a tracer for manual instrumentation.
# The setup is already done, so this just retrieves the tracer.
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))

# --- Log Sampling Setup ---
# Apply sampling to Celery task logs to reduce noise from high-volume tasks
# Apply to both celery app logger and celery.worker.strategy (task received/succeeded messages)
setup_log_sampling(["celery", "celery.app.trace", "celery.worker.strategy"])


# Configure Celery Beat schedule
app.conf.beat_schedule = {
    "update-balance": {
        "task": "worker.tasks.update_balance",
        "schedule": config.get("celery.schedules.update_balance", 30.0),
    },
    "schedule-market-data-fetching": {
        "task": "worker.tasks.schedule_market_data_fetching",
        "schedule": config.get("celery.schedules.schedule_market_data_fetching", 300.0),
    },
    "update-trading-range": {
        "task": "worker.trading_range.update_trading_range",
        "schedule": config.get("celery.schedules.update_trading_range", 600.0),
    },
    "reconcile-positions": {
        "task": "worker.reconciliation_engine.reconcile_positions",
        "schedule": config.get("reconciliation_engine.rebalance_frequency", 900.0),
    },
    "providence-supervisor": {
        "task": "worker.providence.providence_supervisor",
        "schedule": config.get("celery.schedules.providence_supervisor", 300.0),
    },
    "providence-iteration-scheduler": {
        "task": "worker.providence.providence_iteration_scheduler",
        "schedule": config.get("celery.schedules.providence_iteration_scheduler", 5.0),
    },
    "purge-stale-runs": {
        "task": "worker.purge.purge_stale_runs",
        "schedule": config.get("celery.schedules.purge_stale_runs", 900.0),
    },
    "feed-supervisor": {
        "task": "worker.feed.supervisor",
        "schedule": config.get("celery.schedules.feed_supervisor", 15.0),
    },
    "volatility-supervisor": {
        "task": "worker.volatility.supervisor",
        "schedule": config.get("celery.schedules.volatility_supervisor", 15.0),
    },
}
app.conf.timezone = "UTC"


@beat_init.connect
def beat_startup(sender, **kwargs):
    """
    Triggered once when Celery Beat starts. Immediately fires all scheduled tasks
    so the system is fully operational without waiting for the first beat interval.

    This runs in the Beat process (single instance, no recycles), so it only fires
    on true system startup — not on worker memory recycles.
    """
    with tracer.start_as_current_span("beat_startup") as span:
        print("BEAT STARTUP: Firing all scheduled tasks immediately...")
        span.add_event("Firing all scheduled tasks on beat startup")

        try:
            # Skip the redundant, rate-limiting forced backfill on bootup (since DB is already populated)
            # This completely eliminates startup API floods and permanently protects your IP!
            schedule_market_data_fetching.delay(is_backfill=False)
            update_trading_range.delay()
            reconcile_positions.delay()

            from worker.providence import providence_supervisor

            providence_supervisor.delay()

            span.add_event("Successfully triggered startup tasks.")
            print(
                "Beat startup tasks dispatched: market data backfill, trading range update, reconciliation, and providence supervisor."
            )
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"ERROR: Failed to dispatch beat startup tasks: {e}")


@app.task(name="worker.tasks.schedule_market_data_fetching")
def schedule_market_data_fetching(is_backfill=False):
    """
    Stateful, resilient orchestrator for fetching market data.
    - Reads timeframes from config.
    - Uses Redis to track the last fetched bar for each symbol/timeframe.
    - Only fetches if a new bar should be available.
    - If is_backfill is True, it ignores Redis and fetches everything.
    """
    parent_context = opentelemetry_context.get_current()
    span_name = "schedule_market_data_fetching"
    if is_backfill:
        span_name += "_backfill"

    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("is_backfill", is_backfill)
        db_cnx = None
        failed_jobs = []
        try:
            db_cnx = get_db_connection()
            with get_redis_connection() as redis_cnx:
                cursor = db_cnx.cursor(dictionary=True)

                cursor.execute("SELECT symbol FROM products")
                products = cursor.fetchall()
                timeframes = config.get("market_data.timeframes", ["1m"])
                span.set_attribute("products.count", len(products))
                span.set_attribute("timeframes.configured", timeframes)

                lookback_config = config.get("market_data.lookback_bars", {})
                default_lookback = lookback_config.get("default", 20)

                jobs_to_run = []
                for product in products:
                    for timeframe in timeframes:
                        symbol = product["symbol"]
                        lookback = lookback_config.get(timeframe, default_lookback)

                        if is_backfill:
                            jobs_to_run.append((symbol, timeframe, lookback))
                            continue

                        # Stateful check
                        last_fetch_key = f"last_fetch:{symbol}:{timeframe}"
                        try:
                            last_fetched_ts = int(redis_cnx.get(last_fetch_key) or 0)
                        except (ValueError, TypeError):
                            last_fetched_ts = 0

                        # Calculate the timestamp of the most recent, completed bar
                        now_utc = datetime.now(UTC)
                        timeframe_seconds = ccxt.Exchange.parse_timeframe(timeframe)
                        latest_bar_ts = (
                            (int(now_utc.timestamp()) // timeframe_seconds)
                            * timeframe_seconds
                            * 1000
                        )

                        if latest_bar_ts > last_fetched_ts:
                            jobs_to_run.append((symbol, timeframe, lookback))

                if not jobs_to_run:
                    span.add_event("No new market data to fetch at this time.")
                    return

                span.add_event(f"Found {len(jobs_to_run)} new data points to fetch.")
                concurrency_limit = config.get(
                    "market_data.concurrency_limit", 2
                )  # Reduced from 10
                pool = GreenPool(size=concurrency_limit)

                threads = []
                for i, job in enumerate(jobs_to_run):
                    # Inject a small staggered delay (0.5s) to cooperatively pace requests to the exchange API,
                    # permanently preventing millisecond-level rate-limit spikes from the same IP address.
                    time.sleep(0.5)
                    gt = pool.spawn(
                        traced_fetch_and_store_ohlcv,
                        job[0],
                        job[1],
                        job[2],
                        parent_context,
                    )
                    threads.append((job, gt))

                for job, gt in threads:
                    try:
                        gt.wait()
                    except Exception as e:
                        failed_jobs.append((job, e))

                if failed_jobs:
                    raise Exception(
                        f"{len(failed_jobs)}/{len(jobs_to_run)} market data fetches failed."
                    )

                span.add_event(
                    f"Successfully completed {len(jobs_to_run) - len(failed_jobs)} jobs."
                )

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in {span_name}: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


def traced_fetch_and_store_ohlcv(
    symbol: str,
    timeframe: str,
    lookback: int,
    parent_context: opentelemetry_context.Context,
):
    """
    A wrapper that creates a dedicated OTEL span for each fetch, and upon success,
    updates the 'last_fetch' timestamp in Redis.
    """
    token = opentelemetry_context.attach(parent_context)
    try:
        with tracer.start_as_current_span(f"fetch_job.{symbol}.{timeframe}") as span:
            span.set_attribute("symbol", symbol)
            span.set_attribute("timeframe", timeframe)
            span.set_attribute("lookback", lookback)
            try:
                fetch_and_store_ohlcv(symbol, timeframe, lookback)

                # On success, update Redis with the timestamp of the latest bar
                with get_redis_connection() as redis_cnx:
                    timeframe_seconds = ccxt.Exchange.parse_timeframe(timeframe)
                    latest_bar_ts = (
                        (int(datetime.now(UTC).timestamp()) // timeframe_seconds)
                        * timeframe_seconds
                        * 1000
                    )
                    last_fetch_key = f"last_fetch:{symbol}:{timeframe}"
                    redis_cnx.setex(last_fetch_key, 86400, latest_bar_ts)

                span.set_attribute("otel.status_code", "OK")
                span.add_event("Fetch successful and Redis updated")

            except RateLimitExceeded:
                span.set_attribute("otel.status_code", "ERROR")
                print(
                    f"Rate limited: {symbol} ({timeframe}) — skipping, will retry next cycle"
                )
            except Exception as e:
                span.set_attribute("otel.status_code", "ERROR")
                span.record_exception(e)
                print(
                    f"ERROR in traced_fetch_and_store_ohlcv for {symbol} ({timeframe}): {e}"
                )
                raise
    finally:
        opentelemetry_context.detach(token)


def fetch_and_store_ohlcv(symbol: str, timeframe: str, lookback: int):
    """
    Connects to the exchange, fetches OHLCV data, and stores it in the database.
    Uses the resilient exchange adapters to fetch data.
    """
    db_cnx = None
    try:
        adapter = get_adapter_for_symbol(symbol)
        ohlcv_data = adapter.fetch_ohlcv(symbol, timeframe, lookback)

        if not ohlcv_data:
            return

        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()
        query = "REPLACE INTO market_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        rows_to_insert = [
            (
                int(r[0]),
                symbol,
                timeframe,
                round(float(r[1]), 8),
                round(float(r[2]), 8),
                round(float(r[3]), 8),
                round(float(r[4]), 8),
                round(float(r[5]), 8),
            )
            for r in ohlcv_data
        ]
        cursor.executemany(query, rows_to_insert)
        db_cnx.commit()

        # Publish the latest 1m price to Redis stream to feed Providence strategy runners
        if timeframe == "1m" and ohlcv_data:
            try:
                ohlcv_sorted = sorted(ohlcv_data, key=lambda x: x[0])
                latest_timestamp = ohlcv_sorted[-1][0]
                latest_price = ohlcv_sorted[-1][4]
                
                with get_redis_connection() as r:
                    r.set(f"price:{symbol}", float(latest_price), ex=120)
                    
                    stream_name = config.get("redis.streams.price_updates", "prices:updated")
                    r.xadd(stream_name, {
                        "symbol": symbol,
                        "price": str(latest_price),
                        "timestamp": str(latest_timestamp)
                    })
                    print(f"Published TradFi price update to Redis: {symbol} = ${latest_price} (timestamp: {latest_timestamp})")
            except Exception as redis_err:
                print(f"Error publishing TradFi price update to Redis: {redis_err}")

    except Exception:
        if db_cnx:
            db_cnx.rollback()
        raise
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.tasks.update_balance")
def update_balance():
    with tracer.start_as_current_span("update_balance_task") as span:
        span.set_attribute("task.name", "update_balance")
        try:
            account_value = fetch_and_store_balance()
            # After successful update, publish the new account value to Redis Streams
            if account_value is not None:
                publish_balance_update_event(account_value)
        except RateLimitExceeded:
            print("Rate limited: update_balance — skipping, will retry next cycle")
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in update_balance task: {e}")
            raise


def publish_balance_update_event(account_value: float):
    with tracer.start_as_current_span("publish_balance_update_event") as span:
        try:
            with get_redis_connection() as r:
                stream_name = config.get("redis.streams.balance_updates")
                event_data = {"account_value": account_value}
                r.xadd(stream_name, event_data, maxlen=10000)
                span.set_attribute("redis.stream.name", stream_name)
                span.set_attribute("redis.event.account_value", account_value)
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error publishing to Redis stream: {e}")
            raise


def fetch_and_store_balance() -> float | None:
    with tracer.start_as_current_span("fetch_and_store_balance") as span:
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor(dictionary=True)
            cursor.execute("START TRANSACTION")

            cursor.execute("SELECT id, name FROM exchanges")
            exchanges = cursor.fetchall()
            
            total_account_value = 0.0
            total_margin_used = 0.0
            success_count = 0
            
            balances_to_insert = []

            for exc in exchanges:
                exc_id = exc["id"]
                exc_name = exc["name"].lower()

                try:
                    adapter = get_adapter(exc_name)
                    val, margin = adapter.fetch_and_store_balance(cursor, exc_id)
                    balances_to_insert.append((exc_id, val, margin))
                    total_account_value += val
                    total_margin_used += margin
                    success_count += 1
                except Exception as exc_err:
                    import traceback
                    print(f"Error fetching balance for exchange '{exc['name']}': {exc_err}\n{traceback.format_exc()}")

            if success_count == len(exchanges):
                # Store isolated balances for each exchange to support strict sizing isolation
                for exc_id, val, margin in balances_to_insert:
                    query = "INSERT INTO balance_history (exchange_id, account_value, cross_maintenance_margin_used) VALUES (%s, %s, %s)"
                    cursor.execute(query, (exc_id, val, margin))
                print(f"Isolated balances synced successfully. Succeeded: {success_count}/{len(exchanges)}")
            else:
                print(f"Warning: Only {success_count}/{len(exchanges)} exchange balance fetches succeeded. Skipping database entry to prevent partial-sum chart corruption.")

            cursor.execute("COMMIT")

            return total_account_value if success_count == len(exchanges) else None

        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error during fetch_and_store_balance: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


# --- Permutation Entropy C++ Library Loading ---


def load_perm_entropy_library():
    """Load the permutation entropy C++ library."""
    try:
        lib_path = "/usr/local/lib/libperm_entropy_cpu.so"
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


# Load the library on module import
calculate_cpu_entropy = load_perm_entropy_library()


@app.task(name="worker.tasks.calculate_permutation_entropy")
def calculate_permutation_entropy(data, order=3, delay=1, iterations=1000):
    """
    Celery task to calculate permutation entropy using the C++ library.

    Args:
        data: List of float values for entropy calculation
        order: Order parameter for permutation entropy (default: 3)
        delay: Delay parameter for permutation entropy (default: 1)
        iterations: Number of iterations to amplify work (default: 1000)

    Returns:
        dict: Result containing entropy value and metadata
    """
    with tracer.start_as_current_span("calculate_permutation_entropy_task") as span:
        span.set_attribute("task.name", "calculate_permutation_entropy")
        span.set_attribute("data.length", len(data))
        span.set_attribute("order", order)
        span.set_attribute("delay", delay)
        span.set_attribute("iterations", iterations)

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

            # Calculate entropy with iterations to amplify work
            result = 0
            for i in range(iterations):
                result = calculate_cpu_entropy(x_np, n, order, delay)
                if i % 100 == 0:  # Log progress every 100 iterations
                    span.add_event(f"Completed {i + 1}/{iterations} iterations")

            span.set_attribute("result", result)
            span.add_event(f"Successfully calculated permutation entropy: {result}")

            return {
                "result": result,
                "engine": "cpu_native",
                "order": order,
                "delay": delay,
                "iterations": iterations,
                "data_length": n,
            }

        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in calculate_permutation_entropy task: {e}")
            return {"error": str(e), "result": None}


def _create_run_impl(
    start_balance,
    max_duration,
    symbol=None,
    ann_params=None,
    controller_seed=None,
    pid=None,
    host=None,
):
    """
    Internal function to create a new run in the database.
    Can be called directly from other tasks without going through Celery.
    No tracing to avoid orphaned spans in long-running tasks.
    """
    db_cnx = None
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()
        query = """
            INSERT INTO runs (start_balance, max_duration, symbol, ann_params, controller_seed, pid, host)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                start_balance,
                max_duration,
                symbol,
                ann_params,
                controller_seed,
                pid,
                host,
            ),
        )
        run_id = cursor.lastrowid
        db_cnx.commit()
        return run_id
    except Exception:
        if db_cnx:
            db_cnx.rollback()
        raise
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.tasks.create_run")
def create_run(
    start_balance,
    max_duration,
    symbol=None,
    ann_params=None,
    controller_seed=None,
    pid=None,
    host=None,
):
    """
    Celery task wrapper for creating a new run in the database.

    Args:
        start_balance (float): The starting balance for the run.
        max_duration (int): The maximum duration of the run in seconds.
        symbol (str, optional): The trading symbol.
        ann_params (str, optional): The ANN parameters.
        controller_seed (float, optional): The controller seed.
        pid (int, optional): The process ID.
        host (str, optional): The host name.

    Returns:
        int: The ID of the newly created run.
    """
    return _create_run_impl(
        start_balance, max_duration, symbol, ann_params, controller_seed, pid, host
    )


@app.task(name="worker.tasks.end_run")
def end_run(run_id, balance):
    """
    Ends a run by setting its end balance and end time.

    Args:
        run_id (int): The ID of the run to update.
        balance (float): The ending balance for the run.
    """
    with tracer.start_as_current_span("end_run_task") as span:
        span.set_attribute("task.name", "end_run")
        span.set_attribute("run_id", run_id)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "UPDATE runs SET end_balance=%s, end_time=NOW() WHERE id=%s"
            cursor.execute(query, (balance, run_id))
            db_cnx.commit()
            span.set_attribute("end_balance", balance)
            return True
        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in end_run task: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


def _get_exit_status_impl(run_id):
    """
    Internal function to retrieve the exit status for a given run.
    Can be called directly from other tasks without going through Celery.
    No tracing to avoid orphaned spans in long-running tasks.
    """
    db_cnx = None
    try:
        db_cnx = get_db_connection()
        cursor = db_cnx.cursor()
        query = "SELECT exit_run FROM runs WHERE id = %s"
        cursor.execute(query, (run_id,))
        row = cursor.fetchone()
        should_exit = bool(row[0]) if row else False
        return should_exit
    except Exception as e:
        print(f"Error in get_exit_status: {e}")
        raise
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.tasks.get_exit_status")
def get_exit_status(run_id):
    """
    Celery task wrapper for retrieving the exit status for a given run.

    Args:
        run_id (int): The ID of the run to check.

    Returns:
        bool: The value of the exit_run flag.
    """
    return _get_exit_status_impl(run_id)


@app.task(name="worker.tasks.get_active_run_count")
def get_active_run_count(symbol):
    """
    Counts the number of active runs for a given symbol.

    Args:
        symbol (str): The trading symbol to count active runs for.

    Returns:
        int: The number of active runs.
    """
    with tracer.start_as_current_span("get_active_run_count_task") as span:
        span.set_attribute("task.name", "get_active_run_count")
        span.set_attribute("symbol", symbol)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "SELECT count(*) FROM runs WHERE exit_run=0 AND end_time IS NULL AND symbol=%s"
            cursor.execute(query, (symbol,))
            count = cursor.fetchone()[0]
            span.set_attribute("active_runs", count)
            return count
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in get_active_run_count task: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


def _calculate_weight_from_data(results):
    """Processes raw market data to calculate an average weight without pandas."""
    if not results:
        return 0

    close_prices = []
    for row in results:
        try:
            close_prices.append(float(row["close_price"]))
        except (ValueError, TypeError):
            continue

    if len(close_prices) < 2:
        return 0

    mean_price = sum(close_prices) / len(close_prices)
    threshold = (0.01 / 100) * mean_price

    weights = []
    for i in range(1, len(close_prices)):
        diff = close_prices[i] - close_prices[i - 1]
        if diff > threshold:
            weights.append(1)
        elif diff < -threshold:
            weights.append(-1)
        else:
            weights.append(0)

    sum_weights = sum(weights)
    if not weights:
        return 0

    avg_weights = sum_weights / len(weights)

    if sum_weights:
        return avg_weights
    else:
        return 0


def _get_market_weight_impl(symbol):
    """
    Internal function to fetch market data for a symbol, calculate a weight, and cache the result.
    Can be called directly from other tasks without going through Celery.
    No tracing to avoid orphaned spans in long-running tasks.
    """
    db_cnx = None

    try:
        with get_redis_connection() as redis_cnx:
            current_minute = int(time.time() / 60)
            cache_key = f"market_weight:{symbol}:{current_minute}"

            cached_data = redis_cnx.get(cache_key)
            if cached_data:
                return float(json.loads(cached_data))

            db_cnx = get_db_connection()
            cursor = db_cnx.cursor(dictionary=True)
            query = """
                SELECT from_unixtime(timestamp/1000) as 'timestamp', close as 'close_price'
                FROM market_data
                WHERE timeframe='1m' AND symbol=%s  AND timestamp >= (unix_timestamp() - 1800) * 1000
                ORDER BY timestamp
            """
            cursor.execute(query, (symbol,))
            raw_data = cursor.fetchall()

            # Process the raw data to get the final weight
            final_weight = _calculate_weight_from_data(raw_data)

            # Store the final float value in cache
            redis_cnx.set(cache_key, json.dumps(final_weight), ex=60)

            return final_weight

    except Exception as e:
        print(f"Error in get_market_weight: {e}")
        raise
    finally:
        if db_cnx and db_cnx.is_connected():
            cursor.close()
            db_cnx.close()


@app.task(name="worker.tasks.get_market_weight")
def get_market_weight(symbol):
    """
    Celery task wrapper for fetching market data and calculating weight.

    Args:
        symbol (str): The trading symbol.

    Returns:
        float: The calculated average weight.
    """
    return _get_market_weight_impl(symbol)


@app.task(name="worker.tasks.get_max_run_height")
def get_max_run_height():
    """
    Retrieves the maximum height from the runs table.

    Returns:
        int: The maximum height value.
    """
    with tracer.start_as_current_span("get_max_run_height_task") as span:
        span.set_attribute("task.name", "get_max_run_height")
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "SELECT max(height) FROM runs"
            cursor.execute(query)
            row = cursor.fetchone()
            max_height = row[0] if row and row[0] is not None else 0
            span.set_attribute("max_height", max_height)
            return max_height
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in get_max_run_height task: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


@app.task(name="worker.tasks.set_exit_for_runs_by_height")
def set_exit_for_runs_by_height(height):
    """
    Sets the exit_run flag for all runs at a specific height.

    Args:
        height (int): The height of the runs to exit.
    """
    with tracer.start_as_current_span("set_exit_for_runs_by_height_task") as span:
        span.set_attribute("task.name", "set_exit_for_runs_by_height")
        span.set_attribute("height", height)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "UPDATE runs SET exit_run = 1 WHERE height = %s"
            cursor.execute(query, (height,))
            db_cnx.commit()
            span.add_event(
                f"Exited runs for height {height}. Rows affected: {cursor.rowcount}"
            )
            return cursor.rowcount
        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in set_exit_for_runs_by_height task: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


@app.task(name="worker.tasks.get_all_product_symbols")
def get_all_product_symbols():
    """
    Retrieves a list of all symbols from the products table.

    Returns:
        list: A list of symbol strings.
    """
    with tracer.start_as_current_span("get_all_product_symbols_task") as span:
        span.set_attribute("task.name", "get_all_product_symbols")
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "SELECT symbol FROM products"
            cursor.execute(query)
            symbols = [item[0] for item in cursor.fetchall()]
            span.set_attribute("symbol.count", len(symbols))
            return symbols
        except Exception as e:
            span.set_attribute("error", True)
            span.record_exception(e)
            print(f"Error in get_all_product_symbols task: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()


@app.task(name="worker.tasks.save_run_state", ignore_result=True)
def save_run_state(run_id, state_json):
    """
    Saves the complete state of a trading run to the database.

    Args:
        run_id (int): The ID of the run to update.
        state_json (str): A JSON string representing the run's state.
    """
    with tracer.start_as_current_span("save_run_state_task") as span:
        span.set_attribute("run_id", run_id)
        db_cnx = None
        try:
            db_cnx = get_db_connection()
            cursor = db_cnx.cursor()
            query = "UPDATE runs SET run_state = %s WHERE id = %s"
            cursor.execute(query, (state_json, run_id))
            db_cnx.commit()
        except Exception as e:
            if db_cnx:
                db_cnx.rollback()
            span.record_exception(e)
            print(f"Error saving state for run_id {run_id}: {e}")
            raise
        finally:
            if db_cnx and db_cnx.is_connected():
                cursor.close()
                db_cnx.close()
