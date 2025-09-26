#!/usr/bin/env python3
"""
Comprehensive showcase script for all new Celery tasks.

This script demonstrates:
- Run creation and management.
- Looping through all product symbols.
- The optimized, cached market weight calculation.

Run this after starting the services with 'make install'.
"""

import time

from celery import Celery

# Configure Celery client to connect to the same broker
app = Celery(
    "showcase_client",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)


def call_task(task_name, kwargs=None, args=None):
    """Helper function to call a Celery task and print its status."""
    print(f"\n--- 🧪 Calling task: {task_name} ---")
    if kwargs:
        print(f"📊 Kwargs: {kwargs}")
    if args:
        print(f"📊 Args: {args}")

    result = app.send_task(task_name, kwargs=kwargs, args=args)
    print(f"⏳ Waiting for result of task ID: {result.id}...")

    try:
        start_time = time.time()
        task_result = result.get(timeout=60)
        end_time = time.time()

        print(f"✅ Task completed in {end_time - start_time:.4f} seconds.")
        print(f"📈 Result: {task_result}")
        return task_result
    except Exception as e:
        print(f"❌ Task failed: {e}")
        return None


if __name__ == "__main__":
    print("🚀 Starting comprehensive Celery showcase...")
    print("⚠️  Make sure services are running with 'make install' first!\n")

    # 1. Create a new run to have some data to work with
    run_params = {
        "start_balance": 5000.0,
        "max_duration": 7200,
        "symbol": "BTC/USDC:USDC",  # Symbol for the test run
    }
    run_id = call_task("worker.tasks.create_run", kwargs=run_params)

    if run_id is None:
        print("\n❌ Cannot proceed without a run ID. Exiting.")
        exit()

    # 2. Test the run management tasks
    active_runs_before = call_task(
        "worker.tasks.get_active_run_count", args=[run_params["symbol"]]
    )
    if active_runs_before > 0:
        print(f"✅ Verified: Found {active_runs_before} active run(s).")

    # 3. Get all product symbols and loop through them
    all_symbols = call_task("worker.tasks.get_all_product_symbols")
    if not all_symbols:
        print("\n❌ Could not retrieve product symbols. Exiting run management tests.")
    else:
        print(
            f"\n✅ Found {len(all_symbols)} symbols. Now processing market weight for each."
        )

        for i, symbol in enumerate(all_symbols):
            print("\n==================================================")
            print(f"Processing symbol {i + 1}/{len(all_symbols)}: {symbol}")
            print("==================================================")

            # For the first symbol, demonstrate the caching
            if i == 0:
                print("\n--- ⏱️  Caching Showcase (for first symbol only) ---")
                print("First call (cache miss) will be slower.")
                call_task("worker.tasks.get_market_weight", args=[symbol])

                print("\nSecond call (cache hit) will be faster.")
                call_task("worker.tasks.get_market_weight", args=[symbol])
                print("--- End Caching Showcase ---")
            else:
                call_task("worker.tasks.get_market_weight", args=[symbol])

    # 4. Exit the run we created at the start
    end_run_result = call_task(
        "worker.tasks.end_run", args=[run_id, run_params["start_balance"]]
    )
    if end_run_result:
        print(f"✅ Verified: Run {run_id} was marked for exit.")

    # 5. Verify the run was exited
    print(f"\n--- Verifying exit for run {run_id} ---")
    exit_status = call_task("worker.tasks.get_exit_status", args=[run_id])
    if exit_status:
        print(
            f"✅ Verified: Run {run_id} has been successfully marked with exit_run=True."
        )
    else:
        # This check is important. If end_run sets end_time, the run is considered inactive,
        # so get_active_run_count should decrease.
        active_runs_after = call_task(
            "worker.tasks.get_active_run_count", args=[run_params["symbol"]]
        )
        if active_runs_after < active_runs_before:
            print(
                f"✅ Verified: Active run count decreased from {active_runs_before} to {active_runs_after}."
            )
        else:
            print(
                "❌ Verification failed: Run was not exited and active count did not decrease."
            )

    print("\n\n🏁 Showcase completed!")
