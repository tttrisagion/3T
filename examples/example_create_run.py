#!/usr/bin/env python3
"""
Test script for the create_run Celery task.
Run this after starting the services with 'make install'.
"""

from celery import Celery

# Configure Celery client to connect to the same broker
app = Celery(
    "test_client", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0"
)


def test_create_run_task():
    """Test the create_run Celery task."""
    print("🧪 Testing create_run Celery task...")

    # Define the parameters for the new run
    run_params = {
        "start_balance": 10000.0,
        "max_duration": 3600,
        "symbol": "BTC/USDC:USDC",
        "ann_params": '{"layer_sizes": [10, 5], "activation": "relu"}',
        "controller_seed": 0.12345,
        "pid": 12345,
        "host": "example-host",
        "height": 100,
    }

    print(f"📊 Sending task with params: {run_params}")

    # Send the task
    result = app.send_task(
        "worker.tasks.create_run",
        kwargs=run_params,
    )

    print(f"📋 Task ID: {result.id}")
    print("⏳ Waiting for result...")

    # Wait for result with timeout
    try:
        task_result = result.get(timeout=60)  # 60 second timeout
        print("✅ Task completed successfully!")
        print(f"📈 New Run ID: {task_result}")
        return task_result
    except Exception as e:
        print(f"❌ Task failed: {e}")
        return None


if __name__ == "__main__":
    print("🚀 Starting Celery create_run tests...")
    print("⚠️  Make sure services are running with 'make install' first!\n")

    test_create_run_task()

    print("\n🏁 Test completed!")
