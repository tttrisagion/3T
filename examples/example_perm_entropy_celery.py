#!/usr/bin/env python3
"""
Test script for the permutation entropy Celery task.
Run this after starting the services with 'make install'.
"""

import numpy as np
from celery import Celery

# Configure Celery client to connect to the same broker
app = Celery(
    "test_client", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0"
)


def test_permutation_entropy_task():
    """Test the permutation entropy Celery task."""
    print("🧪 Testing permutation entropy Celery task...")

    # Generate some test data
    np.random.seed(42)  # For reproducible results
    test_data = np.random.randn(100).tolist()

    print(f"📊 Sending task with {len(test_data)} data points...")

    # Send the task
    result = app.send_task(
        "worker.tasks.calculate_permutation_entropy",
        args=[test_data],
        kwargs={"order": 3, "delay": 1, "iterations": 1000},
    )

    print(f"📋 Task ID: {result.id}")
    print("⏳ Waiting for result...")

    # Wait for result with timeout
    try:
        task_result = result.get(timeout=60)  # 60 second timeout
        print("✅ Task completed successfully!")
        print(f"📈 Result: {task_result}")
        return task_result
    except Exception as e:
        print(f"❌ Task failed: {e}")
        return None


def test_multiple_tasks():
    """Test multiple concurrent tasks."""
    print("\n🔄 Testing multiple concurrent tasks...")

    tasks = []
    for i in range(5):
        # Generate different test data for each task
        np.random.seed(i)
        test_data = np.random.randn(50).tolist()

        result = app.send_task(
            "worker.tasks.calculate_permutation_entropy",
            args=[test_data],
            kwargs={
                "order": 3,
                "delay": 1,
                "iterations": 100,  # Fewer iterations for faster testing
            },
        )
        tasks.append((i, result))
        print(f"📤 Sent task {i + 1}/5: {result.id}")

    print("⏳ Waiting for all tasks to complete...")

    successful_tasks = 0
    for i, task in tasks:
        try:
            task_result = task.get(timeout=30)
            print(
                f"✅ Task {i + 1} completed: entropy = {task_result.get('result', 'N/A')}"
            )
            successful_tasks += 1
        except Exception as e:
            print(f"❌ Task {i + 1} failed: {e}")

    print(f"\n📊 Summary: {successful_tasks}/5 tasks completed successfully")


if __name__ == "__main__":
    print("🚀 Starting Celery permutation entropy tests...")
    print("⚠️  Make sure services are running with 'make install' first!\n")

    # Test single task
    result = test_permutation_entropy_task()

    if result and result.get("result") is not None:
        # Test multiple tasks if single task works
        test_multiple_tasks()
    else:
        print("❌ Single task test failed, skipping multiple task test")

    print("\n🏁 Test completed!")
