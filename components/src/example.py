import threading
import os
import time
from random import randint

from shared.opentelemetry_config import get_tracer
from worker.tasks import add

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "components"))

def dispatch_task():
    """Dispatches a Celery task in an infinite loop."""
    while True:
        with tracer.start_as_current_span("dispatch_task") as span:
            x = randint(1, 100)
            y = randint(1, 100)
            task = add.delay(x, y)
            span.set_attribute("task_id", task.id)
            print(f"Dispatched task {task.id} to add {x} + {y}")
            time.sleep(1) # Simulate some work

if __name__ == '__main__':
    num_threads = int(os.environ.get("NUM_THREADS", 20))
    threads = []

    print(f"Starting {num_threads} threads...")

    for i in range(num_threads):
        thread = threading.Thread(target=dispatch_task)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
