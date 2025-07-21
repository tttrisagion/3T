import time

import time

from shared.celery_app import app
from shared.opentelemetry_config import get_tracer

# Instrument Celery
from opentelemetry.instrumentation.celery import CeleryInstrumentor
CeleryInstrumentor().instrument()

# Get a tracer
tracer = get_tracer("celery-worker")

@app.task
def add(x, y):
    time.sleep(5)
    return x + y

