import time
import os
from shared.celery_app import app
from shared.opentelemetry_config import get_tracer

# Get a tracer
tracer = get_tracer(os.environ.get("OTEL_SERVICE_NAME", "celery-worker"))

# Instrument Celery
from opentelemetry.instrumentation.celery import CeleryInstrumentor
CeleryInstrumentor().instrument()

@app.task
def add(x, y):
    time.sleep(5)
    return x + y