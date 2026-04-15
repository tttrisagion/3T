import os
import sys

# Only patch if we are explicitly booting an eventlet pool
# We check both command line arguments and environment variables for robustness
is_eventlet = (
    "eventlet" in sys.argv
    or "--pool=eventlet" in sys.argv
    or os.environ.get("CELERY_WORKER_POOL") == "eventlet"
    or os.environ.get("CELERY_POOL") == "eventlet"
)

if is_eventlet:
    try:
        import eventlet

        eventlet.monkey_patch()
        # Direct print to stdout as logging may not be initialized yet
        print("✅ Eventlet monkey patch applied successfully")
    except ImportError:
        print("⚠️ WARNING: eventlet pool requested but eventlet package not found")
