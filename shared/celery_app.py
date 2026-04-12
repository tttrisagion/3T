# fmt: off
import shared.eventlet_patch  # isort: skip
# fmt: on
import logging
import os
import signal

from celery import Celery

logger = logging.getLogger(__name__)

app = Celery(
    "tasks",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0",
    include=[
        "worker.tasks",
        "worker.trading_range",
        "worker.providence",
        "worker.purge",
        "worker.feed",
        "worker.volatility",
        "worker.reconciliation_engine",
    ],
)

# Configure task routing with priorities
app.conf.update(
    task_ignore_result=True,
    result_expires=3600,
    worker_prefetch_multiplier=1,
    # Broker connection resilience - reconnect on Redis connection drops
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=None,  # Retry forever
    broker_connection_timeout=10,
    broker_transport_options={
        "socket_timeout": 15,
        "socket_connect_timeout": 5,
        "socket_keepalive": True,
        "retry_on_timeout": True,
        "health_check_interval": 30,
    },
    broker_heartbeat=120,
    task_routes={
        # High priority: Critical system tasks
        "worker.tasks.update_balance": {"queue": "high_priority"},
        "worker.tasks.schedule_market_data_fetching": {"queue": "high_priority"},
        "worker.trading_range.update_trading_range": {"queue": "high_priority"},
        "worker.reconciliation_engine.reconcile_positions": {"queue": "high_priority"},
        "worker.providence.providence_supervisor": {"queue": "high_priority"},
        "worker.purge.purge_stale_runs": {"queue": "high_priority"},
        "worker.feed.supervisor": {"queue": "high_priority"},
        "worker.feed.update_prices_in_redis": {"queue": "high_priority"},
        "worker.volatility.supervisor": {"queue": "high_priority"},
        # Low priority: Trading iterations (can wait if system busy)
        "worker.providence.providence_trading_iteration": {"queue": "low_priority"},
        "worker.providence.providence_iteration_scheduler": {"queue": "high_priority"},
    },
    task_default_queue="high_priority",  # Default to high priority for safety
)
