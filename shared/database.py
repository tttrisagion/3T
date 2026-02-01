import contextlib

import mysql.connector
import redis

from shared.config import config


def get_db_connection():
    """
    Creates a direct MySQL connection.
    No pooling - let MySQL handle connection management with its 16K max_connections.
    This bypasses the mysql.connector library's 32-connection pool limit.
    """
    return mysql.connector.connect(
        host=config.get("database.host"),
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )


# Create Redis connection pools (separate pools for different decode_responses settings)
_redis_pool = None
_redis_pool_decoded = None


def _get_redis_pool(decode_responses=False):
    """Get or create the Redis connection pool."""
    global _redis_pool, _redis_pool_decoded

    if decode_responses:
        if _redis_pool_decoded is None:
            _redis_pool_decoded = redis.ConnectionPool(
                host=config.get("redis.host"),
                port=config.get("redis.port"),
                db=config.get("redis.db"),
                decode_responses=True,
                max_connections=1000,  # 10x increase
            )
        return _redis_pool_decoded
    else:
        if _redis_pool is None:
            _redis_pool = redis.ConnectionPool(
                host=config.get("redis.host"),
                port=config.get("redis.port"),
                db=config.get("redis.db"),
                decode_responses=False,
                max_connections=1000,  # 10x increase
            )
        return _redis_pool


@contextlib.contextmanager
def get_redis_connection(decode_responses=True):
    """Gets a connection from the Redis pool."""
    pool = _get_redis_pool(decode_responses)
    r = redis.Redis(connection_pool=pool)
    try:
        yield r
    finally:
        # Connection returned to pool, not closed
        pass


def save_state_to_redis(run_id: int, state: dict, ttl: int = 86400):
    """Save run state to Redis with TTL (default 24 hours)."""
    import json

    with get_redis_connection(decode_responses=True) as r:
        r.setex(f"providence:state:{run_id}", ttl, json.dumps(state))


def load_state_from_redis(run_id: int) -> dict | None:
    """Load run state from Redis."""
    import json

    with get_redis_connection(decode_responses=True) as r:
        state_json = r.get(f"providence:state:{run_id}")
        if state_json:
            return json.loads(state_json)
    return None


def delete_state_from_redis(run_id: int):
    """Delete run state from Redis."""
    with get_redis_connection(decode_responses=False) as r:
        r.delete(f"providence:state:{run_id}")
        r.delete(f"providence:exit:{run_id}")


def set_exit_signal_redis(run_id: int):
    """Set exit signal for a run in Redis."""
    with get_redis_connection(decode_responses=False) as r:
        r.setex(f"providence:exit:{run_id}", 86400, "1")  # 24 hour TTL


def check_exit_signal_redis(run_id: int) -> bool:
    """Check if exit signal is set for a run in Redis."""
    with get_redis_connection(decode_responses=False) as r:
        return r.exists(f"providence:exit:{run_id}") > 0


def mark_run_completed_redis(run_id: int):
    """Mark a run as completed in Redis."""
    with get_redis_connection(decode_responses=False) as r:
        r.setex(f"providence:completed:{run_id}", 86400, "1")  # 24 hour TTL


def is_run_completed_redis(run_id: int) -> bool:
    """Check if a run is marked as completed in Redis."""
    with get_redis_connection(decode_responses=False) as r:
        return r.exists(f"providence:completed:{run_id}") > 0
