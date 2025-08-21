import contextlib

import mysql.connector
import redis

from shared.config import config


def get_db_connection():
    """Creates and returns a new database connection."""
    return mysql.connector.connect(
        host=config.get("database.host"),
        user=config.get("database.user"),
        password=config.get_secret("database.password"),
        database=config.get("database.database"),
    )


@contextlib.contextmanager
def get_redis_connection():
    """Creates and returns a new Redis connection as a context manager."""
    r = redis.Redis(
        host=config.get("redis.host"),
        port=config.get("redis.port"),
        db=config.get("redis.db"),
        decode_responses=True,
    )
    try:
        yield r
    finally:
        r.close()
