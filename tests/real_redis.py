"""Connects to a real Redis server for the integration tests, if one is available.

real_redis is None when the redis package isn't installed or no server answers,
and the real-Redis test classes skip themselves in that case.
"""

import os


def connect_to_test_redis():
    """Return a connected client, or None if redis-py or the server is missing."""
    try:
        import redis
    except ImportError:
        return None
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        socket_connect_timeout=1,
    )
    try:
        client.ping()
    except redis.exceptions.RedisError:
        return None
    return client


real_redis = connect_to_test_redis()
