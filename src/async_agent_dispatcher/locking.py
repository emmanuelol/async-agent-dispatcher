import functools
import logging
import os
import uuid
from contextlib import contextmanager
from typing import Callable, Optional, Any
from redis import Redis

logger = logging.getLogger("async_agent_dispatcher.locking")

_RELEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class JobAlreadyRunning(Exception):
    """Raised when an execution lock cannot be acquired because the job is already active."""
    pass


_default_redis_pool: Optional[Redis] = None


def get_redis_connection(redis_url: Optional[str] = None) -> Redis:
    """Thread/Process-local or cached Redis connection factory with fork safety."""
    global _default_redis_pool
    if _default_redis_pool is None or redis_url is not None:
        target_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = Redis.from_url(
            target_url,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=3,
            health_check_interval=30,
            socket_keepalive=True,
        )
        if redis_url is None:
            _default_redis_pool = client
        return client
    return _default_redis_pool


def clear_redis_connection_cache():
    """Clear cached connection on fork or worker restart."""
    global _default_redis_pool
    if _default_redis_pool is not None:
        try:
            _default_redis_pool.close()
        except Exception:
            pass
        _default_redis_pool = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=clear_redis_connection_cache)


def job_lock(
    name: str,
    ttl: int = 900,
    redis_client: Optional[Redis] = None,
    key_prefix: str = "joblock",
):
    """
    Decorator enforcing distributed execution-level mutual exclusion via Redis.
    Uses atomic CAS Lua release to avoid deleting expired or reassigned locks.
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            r = redis_client or get_redis_connection()
            key = f"{key_prefix}:{name}"
            token = str(uuid.uuid4())

            acquired = r.set(key, token, nx=True, ex=ttl)
            if not acquired:
                logger.warning(f"[{name}] Lock '{key}' already held. Aborting duplicate execution.")
                raise JobAlreadyRunning(f"Job '{name}' is already executing under lock '{key}'.")

            try:
                return fn(*args, **kwargs)
            finally:
                try:
                    r.eval(_RELEASE, 1, key, token)
                except Exception as e:
                    logger.debug(f"Failed to release lock '{key}': {e}")

        return wrapper
    return decorator


def execution_lock(
    name: str,
    ttl: int = 900,
    redis_client: Optional[Redis] = None,
    key_prefix: str = "joblock",
):
    """
    Context manager enforcing distributed execution-level mutual exclusion via Redis.
    """
    @contextmanager
    def _lock():
        r = redis_client or get_redis_connection()
        key = f"{key_prefix}:{name}"
        token = str(uuid.uuid4())

        acquired = r.set(key, token, nx=True, ex=ttl)
        if not acquired:
            logger.warning(f"[{name}] Lock '{key}' already held. Aborting duplicate execution.")
            raise JobAlreadyRunning(f"Job '{name}' is already executing under lock '{key}'.")

        try:
            yield token
        finally:
            try:
                r.eval(_RELEASE, 1, key, token)
            except Exception as e:
                logger.debug(f"Failed to release lock '{key}': {e}")

    return _lock()
