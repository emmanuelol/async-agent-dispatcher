import functools
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Callable, Optional, Any, Dict
from redis import Redis

logger = logging.getLogger("async_agent_dispatcher.locking")

_RELEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

_RENEW = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""


class JobAlreadyRunning(Exception):
    """Raised when an execution lock cannot be acquired because the job is already active."""
    pass


_redis_pools: Dict[str, Redis] = {}
_pools_lock = threading.Lock()


def get_redis_connection(redis_url: Optional[str] = None) -> Redis:
    """Thread/Process-local or cached Redis connection factory indexed by URL with fork safety."""
    target_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    with _pools_lock:
        client = _redis_pools.get(target_url)
        if client is None:
            client = Redis.from_url(
                target_url,
                decode_responses=False,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=30,
                socket_keepalive=True,
            )
            _redis_pools[target_url] = client
        return client


def clear_redis_connection_cache():
    """Clear cached connections on fork or worker restart."""
    with _pools_lock:
        for client in list(_redis_pools.values()):
            try:
                client.close()
            except Exception:
                pass
        _redis_pools.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=clear_redis_connection_cache)


def job_lock(
    name: str,
    ttl: int = 900,
    redis_client: Optional[Redis] = None,
    key_prefix: str = "joblock",
    auto_renew: bool = False,
    renew_interval: Optional[float] = None,
):
    """
    Decorator enforcing distributed execution-level mutual exclusion via Redis.
    Uses atomic CAS Lua release to avoid deleting expired or reassigned locks.
    Delegates directly to execution_lock.
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with execution_lock(
                name=name,
                ttl=ttl,
                redis_client=redis_client,
                key_prefix=key_prefix,
                auto_renew=auto_renew,
                renew_interval=renew_interval,
            ):
                return fn(*args, **kwargs)

        return wrapper
    return decorator


def execution_lock(
    name: str,
    ttl: int = 900,
    redis_client: Optional[Redis] = None,
    key_prefix: str = "joblock",
    auto_renew: bool = False,
    renew_interval: Optional[float] = None,
):
    """
    Context manager enforcing distributed execution-level mutual exclusion via Redis.
    Supports optional auto-renewal watchdog thread.
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

        renew_stop_event = threading.Event()
        renew_thread: Optional[threading.Thread] = None

        if auto_renew:
            interval = renew_interval if renew_interval is not None else max(1.0, float(ttl) / 3.0)
            ttl_ms = int(ttl * 1000)

            def _renew_worker():
                while not renew_stop_event.wait(interval):
                    try:
                        res = r.eval(_RENEW, 1, key, token, ttl_ms)
                        if res == 0:
                            logger.warning(
                                f"[{name}] Lock '{key}' renewal failed (token mismatch or expired)."
                            )
                            break
                    except Exception as e:
                        logger.warning(f"[{name}] Lock '{key}' renewal encountered error: {e}")

            renew_thread = threading.Thread(target=_renew_worker, daemon=True)
            renew_thread.start()

        try:
            yield token
        finally:
            if renew_thread is not None:
                renew_stop_event.set()
                renew_thread.join(timeout=2.0)

            try:
                res = r.eval(_RELEASE, 1, key, token)
                if res == 0:
                    logger.warning(
                        f"[{name}] Lock '{key}' release failed (token mismatch or TTL expired). "
                        "Lock may have been acquired by another worker."
                    )
            except Exception as e:
                logger.debug(f"Failed to release lock '{key}': {e}")

    return _lock()
