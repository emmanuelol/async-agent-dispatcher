import functools
import importlib.util
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Union
from redis import Redis
from rq import Queue, Retry
from rq.job import Job

from async_agent_dispatcher.locking import get_redis_connection, clear_redis_connection_cache
from async_agent_dispatcher.worker import run_job_module

logger = logging.getLogger("async_agent_dispatcher.dispatcher")

# Register fork handler for connection isolation across worker forks
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=clear_redis_connection_cache)


@dataclass
class EnqueueResult:
    """Represents the outcome of a job enqueue operation."""
    job_id: str
    status: str
    enqueued: bool
    message: str

    def __str__(self) -> str:
        return self.message


class AgentDispatcher:
    """
    Central SRE dispatcher for asynchronous agent and task execution.
    Provides strict whitelist registration, idempotent job deduplication,
    zombie failed-job purging, and configurable failure hooks.
    """
    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        redis_url: Optional[str] = None,
        default_queue_name: str = "default",
        default_timeout: int = 300,
        default_retry_max: int = 3,
        default_retry_intervals: Optional[List[int]] = None,
        on_failure: Optional[Callable[..., Any]] = None,
    ):
        self._redis_client = redis_client
        self._redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.default_queue_name = default_queue_name
        self.default_timeout = default_timeout
        self.default_retry_max = default_retry_max
        self.default_retry_intervals = default_retry_intervals or [60, 180, 600]
        self.on_failure = on_failure
        self._registry: Dict[str, Any] = {}

    def get_redis(self) -> Redis:
        """Obtain a valid Redis client connection."""
        if self._redis_client is not None:
            return self._redis_client
        return get_redis_connection(self._redis_url)

    def register(self, name_or_func: Union[str, Callable]) -> Any:
        """
        Register a task module, function, or agent workflow for authorized dispatch.
        Can be used as a decorator `@dispatcher.register("name")` or directly `dispatcher.register(target)`.
        """
        if isinstance(name_or_func, str):
            def decorator(fn: Callable) -> Callable:
                self._registry[name_or_func] = fn
                return fn
            return decorator
        elif callable(name_or_func):
            name = getattr(name_or_func, "__name__", str(name_or_func))
            self._registry[name] = name_or_func
            return name_or_func
        else:
            raise TypeError("Target must be a string identifier or a callable.")

    def register_module(self, module_path: str):
        """Explicitly register an importable Python module path."""
        self._registry[module_path] = module_path

    def is_registered(self, target: str) -> bool:
        """Check whether a target string identifier is in the authorized registry."""
        return target in self._registry

    @property
    def registered_targets(self) -> Set[str]:
        return set(self._registry.keys())

    def enqueue(
        self,
        target: str,
        *args: Any,
        job_id: Optional[str] = None,
        timeout: Optional[int] = None,
        queue_name: Optional[str] = None,
        retry_max: Optional[int] = None,
        retry_intervals: Optional[List[int]] = None,
        on_failure: Optional[Callable[..., Any]] = None,
        **kwargs: Any,
    ) -> EnqueueResult:
        """
        Enqueues an authorized task or module for asynchronous execution in RQ.
        Enforces idempotency deduplication if job_id is provided.
        """
        if target not in self._registry:
            raise ValueError(
                f"Target '{target}' is not authorized. Must be registered with dispatcher."
            )

        redis_conn = self.get_redis()
        q_name = queue_name or self.default_queue_name
        q = Queue(q_name, connection=redis_conn)

        # Sanitize job_id for RQ validation requirements (alphanumeric, underscores, dashes)
        clean_job_id = re.sub(r"[^a-zA-Z0-9_\-]", "_", str(job_id)) if job_id else None

        # Idempotency check and Zombie purge
        if clean_job_id:
            try:
                existing_job = Job.fetch(clean_job_id, connection=redis_conn)
                status = existing_job.get_status()

                if status in {"queued", "started", "deferred", "finished"}:
                    logger.info(f"Job {clean_job_id} already exists with status '{status}'. Deduplicated.")
                    return EnqueueResult(
                        job_id=clean_job_id,
                        status=status,
                        enqueued=False,
                        message=f"Job {clean_job_id} already exists ({status})",
                    )
                elif status == "failed":
                    logger.warning(
                        f"Job {clean_job_id} previously failed. Purging from registry for clean retry."
                    )
                    existing_job.delete()
            except Exception:
                # Job not found in Redis, proceed to enqueue
                pass

        max_retries = retry_max if retry_max is not None else self.default_retry_max
        intervals = retry_intervals if retry_intervals is not None else self.default_retry_intervals
        failure_cb = on_failure or self.on_failure

        enqueue_kwargs: Dict[str, Any] = {
            "job_timeout": timeout or self.default_timeout,
            "result_ttl": 86400,
            "failure_ttl": 604800,
        }

        if max_retries > 0:
            enqueue_kwargs["retry"] = Retry(max=max_retries, interval=intervals)
        if failure_cb:
            enqueue_kwargs["on_failure"] = failure_cb
        if clean_job_id:
            enqueue_kwargs["job_id"] = clean_job_id

        registered_target = self._registry[target]

        # Dispatch via run_job_module if string module path, else direct callable
        if isinstance(registered_target, str):
            job = q.enqueue(
                run_job_module,
                registered_target,
                *args,
                **enqueue_kwargs,
            )
        else:
            job = q.enqueue(
                registered_target,
                *args,
                **enqueue_kwargs,
                **kwargs,
            )

        logger.info(f"Enqueued '{target}' on queue '{q_name}' with job ID: {job.id}")
        return EnqueueResult(
            job_id=job.id,
            status="queued",
            enqueued=True,
            message=f"Job {job.id} enqueued for {target}",
        )
