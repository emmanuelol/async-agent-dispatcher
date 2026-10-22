import datetime
import logging
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from async_agent_dispatcher.dispatcher import AgentDispatcher, EnqueueResult

logger = logging.getLogger("async_agent_dispatcher.batch")


def sanitize_key_component(component: str) -> str:
    """Sanitize entity and window strings to be safe for Redis keys and RQ job IDs."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", str(component))


def format_batch_job_id(
    job_name: str,
    entity_id: str,
    window: Union[str, datetime.date, datetime.datetime],
) -> str:
    """
    Generate deterministic idempotency key for batch fan-out.
    Format: {job_name}_{entity_id}_{window}
    """
    clean_job = sanitize_key_component(job_name)
    clean_entity = sanitize_key_component(entity_id)
    if isinstance(window, datetime.datetime):
        clean_window = window.strftime("%Y-%m-%dT%H-%M-%S")
    elif isinstance(window, datetime.date):
        clean_window = window.strftime("%Y-%m-%d")
    else:
        clean_window = sanitize_key_component(window)

    return f"{clean_job}_{clean_entity}_{clean_window}"


class BatchDispatcher:
    """
    Batch Fan-Out Dispatcher.
    Replaces brittle in-process multi-tenant loops with isolated per-entity RQ jobs.
    Guarantees deterministic idempotency across retries and prevents head-of-line blocking.
    """
    def __init__(self, dispatcher: AgentDispatcher):
        self.dispatcher = dispatcher

    def dispatch_batch(
        self,
        target: str,
        entities: Sequence[str],
        window: Union[str, datetime.date, datetime.datetime],
        arg_builder: Optional[Callable[[str], Sequence[Any]]] = None,
        kwarg_builder: Optional[Callable[[str], Dict[str, Any]]] = None,
        timeout: Optional[int] = None,
        queue_name: Optional[str] = None,
        result_ttl: Optional[int] = None,
    ) -> Dict[str, EnqueueResult]:
        """
        Dispatches an isolated job for each entity in the batch.
        Failure or timeouts in Entity A never block Entity B.
        Returns a mapping of entity_id -> EnqueueResult.
        """
        results: Dict[str, EnqueueResult] = {}

        for entity in entities:
            job_id = format_batch_job_id(target, entity, window)
            args = list(arg_builder(entity)) if arg_builder else [entity]
            kwargs = dict(kwarg_builder(entity)) if kwarg_builder else {}

            try:
                result = self.dispatcher.enqueue(
                    target,
                    *args,
                    job_id=job_id,
                    timeout=timeout,
                    queue_name=queue_name,
                    result_ttl=result_ttl,
                    **kwargs,
                )
                results[entity] = result
            except Exception as e:
                logger.error(f"Failed to enqueue batch job for entity '{entity}': {e}")
                results[entity] = EnqueueResult(
                    job_id=job_id,
                    status="error",
                    enqueued=False,
                    message=f"Enqueue failed: {str(e)}",
                )

        return results
