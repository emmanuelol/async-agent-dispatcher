"""
async-agent-dispatcher: SRE-hardened asynchronous task and agent dispatcher.
"""

from async_agent_dispatcher.locking import (
    job_lock,
    execution_lock,
    JobAlreadyRunning,
    get_redis_connection,
    clear_redis_connection_cache,
)
from async_agent_dispatcher.worker import (
    run_job_module,
    run_agent_graph,
    arun_agent_graph,
    BoundedStringIO,
    isolated_argv,
    JobExecutionError,
)
from async_agent_dispatcher.dispatcher import (
    AgentDispatcher,
    EnqueueResult,
)
from async_agent_dispatcher.dlq import (
    RedisDLQ,
    safe_append_jsonl_dlq,
    drain_jsonl_dlq,
)
from async_agent_dispatcher.batch import (
    BatchDispatcher,
    format_batch_job_id,
)

__version__ = "0.2.0"

__all__ = [
    "job_lock",
    "execution_lock",
    "JobAlreadyRunning",
    "get_redis_connection",
    "clear_redis_connection_cache",
    "run_job_module",
    "run_agent_graph",
    "arun_agent_graph",
    "BoundedStringIO",
    "isolated_argv",
    "JobExecutionError",
    "AgentDispatcher",
    "EnqueueResult",
    "RedisDLQ",
    "safe_append_jsonl_dlq",
    "drain_jsonl_dlq",
    "BatchDispatcher",
    "format_batch_job_id",
]
