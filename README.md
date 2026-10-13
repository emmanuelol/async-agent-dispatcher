# async-agent-dispatcher

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

SRE-hardened asynchronous task and agent dispatcher library extracted from high-throughput production workloads. Provides RQ batch consolidation, atomic Redis distributed locking, fork-safe connection management, bounded stdout/stderr execution sandboxes, and two-tier dead-letter queues (Redis Lua state machine + POSIX flock fallback).

## Features

- **Distributed Execution Locking (`@job_lock`, `execution_lock`):** Non-reentrant mutual exclusion across distributed workers with atomic Lua compare-and-swap release and configurable TTL fencing.
- **Worker Execution Sandbox (`run_job_module`, `BoundedStringIO`):** OOM-capped stream buffer (5MB default) to protect memory-constrained containers, coupled with `sys.argv` isolation.
- **Fork-Safe Connection Pooling:** Automatic Redis connection cache eviction across worker forks (`os.register_at_fork`).
- **Declarative Agent & Task Dispatcher (`AgentDispatcher`):** Strict job registration, deterministic deduplication (`queued`, `started`, `finished`), and zombie failed job recovery.
- **Two-Tier Dead-Letter Queue (DLQ):**
  - *Tier 1 (Redis):* Atomic `RPOPLPUSH` with lease timers, Lua orphan rescue, ACK, and NACK retries.
  - *Tier 2 (POSIX JSONL Fallback):* Non-blocking `fcntl.flock(LOCK_EX | LOCK_NB)` append and in-place inode-preserving drain.
- **Deterministic Batch Fan-Out (`BatchDispatcher`):** Idempotent partition dispatching (`{job}:{entity}:{window}`) preventing multi-tenant head-of-line blocking.

## Installation

```bash
pip install async-agent-dispatcher
```

## Quick Start

```python
from async_agent_dispatcher import AgentDispatcher, job_lock

dispatcher = AgentDispatcher(redis_url="redis://localhost:6379/0")

@dispatcher.register("tasks.sample_task")
def sample_task(name: str):
    return f"Hello, {name}!"

# Enqueue with deduplication
job_id = dispatcher.enqueue("tasks.sample_task", "world", job_id="sample:world:2026-10-13")
```

## License

MIT License. See [LICENSE](LICENSE) for details.
