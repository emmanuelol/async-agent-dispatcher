import pytest
from rq import Queue, SimpleWorker
from rq.job import Job
from async_agent_dispatcher.dispatcher import AgentDispatcher

def add_numbers(a: int, b: int = 0) -> int:
    return a + b

def test_e2e_worker_execution(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client, default_queue_name="e2e_queue")
    dispatcher.register(add_numbers)

    res = dispatcher.enqueue("add_numbers", 15, b=27, queue_name="e2e_queue")
    assert res.enqueued is True

    q = Queue("e2e_queue", connection=redis_client)
    worker = SimpleWorker([q], connection=redis_client)
    worker.work(burst=True)

    job = Job.fetch(res.job_id, connection=redis_client)
    assert job.is_finished
    result_val = job.return_value() if callable(job.return_value) else job.result
    assert result_val == 42
