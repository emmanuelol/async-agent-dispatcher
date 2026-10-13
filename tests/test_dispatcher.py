import pytest
from async_agent_dispatcher.dispatcher import AgentDispatcher, EnqueueResult

def dummy_task(arg: str):
    return f"processed {arg}"


def test_dispatcher_registration():
    dispatcher = AgentDispatcher()

    @dispatcher.register("tasks.sample")
    def sample():
        return 1

    assert dispatcher.is_registered("tasks.sample")
    assert not dispatcher.is_registered("unauthorized.task")


def test_enqueue_unregistered_raises(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)
    with pytest.raises(ValueError) as exc:
        dispatcher.enqueue("unregistered.task")
    assert "not authorized" in str(exc.value)


def test_enqueue_callable_success(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)
    dispatcher.register(dummy_task)

    res = dispatcher.enqueue("dummy_task", "data_1")
    assert res.enqueued is True
    assert res.status == "queued"
    assert res.job_id is not None


def test_enqueue_idempotency_deduplication(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)
    dispatcher.register(dummy_task)

    job_id = "test:fixed_id:123"
    res1 = dispatcher.enqueue("dummy_task", "data_1", job_id=job_id)
    assert res1.enqueued is True

    # Immediate re-enqueue with same job_id
    res2 = dispatcher.enqueue("dummy_task", "data_1", job_id=job_id)
    assert res2.enqueued is False
    assert "already exists" in res2.message
