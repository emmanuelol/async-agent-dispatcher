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


def test_enqueue_module_rejects_kwargs(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)
    dispatcher.register_module("os.path")
    with pytest.raises(TypeError) as exc_info:
        dispatcher.enqueue("os.path", "arg1", unsupported_kwarg="val")
    assert "does not accept keyword arguments" in str(exc_info.value)


def test_job_id_sanitization_hash_suffix(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)
    dispatcher.register(dummy_task)

    # Contains colon which requires sanitization
    res = dispatcher.enqueue("dummy_task", "data_1", job_id="tenant:123#beta")
    assert res.enqueued is True
    # Verify job_id has hash suffix
    assert "_" in res.job_id
    assert len(res.job_id) > len("tenant_123_beta")


def test_register_lambda_warning(caplog):
    import logging
    dispatcher = AgentDispatcher()
    with caplog.at_level(logging.WARNING):
        dispatcher.register(lambda x: x + 1)
    assert any("unpickling" in record.message for record in caplog.records)

