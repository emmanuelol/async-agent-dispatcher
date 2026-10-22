import pytest
from async_agent_dispatcher.locking import job_lock, execution_lock, JobAlreadyRunning, _RELEASE

def test_job_lock_success(redis_client):
    executed = False

    @job_lock("test_task", ttl=10, redis_client=redis_client)
    def my_job():
        nonlocal executed
        executed = True
        return 42

    result = my_job()
    assert result == 42
    assert executed is True
    # Lock should be released after execution
    assert redis_client.get("joblock:test_task") is None


def test_job_lock_conflict(redis_client):
    redis_client.set("joblock:conflict_task", "other_token", ex=30)

    @job_lock("conflict_task", ttl=10, redis_client=redis_client)
    def my_job():
        return 1

    with pytest.raises(JobAlreadyRunning) as exc_info:
        my_job()
    assert "already executing" in str(exc_info.value)


def test_job_lock_releases_on_exception(redis_client):
    @job_lock("failing_task", ttl=10, redis_client=redis_client)
    def failing_job():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        failing_job()

    assert redis_client.get("joblock:failing_task") is None


def test_cas_release_does_not_delete_foreign_lock(redis_client):
    # Simulate our lock expired and was acquired by another worker
    key = "joblock:foreign_test"
    redis_client.set(key, "new_token")

    # Attempt to release using stale token
    res = redis_client.eval(_RELEASE, 1, key, "old_token")
    assert res == 0
    assert redis_client.get(key) == b"new_token" or redis_client.get(key) == "new_token"


def test_execution_lock_context_manager(redis_client):
    key = "joblock:ctx_task"
    assert redis_client.get(key) is None

    with execution_lock("ctx_task", ttl=10, redis_client=redis_client) as token:
        assert token is not None
        assert redis_client.get(key) is not None

        # Collision within context
        with pytest.raises(JobAlreadyRunning):
            with execution_lock("ctx_task", ttl=10, redis_client=redis_client):
                pass

    # Released after context exit
    assert redis_client.get(key) is None


def test_get_redis_connection_caches_by_url():
    from async_agent_dispatcher.locking import get_redis_connection, clear_redis_connection_cache
    clear_redis_connection_cache()
    url = "redis://127.0.0.1:6399/1"
    conn1 = get_redis_connection(url)
    conn2 = get_redis_connection(url)
    assert conn1 is conn2
    clear_redis_connection_cache()


def test_cas_release_warning_on_lock_loss(redis_client, caplog):
    import logging
    key = "joblock:loss_task"
    with caplog.at_level(logging.WARNING):
        with execution_lock("loss_task", ttl=1, redis_client=redis_client) as token:
            # Overwrite lock key simulating expiration and reassignment
            redis_client.set(key, "stolen_token")
    assert any("release failed" in record.message for record in caplog.records)


def test_auto_renew_watchdog(redis_client):
    import time
    key = "joblock:renew_task"
    with execution_lock("renew_task", ttl=2, redis_client=redis_client, auto_renew=True, renew_interval=0.5):
        # Sleep longer than original TTL to verify renewal kept it alive
        time.sleep(2.5)
        assert redis_client.get(key) is not None
        ttl_left = redis_client.ttl(key)
        assert ttl_left > 0
    assert redis_client.get(key) is None

