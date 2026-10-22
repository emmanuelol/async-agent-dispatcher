from async_agent_dispatcher.batch import (
    BatchDispatcher,
    format_batch_job_id,
    sanitize_key_component,
)
from async_agent_dispatcher.dispatcher import AgentDispatcher

def test_sanitize_key_component():
    assert sanitize_key_component("org:tenant#1") == "org_tenant_1"
    assert sanitize_key_component("normal-key.1") == "normal-key_1"


def test_format_batch_job_id():
    job_id = format_batch_job_id("data_sync", "tenant_a", "2026-10-13")
    assert job_id == "data_sync_tenant_a_2026-10-13"


def test_format_batch_job_id_datetime_preserves_time():
    import datetime
    dt = datetime.datetime(2026, 10, 13, 14, 30, 0)
    d = datetime.date(2026, 10, 13)
    id_dt = format_batch_job_id("data_sync", "tenant_a", dt)
    id_d = format_batch_job_id("data_sync", "tenant_a", d)
    assert id_dt != id_d
    assert "14-30" in id_dt
    assert id_d == "data_sync_tenant_a_2026-10-13"



def test_batch_dispatch_isolation_and_idempotency(redis_client):
    dispatcher = AgentDispatcher(redis_client=redis_client)

    @dispatcher.register("tasks.batch_sync")
    def batch_sync(entity_id: str):
        return f"synced {entity_id}"

    batch = BatchDispatcher(dispatcher)
    entities = ["tenant_1", "tenant_2", "tenant_3"]

    results_run1 = batch.dispatch_batch(
        target="tasks.batch_sync",
        entities=entities,
        window="2026-10-13",
    )

    assert len(results_run1) == 3
    for entity in entities:
        assert results_run1[entity].enqueued is True
        assert results_run1[entity].status == "queued"

    # Second run with same window is idempotent
    results_run2 = batch.dispatch_batch(
        target="tasks.batch_sync",
        entities=entities,
        window="2026-10-13",
    )

    for entity in entities:
        assert results_run2[entity].enqueued is False
        assert "already exists" in results_run2[entity].message
