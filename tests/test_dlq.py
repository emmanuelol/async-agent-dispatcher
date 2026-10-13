import concurrent.futures
import json
import os
import time
import pytest
from async_agent_dispatcher.dlq import (
    safe_append_jsonl_dlq,
    drain_jsonl_dlq,
    RedisDLQ,
)

def test_safe_append_and_drain_jsonl(tmp_path):
    queue_file = str(tmp_path / "test_dlq.jsonl")

    # Append 3 entries
    assert safe_append_jsonl_dlq({"id": 1, "task": "a"}, queue_file)
    assert safe_append_jsonl_dlq({"id": 2, "task": "b"}, queue_file)
    assert safe_append_jsonl_dlq({"id": 3, "task": "c"}, queue_file)

    consumed = []

    def consumer(payload):
        consumed.append(payload["id"])
        # Succeed on 1 and 3, fail on 2
        return payload["id"] != 2

    success_cnt, retained_cnt = drain_jsonl_dlq(queue_file, consumer)
    assert success_cnt == 2
    assert retained_cnt == 1

    # Check file retains only item 2
    with open(queue_file, "r") as f:
        remaining = [json.loads(line) for line in f]
    assert len(remaining) == 1
    assert remaining[0]["id"] == 2


def test_concurrent_jsonl_append_during_drain(tmp_path):
    """
    Stress test verifying inode preservation: concurrent writers
    never lose writes when drain is in-flight.
    """
    queue_file = str(tmp_path / "concurrent_dlq.jsonl")
    total_appends = 50

    # Initial batch
    for i in range(10):
        safe_append_jsonl_dlq({"id": f"init_{i}"}, queue_file)

    consumed_ids = []

    def drain_worker():
        time.sleep(0.01)
        def consumer(payload):
            consumed_ids.append(payload["id"])
            return True
        drain_jsonl_dlq(queue_file, consumer)

    def append_worker(idx):
        safe_append_jsonl_dlq({"id": f"worker_{idx}"}, queue_file)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        # Submit draining
        futures.append(executor.submit(drain_worker))
        # Submit concurrent writers
        for i in range(total_appends):
            futures.append(executor.submit(append_worker, i))
        concurrent.futures.wait(futures)

    # Second drain to collect all remaining
    def final_consumer(payload):
        consumed_ids.append(payload["id"])
        return True

    drain_jsonl_dlq(queue_file, final_consumer)

    # All items (10 init + 50 workers) must be accounted for
    expected_count = 10 + total_appends
    assert len(consumed_ids) == expected_count, (
        f"Data loss detected! Expected {expected_count}, got {len(consumed_ids)}"
    )


def test_redis_dlq_ack_and_nack(redis_client):
    dlq = RedisDLQ(topic="test_topic", redis_client=redis_client)

    dlq.push({"id": "task_1", "data": "ok"})
    dlq.push({"id": "task_2", "data": "fail"})
    assert dlq.length() == 2

    def consumer(payload):
        return payload["data"] == "ok"

    success_cnt, failed_cnt = dlq.drain(consumer)
    assert success_cnt == 1
    assert failed_cnt == 1
    # task_2 was NACKed back to queue
    assert dlq.length() == 1
    assert dlq.processing_length() == 0


def test_redis_dlq_orphan_rescue(redis_client):
    dlq = RedisDLQ(topic="orphan_topic", redis_client=redis_client, orphan_timeout=1)

    # Simulate worker crash leaving item in processing queue
    item_payload = json.dumps({"id": "zombie_task"})
    redis_client.lpush(dlq.processing_key, item_payload)
    # Set timer 5 seconds in the past
    past_time = redis_client.time()[0] - 5
    redis_client.hset(dlq.timer_key, item_payload, past_time)

    assert dlq.length() == 0
    assert dlq.processing_length() == 1

    rescued = dlq.rescue_orphans(timeout_seconds=1)
    assert rescued == 1
    assert dlq.length() == 1
    assert dlq.processing_length() == 0
