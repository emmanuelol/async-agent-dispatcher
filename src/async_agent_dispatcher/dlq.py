import fcntl
import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from redis import Redis

from async_agent_dispatcher.locking import get_redis_connection

logger = logging.getLogger("async_agent_dispatcher.dlq")

# --- Redis Lua State Machine Scripts ---

LUA_ACK_SUCCESS = """
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

LUA_CLAIM_ITEM = """
local item = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
if not item then
    return nil
end
local server_time = redis.call('TIME')
local now = tonumber(server_time[1])
redis.call('HSET', KEYS[3], item, now)
return item
"""

LUA_NACK_OR_POISON = """
local processing_key = KEYS[1]
local queue_key = KEYS[2]
local timer_key = KEYS[3]
local attempts_key = KEYS[4]
local malformed_key = KEYS[5]
local item = ARGV[1]
local max_attempts = tonumber(ARGV[2])

redis.call('LREM', processing_key, 1, item)
redis.call('HDEL', timer_key, item)

local attempts = redis.call('HINCRBY', attempts_key, item, 1)
if attempts >= max_attempts then
    redis.call('HDEL', attempts_key, item)
    redis.call('LPUSH', malformed_key, item)
    return 0
else
    redis.call('LPUSH', queue_key, item)
    return 1
end
"""

LUA_SWEEP_ORPHANS = """
local processing_key = KEYS[1]
local queue_key = KEYS[2]
local timer_key = KEYS[3]
local max_lease = tonumber(ARGV[1])

local now = nil
if ARGV[2] and tonumber(ARGV[2]) then
    now = tonumber(ARGV[2])
else
    local server_time = redis.call('TIME')
    now = tonumber(server_time[1])
end

local rescued = 0
local items = redis.call('LRANGE', processing_key, 0, -1)
for i, item in ipairs(items) do
    local claimed_at_str = redis.call('HGET', timer_key, item)
    local claimed_at = tonumber(claimed_at_str) or 0
    if (now - claimed_at) > max_lease then
        local removed = redis.call('LREM', processing_key, 1, item)
        if removed > 0 then
            redis.call('HDEL', timer_key, item)
            redis.call('LPUSH', queue_key, item)
            rescued = rescued + 1
        end
    end
end
return rescued
"""


# --- Tier 2: POSIX File-Backed DLQ with Inode Preservation ---

def safe_append_jsonl_dlq(
    payload: Any,
    file_path: str,
    max_retries: int = 5,
    backoff: float = 0.05,
    fsync: bool = False,
) -> bool:
    """
    Safely appends an undelivered task to a fallback JSONL DLQ file using
    non-blocking exclusive locking (LOCK_EX | LOCK_NB) with bounded retries.
    Prevents thread hangs under high concurrency or slow/NFS filesystems.
    """
    try:
        dirname = os.path.dirname(file_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        line = json.dumps(payload, ensure_ascii=False) + "\n"
        with open(file_path, "a", encoding="utf-8") as f:
            acquired = False
            for _ in range(max_retries):
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except (BlockingIOError, OSError):
                    time.sleep(backoff)

            if not acquired:
                logger.critical(
                    f"Timed out acquiring non-blocking flock on {file_path} after {max_retries} attempts."
                )
                return False

            try:
                f.write(line)
                f.flush()
                if fsync:
                    os.fsync(f.fileno())
                return True
            finally:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
    except Exception as e:
        logger.critical(f"Failed to persist to JSONL DLQ fallback {file_path}: {e}")
        return False


def drain_jsonl_dlq(
    file_path: str,
    consumer_fn: Callable[[Any], bool],
) -> Tuple[int, int]:
    """
    Drains a JSONL DLQ file using in-place truncation under exclusive lock.
    CRITICAL SRE GUARANTEE: Inode is preserved during the entire read-process-truncate
    cycle, preventing lost appends from concurrent threads blocked on flock.
    
    Returns (success_count, retained_count).
    """
    if not os.path.exists(file_path):
        return 0, 0

    with open(file_path, "a+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            lines = f.readlines()
            if not lines:
                return 0, 0

            retained_lines: List[str] = []
            success_count = 0

            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    entry = json.loads(stripped)
                except json.JSONDecodeError:
                    logger.error(f"Malformed JSONL line retained: {stripped}")
                    retained_lines.append(line)
                    continue

                try:
                    if consumer_fn(entry):
                        success_count += 1
                    else:
                        retained_lines.append(line)
                except Exception as e:
                    logger.error(f"Consumer raised exception on DLQ entry: {e}")
                    retained_lines.append(line)

            # Inode-preserving rewrite
            f.seek(0)
            f.truncate()
            if retained_lines:
                f.writelines(retained_lines)
            f.flush()

            return success_count, len(retained_lines)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


# --- Tier 1: Redis DLQ State Machine ---

class RedisDLQ:
    """
    Two-stage atomic Dead Letter Queue built on Redis.
    Uses atomic claim Lua scripts, processing lease timers, and retry-bounded poison isolation
    to guarantee zero lost tasks, crash resilience, and orphan rescue.
    """
    def __init__(
        self,
        topic: str = "tasks",
        redis_client: Optional[Redis] = None,
        redis_url: Optional[str] = None,
        orphan_timeout: int = 60,
        max_attempts: int = 3,
    ):
        self.topic = topic
        self._redis_client = redis_client
        self._redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.orphan_timeout = orphan_timeout
        self.max_attempts = max_attempts

        self.queue_key = f"dlq:{topic}"
        self.processing_key = f"dlq:{topic}:processing"
        self.timer_key = f"dlq:{topic}:processing:timers"
        self.attempts_key = f"dlq:{topic}:attempts"
        self.malformed_key = f"dlq:{topic}:malformed"

    def get_redis(self) -> Redis:
        if self._redis_client is not None:
            return self._redis_client
        return get_redis_connection(self._redis_url)

    def push(self, payload: Any, max_payload_bytes: Optional[int] = None) -> int:
        """Push an undeliverable payload into the DLQ with JSON validation and payload limits."""
        r = self.get_redis()
        if isinstance(payload, str):
            serialized = payload
        else:
            serialized = json.dumps(payload, ensure_ascii=False)

        payload_size = len(serialized.encode("utf-8"))
        limit = max_payload_bytes if max_payload_bytes is not None else 5_000_000
        if payload_size > limit:
            raise ValueError(f"Payload size ({payload_size} bytes) exceeds limit of {limit} bytes.")

        return r.lpush(self.queue_key, serialized)

    def rescue_orphans(self, timeout_seconds: Optional[int] = None) -> int:
        """
        Atomic orphan sweep using Redis server time to prevent multi-worker clock skew.
        Recovers zombie tasks left in the processing queue after worker crashes.
        """
        r = self.get_redis()
        lease = timeout_seconds or self.orphan_timeout
        server_now = int(r.time()[0])

        rescued = r.eval(
            LUA_SWEEP_ORPHANS,
            3,
            self.processing_key,
            self.queue_key,
            self.timer_key,
            lease,
            server_now,
        )
        if rescued > 0:
            logger.warning(f"Rescued {rescued} orphaned zombie tasks into '{self.queue_key}'.")
        return int(rescued)

    def drain(
        self,
        consumer_fn: Callable[[Any], bool],
        batch_size: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Drains up to batch_size items (or entire queue) from the DLQ.
        Atomically transfers each item into processing_key with a lease timestamp.
        Returns (success_count, failed_count).
        """
        r = self.get_redis()
        self.rescue_orphans()

        total_items = r.llen(self.queue_key)
        limit = min(total_items, batch_size) if batch_size is not None else total_items

        if limit == 0:
            return 0, 0

        success_count = 0
        failed_count = 0

        for _ in range(limit):
            # Atomic claim item and stamp timer in a single Lua operation
            raw_item = r.eval(
                LUA_CLAIM_ITEM,
                3,
                self.queue_key,
                self.processing_key,
                self.timer_key,
            )
            if not raw_item:
                break

            item_str = raw_item.decode("utf-8") if isinstance(raw_item, bytes) else str(raw_item)

            try:
                payload = json.loads(item_str)
            except json.JSONDecodeError as e:
                logger.error(f"Malformed non-JSON DLQ entry quarantined: {e}")
                r.lpush(self.malformed_key, item_str)
                r.eval(LUA_ACK_SUCCESS, 2, self.processing_key, self.timer_key, item_str)
                failed_count += 1
                continue

            try:
                success = consumer_fn(payload)
                if success:
                    success_count += 1
                    r.eval(LUA_ACK_SUCCESS, 2, self.processing_key, self.timer_key, item_str)
                    r.hdel(self.attempts_key, item_str)
                else:
                    failed_count += 1
                    r.eval(
                        LUA_NACK_ORPOISON if False else LUA_NACK_OR_POISON,
                        5,
                        self.processing_key,
                        self.queue_key,
                        self.timer_key,
                        self.attempts_key,
                        self.malformed_key,
                        item_str,
                        self.max_attempts,
                    )
            except Exception as e:
                logger.error(f"Unhandled consumer exception on DLQ entry: {e}")
                failed_count += 1
                r.eval(
                    LUA_NACK_OR_POISON,
                    5,
                    self.processing_key,
                    self.queue_key,
                    self.timer_key,
                    self.attempts_key,
                    self.malformed_key,
                    item_str,
                    self.max_attempts,
                )

        return success_count, failed_count

    def length(self) -> int:
        return self.get_redis().llen(self.queue_key)

    def processing_length(self) -> int:
        return self.get_redis().llen(self.processing_key)
