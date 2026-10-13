import os
import pytest
from redis import Redis

@pytest.fixture
def redis_client():
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        client = Redis.from_url(redis_url, decode_responses=False)
        try:
            client.ping()
            client.flushdb()
            yield client
            client.flushdb()
            client.close()
            return
        except Exception:
            pass

    # Fallback to fakeredis for local testing without running redis
    try:
        import fakeredis
        client = fakeredis.FakeRedis()
        yield client
        client.flushdb()
    except ImportError:
        pytest.skip("Neither live Redis nor fakeredis available.")
