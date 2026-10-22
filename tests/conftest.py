import os
import pytest
from redis import Redis

@pytest.fixture
def redis_client():
    redis_url = os.getenv("REDIS_URL")
    require_redis = os.getenv("REQUIRE_REDIS", "0") in ("1", "true", "True")

    if redis_url:
        client = Redis.from_url(redis_url, decode_responses=False)
        try:
            client.ping()
            client.flushdb()
            yield client
            client.flushdb()
            client.close()
            return
        except Exception as e:
            pytest.fail(f"Configured REDIS_URL ({redis_url}) is unreachable: {e}")

    # Fallback to fakeredis for local testing without running redis
    try:
        import fakeredis
        client = fakeredis.FakeRedis()
        yield client
        client.flushdb()
    except ImportError as e:
        if require_redis:
            pytest.fail(f"REQUIRE_REDIS is set but neither live Redis nor fakeredis available: {e}")
        pytest.skip("Neither live Redis nor fakeredis available.")
