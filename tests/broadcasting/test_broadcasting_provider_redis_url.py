"""The Redis broadcast URL must carry the password percent-encoded.

redis-py parses the URL with ``urlparse`` and unquotes the credentials. The
provider interpolated ``REDIS_PASSWORD`` raw, so ``/``, ``#`` or ``?`` moved
the rest of the password into the host part (``Port could not be cast to
integer``) and ``%41`` authenticated as ``A``. The cache driver takes the
password as a keyword and stayed healthy, so nothing pointed at the broadcast
connection while every worker-to-dashboard event was dropped.
"""

from __future__ import annotations

import pytest
from redis.connection import parse_url

from cara.broadcasting import BroadcastingProvider
from cara.configuration import Configuration

PASSWORD = "a/b#c%41d?x=1@e:f"


class _FakeApp:
    def __init__(self) -> None:
        self.bound: dict[str, object] = {}

    def bind(self, key: str, value: object) -> None:
        self.bound[key] = value


@pytest.fixture()
def isolated_config():
    saved = Configuration._instance
    try:
        yield Configuration.empty()
    finally:
        Configuration._instance = saved


@pytest.fixture(autouse=True)
def isolated_channel_routes(monkeypatch):
    monkeypatch.setattr(
        BroadcastingProvider, "_load_channel_routes", staticmethod(lambda: None)
    )


@pytest.mark.parametrize("scheme", ["redis", "rediss"])
def test_password_round_trips_through_redis_url_parsing(isolated_config, scheme) -> None:
    isolated_config.set("broadcasting.default", "redis")
    isolated_config.set(
        "broadcasting.drivers.redis",
        {
            "driver": "redis",
            "connection": {
                "host": "redis.internal",
                "port": 6380,
                "password": PASSWORD,
                "db": 2,
                "scheme": scheme,
            },
        },
    )
    app = _FakeApp()

    BroadcastingProvider(app).register()

    redis_url = app.bound["broadcasting"].driver("redis")._redis_url
    assert redis_url.startswith(f"{scheme}://")
    parsed = parse_url(redis_url)
    assert parsed["password"] == PASSWORD
    assert parsed["host"] == "redis.internal"
    assert parsed["port"] == 6380
    assert parsed["db"] == 2
