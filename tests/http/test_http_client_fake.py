"""Regression pins for ``Http.fake()`` — Laravel Http::fake parity.

The HTTP client had retry/backoff but no test seam: anything exercising
an outbound call either monkeypatched httpx internals or hit the real
network. ``Http.fake({...})`` stubs by URL glob, records every request,
raises on unstubbed (stray) requests, and skips retry back-off sleeps so
retry-ladder tests stay instant.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from cara.facades import Http
from cara.http.client import HttpFake
from cara.http.client.HttpFake import FakeExhaustedError, StrayHttpRequestError


@pytest.fixture(autouse=True)
def _restore_fake():
    yield
    HttpFake.deactivate()


def _run(coro):
    return asyncio.run(coro)


class TestFakeResponses:
    def test_dict_stub_returns_json_200(self):
        with Http.fake({"api.example.com/*": {"id": 1}}):
            response = _run(Http.get("https://api.example.com/users/1"))

        assert response.status_code == 200
        assert response.json() == {"id": 1}

    def test_int_stub_returns_bare_status(self):
        with Http.fake({"*": 404}):
            response = _run(Http.get("https://anything.example.com/x"))

        assert response.status_code == 404

    def test_sequence_stub_consumes_per_call(self):
        with Http.fake({"api.example.com/flaky": [503, {"ok": True}]}):
            first = _run(Http.get("https://api.example.com/flaky"))
            second = _run(Http.get("https://api.example.com/flaky"))

            assert first.status_code == 503
            assert second.status_code == 200
            assert second.json() == {"ok": True}

            with pytest.raises(FakeExhaustedError):
                _run(Http.get("https://api.example.com/flaky"))

    def test_full_response_stub(self):
        stub = Http.response(json={"error": "nope"}, status=422, headers={"X-Req": "1"})
        with Http.fake({"*": stub}):
            response = _run(Http.post("https://api.example.com/things", json={"a": 1}))

        assert response.status_code == 422
        assert response.headers["X-Req"] == "1"

    def test_stray_request_raises(self):
        with Http.fake({"api.example.com/*": 200}), pytest.raises(StrayHttpRequestError):
            _run(Http.get("https://other.example.com/leak"))

    def test_no_stub_map_fakes_everything_200(self):
        with Http.fake():
            response = _run(Http.get("https://api.example.com/whatever"))

        assert response.status_code == 200


class TestFakeRetryInterplay:
    def test_retryable_status_consumes_sequence_without_sleeping(self):
        # 2 retries: 503 → 503 → 200; faked retries skip back-off sleeps,
        # so this completes instantly.
        with Http.fake({"api.example.com/r": [503, 503, {"done": True}]}):
            response = _run(
                Http.retry(2, backoff=30.0).get("https://api.example.com/r")
            )

        assert response.status_code == 200
        assert response.json() == {"done": True}

    def test_raise_for_status_works_on_fake(self):
        with Http.fake({"*": 500}):
            response = _run(Http.get("https://api.example.com/x"))
            with pytest.raises(httpx.HTTPStatusError):
                response.raise_for_status()


class TestRecordingAndAssertions:
    def test_assert_sent_with_callable_and_glob(self):
        with Http.fake({"*": 200}):
            _run(Http.post("https://api.example.com/users", json={"name": "Ada"}))

            Http.assert_sent("api.example.com/users")
            Http.assert_sent(
                lambda request: request["method"] == "POST"
                and request["json"] == {"name": "Ada"}
            )

    def test_assert_sent_failure_lists_recorded(self):
        with Http.fake({"*": 200}):
            _run(Http.get("https://api.example.com/a"))

            with pytest.raises(AssertionError, match="No recorded HTTP request"):
                Http.assert_sent("api.example.com/never-called")

    def test_assert_nothing_sent(self):
        with Http.fake({"*": 200}):
            Http.assert_nothing_sent()

            _run(Http.get("https://api.example.com/a"))
            with pytest.raises(AssertionError, match="Expected no HTTP requests"):
                Http.assert_nothing_sent()

    def test_base_url_requests_are_recorded_with_full_url(self):
        with Http.fake({"api.example.com/*": 200}):
            client = Http.base_url("https://api.example.com").accept_json()
            _run(client.get("/users"))

            assert Http.recorded()[0]["url"] == "https://api.example.com/users"
