"""JWTGuard cross-request identity isolation.

The JWT guard is a process singleton, bound on the container under the
``"auth"`` key. If ``self._user`` were a plain instance attribute, then under
asyncio concurrency:

    1. Request A's middleware calls ``guard.user()`` -> caches Alice on the
       singleton.
    2. Request A awaits something (a thread-pool offload, a cache round trip).
    3. Request B arrives, hits the same singleton, reads the cached Alice and
       treats itself as authenticated as her — even when B's Authorization
       header names someone else, or is missing entirely.

The guard routes ``_user`` / ``_token`` / ``_last_payload`` through
module-level ``ContextVar``s. Each ``asyncio.Task`` (one per incoming request
or websocket connection) inherits a *copy* of its parent's context, and writes
inside that copy do not flow back.

These tests live in the framework because the leak is a framework property:
the guard, the descriptors and the ContextVars are all cara-owned, and
``ResetAuth`` (cara's own middleware) is what clears the slots between
requests. Pinning it in a product test suite left the framework free to
regress whenever a product reorganised its tests.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest


def _make_guard(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Construct a JWTGuard without touching a real user model or cache.

    The constructor resolves a dotted user-model path eagerly. Swapping the
    resolver keeps the test free of any application model — the isolation
    property under test never reaches persistence.
    """
    from cara.authentication.guards import JWTGuard

    class _StubUserClass:
        @classmethod
        def authenticate_jwt(cls, _id: Any, _claims: dict) -> None:
            return None

    monkeypatch.setattr(JWTGuard, "_load_user_class", lambda self, _model: _StubUserClass)
    # The constructor rejects signing keys under 32 bytes (suffix-truncation
    # and brute-force hardening), so the fixture secret must clear that bar.
    return JWTGuard(application=None, secret="not-a-real-secret-but-32-plus-bytes-long")


class TestJWTGuardRequestIsolation:
    """``_user`` / ``_token`` / ``_last_payload`` must not leak across tasks."""

    def test_concurrent_tasks_do_not_share_user_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two concurrently running tasks keep ``_user`` apart.

        Worst-case sequencing: task A sets the user and yields, task B reads
        before writing anything of its own. Without ContextVar scoping, B
        observes Alice and treats itself as authenticated as her.
        """
        guard = _make_guard(monkeypatch)

        async def _task_a() -> Any:
            guard._user = "alice"
            # Yield twice so B is guaranteed a turn while alice is set from
            # A's point of view — the exact interleaving the leak needs.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return guard._user

        async def _task_b() -> tuple[Any, Any]:
            await asyncio.sleep(0)
            seen_before_own_write = guard._user
            guard._user = "bob"
            await asyncio.sleep(0)
            return seen_before_own_write, guard._user

        async def _run() -> tuple[Any, tuple[Any, Any]]:
            return await asyncio.gather(_task_a(), _task_b())

        a_user, (b_observed, b_final) = asyncio.run(_run())

        assert a_user == "alice", "task A lost its own user under concurrency"
        assert b_observed is None, (
            "task B saw task A's cached user — cross-request identity leak"
        )
        assert b_final == "bob", "task B's own write was clobbered"

    def test_token_also_isolated_per_task(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``_token`` is the input to ``logout()``'s blacklist write.

        If it leaked across tasks, logging out request A would blacklist
        whatever token request B happened to be holding at that instant.
        """
        guard = _make_guard(monkeypatch)

        async def _task_a() -> Any:
            guard._token = "token-A"
            await asyncio.sleep(0)
            return guard._token

        async def _task_b() -> tuple[Any, Any]:
            await asyncio.sleep(0)
            seen = guard._token
            guard._token = "token-B"
            return seen, guard._token

        async def _run() -> tuple[Any, tuple[Any, Any]]:
            return await asyncio.gather(_task_a(), _task_b())

        a_token, (b_observed, b_final) = asyncio.run(_run())

        assert a_token == "token-A"
        assert b_observed is None, (
            "task B observed task A's token — would blacklist the wrong token"
        )
        assert b_final == "token-B"

    def test_verified_claims_are_isolated_per_task(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``_last_payload`` carries verified claims and leaks the same way.

        ``last_payload`` hands the claims of the most recently decoded token
        to callers deciding what a request may do. A shared slot would let one
        request authorise itself with another request's claims — the same bug
        class as ``_user``, one step further into the authorisation path.
        """
        guard = _make_guard(monkeypatch)

        async def _task_a() -> Any:
            guard._last_payload = {"sub": "alice", "scope": "admin"}
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return guard.last_payload

        async def _task_b() -> tuple[Any, Any]:
            await asyncio.sleep(0)
            seen = guard.last_payload
            guard._last_payload = {"sub": "bob", "scope": "read"}
            await asyncio.sleep(0)
            return seen, guard.last_payload

        async def _run() -> tuple[Any, tuple[Any, Any]]:
            return await asyncio.gather(_task_a(), _task_b())

        a_claims, (b_observed, b_final) = asyncio.run(_run())

        assert a_claims == {"sub": "alice", "scope": "admin"}
        assert b_observed == {}, (
            "task B read task A's verified claims — authorisation would use "
            "the wrong subject"
        )
        assert b_final == {"sub": "bob", "scope": "read"}

    def test_slots_round_trip_within_one_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Within one request the slots still behave like plain attributes.

        Pins that the descriptor swap did not break the in-request flow —
        ``ShouldAuthenticate`` assigns ``guard._user``, and logout, refresh and
        controller paths read it back. Assigning ``None`` (the cleanup
        primitive ``ResetAuth`` uses between requests) must clear the slot.
        """
        guard = _make_guard(monkeypatch)

        guard._user = "in-request"
        guard._token = "in-request-token"
        guard._last_payload = {"sub": "in-request"}

        assert guard._user == "in-request"
        assert guard._token == "in-request-token"
        assert guard.last_payload == {"sub": "in-request"}

        guard._user = None
        guard._token = None
        guard._last_payload = None
        assert guard._user is None
        assert guard._token is None
        assert guard.last_payload == {}
