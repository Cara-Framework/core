"""``cara.support.Auth`` — request user resolution and the required-auth 401.

Two flavours share one resolver, and the whole point of the split is that the
optional ones NEVER raise while the required ones ALWAYS do when the user is
missing. These tests pin both halves of that contract, plus the duck-typing
that lets the helpers run against stand-in request objects.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.exceptions.types.HttpException import HttpException
from cara.support.Auth import (
    authenticated_user,
    gate_allows,
    optional_user_id,
    resolve_user,
    user_id,
)


class _User:
    def __init__(self, uid: int | None = 7) -> None:
        self.id = uid


class _Request:
    """A request whose ``user`` is a METHOD, like ``cara.http.Request``."""

    def __init__(self, user: object | None) -> None:
        self._user = user

    def user(self) -> object | None:
        return self._user


# ── resolve_user ────────────────────────────────────────────────────


def test_resolve_user_calls_the_method_rather_than_reading_the_attribute():
    """``request.user`` is a bound method — truthy and callable but never the
    user. Reading it instead of calling it is the classic bug this helper
    exists to make impossible."""
    user = _User()
    assert resolve_user(_Request(user)) is user


def test_resolve_user_falls_back_to_the_private_attribute():
    """Objects that store ``_user`` without exposing a ``user()`` method (test
    doubles, partially built requests) still resolve."""
    assert resolve_user(SimpleNamespace(_user="stored")) == "stored"


def test_resolve_user_returns_none_when_there_is_no_user_at_all():
    assert resolve_user(SimpleNamespace()) is None
    assert resolve_user(_Request(None)) is None


def test_resolve_user_swallows_a_raising_user_method():
    """A failing ``user()`` must not propagate: resolution is used on public
    paths where "no user" is a legitimate answer."""

    class Boom:
        def user(self):
            raise RuntimeError("session backend down")

    assert resolve_user(Boom()) is None


# ── optional_user_id ────────────────────────────────────────────────


def test_optional_user_id_returns_the_id_when_signed_in():
    assert optional_user_id(_Request(_User(42))) == 42


def test_optional_user_id_returns_none_without_a_user():
    assert optional_user_id(_Request(None)) is None


def test_optional_user_id_returns_none_for_an_id_less_user():
    """A user-shaped object without ``id`` (an API-token principal, a fake)
    must not blow up a public route."""
    assert optional_user_id(_Request(SimpleNamespace())) is None


# ── authenticated_user / user_id ────────────────────────────────────


def test_authenticated_user_returns_the_user():
    user = _User(5)
    assert authenticated_user(_Request(user)) is user


@pytest.mark.parametrize(
    "user",
    [None, SimpleNamespace(), _User(None)],
    ids=["no-user", "id-less-user", "null-id-user"],
)
def test_authenticated_user_aborts_401_when_the_user_is_unusable(user):
    """Missing, id-less and null-id users all mean the same thing on an
    auth-protected route: the middleware did not run. Fail with 401 instead
    of continuing in an authenticated code path with no principal."""
    with pytest.raises(HttpException) as exc:
        authenticated_user(_Request(user))
    assert exc.value.status_code == 401


def test_user_id_returns_a_plain_int_so_call_sites_never_recheck():
    assert user_id(_Request(_User(11))) == 11


def test_user_id_aborts_401_rather_than_returning_none():
    """The whole reason ``user_id`` exists beside ``optional_user_id``: it
    never hands back ``None`` for a caller to forget to check."""
    with pytest.raises(HttpException) as exc:
        user_id(_Request(None))
    assert exc.value.status_code == 401


# ── gate_allows ─────────────────────────────────────────────────────


class _FakeGate:
    def __init__(self) -> None:
        self.seen_user: object | None = "unset"
        self.seen_ability: str | None = None
        self.answer = True

    def for_user(self, user):
        self.seen_user = user
        return self

    def allows(self, ability: str) -> bool:
        self.seen_ability = ability
        return self.answer


def test_gate_allows_asks_the_gate_for_the_resolved_request_user(monkeypatch):
    gate = _FakeGate()
    monkeypatch.setattr("cara.facades.Gate", gate)
    user = _User()

    assert gate_allows(_Request(user), "admin") is True
    assert gate.seen_user is user
    assert gate.seen_ability == "admin"


def test_gate_allows_passes_the_ability_through_unchanged(monkeypatch):
    """The ability name is a parameter, not framework vocabulary — cara must
    not know which abilities an application registers."""
    gate = _FakeGate()
    monkeypatch.setattr("cara.facades.Gate", gate)

    gate_allows(_Request(_User()), "manage-billing")
    assert gate.seen_ability == "manage-billing"


def test_gate_allows_is_false_for_an_unauthenticated_request(monkeypatch):
    """No user resolves to ``None``, the Gate is still consulted (root/before
    bypasses live there), and a falsy answer comes back as ``False`` — never
    an exception."""
    gate = _FakeGate()
    gate.answer = False
    monkeypatch.setattr("cara.facades.Gate", gate)

    assert gate_allows(_Request(None), "admin") is False
    assert gate.seen_user is None
