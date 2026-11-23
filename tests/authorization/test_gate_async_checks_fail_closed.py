"""An ``async def`` check is not a decision, so the gate denies it.

The gate is synchronous: it calls an ability, a policy method or a hook and
reads the return value. An ``async def`` one returns a coroutine object, and a
coroutine is truthy — ``bool(result)`` therefore read every such check as
ALLOWED without running a line of it. A deny-first ``before`` hook written
async admitted everyone; a policy method that awaited a lookup allowed every
caller. The module promises "fails closed", so an awaitable now denies.
"""

from __future__ import annotations

import warnings

import pytest

from cara.authorization import Gate, Policy
from cara.exceptions import AuthorizationFailedException


class _User:
    id = 7


class _Order:
    pass


class _AsyncOrderPolicy(Policy):
    async def update(self, user, model=None):
        return False  # would deny — if anything ever awaited it

    def view(self, user, model=None):
        return True


def _gate() -> Gate:
    return Gate().for_user(_User())


def test_async_ability_is_denied_not_allowed():
    async def owns_listing(user, *args):
        return False

    gate = _gate().define("listing.update", owns_listing)

    assert gate.allows("listing.update") is False
    assert gate.inspect("listing.update").message() == "Authorization check failed."
    with pytest.raises(AuthorizationFailedException):
        gate.authorize("listing.update")


def test_async_policy_method_is_denied_and_sync_sibling_still_decides():
    gate = _gate().policy(_Order, _AsyncOrderPolicy)

    assert gate.allows("update", _Order()) is False
    assert gate.allows("view", _Order()) is True


def test_async_deny_first_before_hook_does_not_open_the_gate():
    async def token_cap(user, ability, *args):
        return False

    gate = _gate().define("orders.read", lambda user, *args: True).before(token_cap)

    assert gate.allows("orders.read") is False


def test_async_after_hook_cannot_turn_a_denial_into_an_allow():
    async def audit(user, ability, result, *args):
        return None

    gate = _gate().define("orders.read", lambda user, *args: False).after(audit)

    assert gate.allows("orders.read") is False


def test_refused_coroutine_is_closed_without_a_never_awaited_warning():
    async def owns_listing(user, *args):
        return True

    gate = _gate().define("listing.update", owns_listing)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        assert gate.denies("listing.update") is True


def test_synchronous_truthiness_is_unchanged():
    gate = (
        _gate()
        .define("yes", lambda user, *args: 1)
        .define("no", lambda user, *args: "")
        .define("none", lambda user, *args: None)
    )

    assert (gate.allows("yes"), gate.allows("no"), gate.allows("none")) == (
        True,
        False,
        False,
    )
