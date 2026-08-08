"""SignInHint — identity recall that outlives the browser's storage.

A hint is a label, not a credential: it selects which account a sign-in form
is about and nothing more. The tests below pin the two properties that make
that safe — the address is ENCRYPTED (never legible in a URL, a Referer, or
a proxy log) and every unusable hint degrades to ``None`` rather than an
exception, because it is read on a public endpoint from attacker-supplied
input.
"""

import builtins

import pendulum
import pytest

from cara.authentication import SignInHint
from cara.encryption.Crypt import Crypt


class _StubApp:
    def __init__(self, bindings: dict):
        self._bindings = bindings

    def make(self, key: str):
        return self._bindings[key]


@pytest.fixture()
def crypt(monkeypatch):
    """Bind a real Crypt behind the facade `SignInHint` resolves through."""
    instance = Crypt(keys={"k1": "a" * 32}, current_key_id="k1")
    app = _StubApp({"crypt": instance, "logger": None})
    monkeypatch.setattr(builtins, "app", lambda: app, raising=False)
    return instance


def test_round_trips_the_claims(crypt):
    token = SignInHint.mint({"e": "yengshiu@example.com", "n": "Yeng Shiu"})

    assert SignInHint.read(token) == {"e": "yengshiu@example.com", "n": "Yeng Shiu"}


def test_address_is_not_legible_in_the_token(crypt):
    """A signed hint would leave the address in browser history and every
    log that records URLs. Encryption is the reason this can ride in a URL
    at all."""
    token = SignInHint.mint({"e": "yengshiu@example.com"})

    assert "yengshiu" not in token
    assert "example.com" not in token


def test_empty_claims_mint_nothing(crypt):
    assert SignInHint.mint({}) is None
    assert SignInHint.mint({"e": None, "n": ""}) is None


def test_blank_claim_values_are_dropped(crypt):
    token = SignInHint.mint({"e": "a@example.com", "n": None, "w": ""})

    assert SignInHint.read(token) == {"e": "a@example.com"}


def test_expired_hint_reads_as_none(crypt, monkeypatch):
    token = SignInHint.mint({"e": "a@example.com"}, ttl_days=1)
    monkeypatch.setattr(
        pendulum, "now", lambda tz=None: pendulum.datetime(2030, 1, 1, tz="UTC")
    )

    assert SignInHint.read(token) is None


@pytest.mark.parametrize(
    "value",
    ["", "not-a-token", "u2~k1~garbage", None, 42, "u2~k1~" + "A" * 900],
)
def test_unusable_hints_degrade_to_none(crypt, value):
    """Read on a public endpoint from hostile input — every failure mode is
    the ordinary case, and none of them may raise."""
    assert SignInHint.read(value) is None


def test_hint_minted_under_a_retired_key_reads_as_none(monkeypatch):
    old = Crypt(keys={"gone": "g" * 32}, current_key_id="gone")
    monkeypatch.setattr(
        builtins, "app", lambda: _StubApp({"crypt": old}), raising=False
    )
    token = SignInHint.mint({"e": "a@example.com"})

    rotated = Crypt(keys={"k1": "a" * 32}, current_key_id="k1")
    monkeypatch.setattr(
        builtins, "app", lambda: _StubApp({"crypt": rotated}), raising=False
    )

    assert SignInHint.read(token) is None


def test_oversized_claims_are_discarded_rather_than_emitted(crypt):
    """A hint shares a query string with a return path and sometimes a
    one-time token; past the budget it stops being a URL mail clients
    forward intact."""
    assert SignInHint.mint({"e": "a@example.com", "n": "x" * 600}) is None
