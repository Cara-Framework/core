"""Signed claims — round-trip, purpose separation, and every fail-closed path.

A signed token is handed to an unauthenticated party and trusted back, so
almost every test here is about REFUSING something. The two that matter most
are purpose confusion (a token minted for one flow must be worthless in
another) and non-canonical re-spelling (a token must not be reachable by a
second byte string carrying the same signature).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import time

import pytest

from cara.configuration import Configuration
from cara.security import SignedToken

# ``cara.security.SignedToken`` is the CLASS (the barrel binds it eagerly so
# the name is not shadowed by its own submodule). Reaching the MODULE — to
# patch the clock it reads — has to go through sys.modules.
_module = sys.modules["cara.security.SignedToken"]

_APP_KEY = "k" * 48


@pytest.fixture
def app_key(monkeypatch: pytest.MonkeyPatch):
    Configuration.empty()
    store = Configuration._instance._config
    monkeypatch.setitem(store, "app.key", _APP_KEY)
    return _APP_KEY


def _body_of(token: str) -> str:
    return token.split(".", 1)[0]


def _sign(body: str, purpose: str, key: str = _APP_KEY) -> str:
    derived = hmac.new(
        key.encode(), b"cara.signed-token.v1." + purpose.encode(), hashlib.sha256
    ).digest()
    return hmac.new(derived, body.encode(), hashlib.sha256).hexdigest()


def _encode(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


class TestRoundTrip:
    def test_claims_survive_the_round_trip(self, app_key):
        issued = SignedToken.issue({"c": "abc", "n": "xyz"}, purpose="demo", ttl=600)
        verified = SignedToken.verify(issued["token"], purpose="demo", max_ttl=600)
        assert verified == {
            "claims": {"c": "abc", "n": "xyz"},
            "expires_at": issued["expires_at"],
        }

    def test_the_issued_expiry_is_now_plus_ttl(self, app_key):
        before = int(time.time())
        issued = SignedToken.issue({}, purpose="demo", ttl=600)
        assert before + 600 <= issued["expires_at"] <= int(time.time()) + 600

    def test_nested_and_typed_claims_survive(self, app_key):
        claims = {"n": 7, "flag": True, "list": [1, "two"], "map": {"k": None}}
        issued = SignedToken.issue(claims, purpose="demo", ttl=60)
        verified = SignedToken.verify(issued["token"], purpose="demo", max_ttl=60)
        assert verified["claims"] == claims

    def test_the_token_is_url_safe(self, app_key):
        token = SignedToken.issue({"c": "a" * 200}, purpose="demo", ttl=60)["token"]
        assert set(token) <= set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
        )

    def test_claim_order_does_not_change_the_token(self, app_key):
        """Deterministic encoding — otherwise an equal claim set could
        produce two different tokens and confuse anything comparing them."""
        first = SignedToken.issue({"a": 1, "b": 2}, purpose="demo", ttl=60)["token"]
        second = SignedToken.issue({"b": 2, "a": 1}, purpose="demo", ttl=60)["token"]
        assert _body_of(first) == _body_of(second)


class TestPurposeSeparation:
    def test_a_token_minted_for_another_purpose_is_refused(self, app_key):
        token = SignedToken.issue({"c": "abc"}, purpose="oauth.state", ttl=600)["token"]
        assert SignedToken.verify(token, purpose="unsubscribe", max_ttl=600) is None

    def test_the_same_claims_sign_differently_per_purpose(self, app_key):
        a = SignedToken.issue({"c": "abc"}, purpose="one", ttl=600)["token"]
        b = SignedToken.issue({"c": "abc"}, purpose="two", ttl=600)["token"]
        assert _body_of(a) == _body_of(b)  # same claims, same instant
        assert a != b  # different signature

    @pytest.mark.parametrize("purpose", ["", "   ", None, 7])
    def test_an_empty_purpose_is_a_programming_error(self, purpose, app_key):
        with pytest.raises(ValueError):
            SignedToken.issue({}, purpose=purpose, ttl=60)


class TestTampering:
    def test_a_forged_body_under_a_captured_signature_is_refused(self, app_key):
        issued = SignedToken.issue({"c": "victim-a"}, purpose="demo", ttl=600)
        signature = issued["token"].rsplit(".", 1)[1]
        forged = _encode({"v": 1, "e": int(time.time()) + 600, "c": {"c": "victim-b"}})
        assert (
            SignedToken.verify(f"{forged}.{signature}", purpose="demo", max_ttl=600)
            is None
        )

    def test_a_forged_signature_is_refused(self, app_key):
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=600)["token"]
        body = _body_of(token)
        assert (
            SignedToken.verify(f"{body}.{'0' * 64}", purpose="demo", max_ttl=600) is None
        )

    def test_a_signature_of_the_wrong_width_is_refused(self, app_key):
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=600)["token"]
        body = _body_of(token)
        assert (
            SignedToken.verify(f"{body}.{'0' * 32}", purpose="demo", max_ttl=600) is None
        )

    def test_a_non_hex_signature_is_refused(self, app_key):
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=600)["token"]
        body = _body_of(token)
        assert (
            SignedToken.verify(f"{body}.{'z' * 64}", purpose="demo", max_ttl=600) is None
        )

    def test_a_token_signed_with_a_different_app_key_is_refused(
        self, app_key, monkeypatch
    ):
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=600)["token"]
        monkeypatch.setitem(Configuration._instance._config, "app.key", "j" * 48)
        assert SignedToken.verify(token, purpose="demo", max_ttl=600) is None

    def test_a_non_canonical_re_spelling_is_refused(self, app_key):
        """base64 has slack: trailing bits can be spelled more than one way.
        A decoder that ignores it lets one payload wear two token strings."""
        payload = {"v": 1, "e": int(time.time()) + 600, "c": {"c": "abc"}}
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        canonical = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
        # Flip the unused low bits of the final character.
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        last = alphabet[alphabet.index(canonical[-1]) ^ 1]
        respelled = canonical[:-1] + last
        assert respelled != canonical
        signature = _sign(respelled, "demo")
        assert (
            SignedToken.verify(f"{respelled}.{signature}", purpose="demo", max_ttl=600)
            is None
        )


class TestEnvelopeValidation:
    def _signed(self, payload: dict, purpose: str = "demo") -> str:
        body = _encode(payload)
        return f"{body}.{_sign(body, purpose)}"

    def test_a_wrong_version_is_refused(self, app_key):
        token = self._signed({"v": 2, "e": int(time.time()) + 60, "c": {}})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_an_extra_envelope_key_is_refused(self, app_key):
        token = self._signed({"v": 1, "e": int(time.time()) + 60, "c": {}, "extra": "x"})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_a_missing_envelope_key_is_refused(self, app_key):
        token = self._signed({"v": 1, "e": int(time.time()) + 60})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_a_boolean_expiry_is_refused(self, app_key):
        """``True`` is an int in Python; an expiry of 1 second past the epoch
        must not read as a valid timestamp."""
        token = self._signed({"v": 1, "e": True, "c": {}})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    @pytest.mark.parametrize("expiry", ["9999999999", 1.5, None, [1]])
    def test_a_non_integer_expiry_is_refused(self, expiry, app_key):
        token = self._signed({"v": 1, "e": expiry, "c": {}})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_non_dict_claims_are_refused(self, app_key):
        token = self._signed({"v": 1, "e": int(time.time()) + 60, "c": ["a"]})
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_a_json_body_that_is_not_an_object_is_refused(self, app_key):
        token = self._signed(["not", "an", "object"])  # type: ignore[arg-type]
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_a_body_that_is_not_json_is_refused(self, app_key):
        body = base64.urlsafe_b64encode(b"not json at all").rstrip(b"=").decode()
        assert (
            SignedToken.verify(
                f"{body}.{_sign(body, 'demo')}", purpose="demo", max_ttl=60
            )
            is None
        )


class TestExpiry:
    def test_an_expired_token_is_refused(self, app_key, monkeypatch):
        issued = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=600)
        real_time = time.time
        monkeypatch.setattr(_module.time, "time", lambda: real_time() + 601)
        assert SignedToken.verify(issued["token"], purpose="demo", max_ttl=600) is None

    def test_an_expiry_beyond_the_callers_maximum_age_is_refused(self, app_key):
        """A token minted under a longer TTL than the verifying flow allows
        must not be honoured just because its signature is genuine."""
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=86400)["token"]
        assert SignedToken.verify(token, purpose="demo", max_ttl=600) is None
        assert SignedToken.verify(token, purpose="demo", max_ttl=86400) is not None

    @pytest.mark.parametrize("ttl", [0, -1, -600])
    def test_a_non_positive_ttl_is_a_programming_error(self, ttl, app_key):
        with pytest.raises(ValueError):
            SignedToken.issue({}, purpose="demo", ttl=ttl)


class TestMalformedTokens:
    @pytest.mark.parametrize(
        "token",
        [None, "", "no-dot", "two.dots.here", ".", "body.", 42, b"bytes", ["list"]],
    )
    def test_malformed_input_returns_none_rather_than_raising(self, token, app_key):
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_an_oversized_token_is_refused_before_decoding(self, app_key):
        token = "a" * (SignedToken.MAX_TOKEN_LENGTH + 1) + "." + "0" * 64
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None

    def test_a_non_ascii_body_is_refused(self, app_key):
        assert SignedToken.verify(f"bödy.{'0' * 64}", purpose="demo", max_ttl=60) is None


class TestKeyStrength:
    @pytest.mark.parametrize("key", ["", "short", "x" * 31])
    def test_a_weak_app_key_refuses_to_sign(self, key, monkeypatch):
        Configuration.empty()
        monkeypatch.setitem(Configuration._instance._config, "app.key", key)
        with pytest.raises(RuntimeError, match="at least 32 bytes"):
            SignedToken.issue({"c": "abc"}, purpose="demo", ttl=60)

    def test_a_weak_app_key_refuses_to_verify(self, monkeypatch):
        Configuration.empty()
        monkeypatch.setitem(Configuration._instance._config, "app.key", "short")
        with pytest.raises(RuntimeError, match="at least 32 bytes"):
            SignedToken.verify(f"body.{'0' * 64}", purpose="demo", max_ttl=60)

    def test_the_key_is_read_at_call_time_not_import_time(self, monkeypatch):
        """Configuration loads during boot; a key captured at import would
        pin whatever (probably nothing) was set at that moment."""
        Configuration.empty()
        monkeypatch.setitem(Configuration._instance._config, "app.key", "a" * 48)
        token = SignedToken.issue({"c": "abc"}, purpose="demo", ttl=60)["token"]
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is not None
        monkeypatch.setitem(Configuration._instance._config, "app.key", "b" * 48)
        assert SignedToken.verify(token, purpose="demo", max_ttl=60) is None


class TestClaimShape:
    def test_non_mapping_claims_are_a_programming_error(self, app_key):
        with pytest.raises(TypeError):
            SignedToken.issue(["not", "a", "mapping"], purpose="demo", ttl=60)

    def test_non_string_claim_names_are_a_programming_error(self, app_key):
        with pytest.raises(TypeError):
            SignedToken.issue({1: "a"}, purpose="demo", ttl=60)

    def test_empty_claims_are_allowed(self, app_key):
        issued = SignedToken.issue({}, purpose="demo", ttl=60)
        verified = SignedToken.verify(issued["token"], purpose="demo", max_ttl=60)
        assert verified["claims"] == {}

    def test_the_returned_claims_are_a_copy_of_the_payload(self, app_key):
        claims = {"c": "abc"}
        issued = SignedToken.issue(claims, purpose="demo", ttl=60)
        claims["c"] = "mutated"
        verified = SignedToken.verify(issued["token"], purpose="demo", max_ttl=60)
        assert verified["claims"] == {"c": "abc"}
