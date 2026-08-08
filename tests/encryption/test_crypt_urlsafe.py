"""The URL-safe Crypt envelope — ciphertext that survives a query string.

Same AES-256-GCM as ``encrypt``; the transport alphabet is the difference.
The properties that matter are that it never needs percent-encoding, and
that the two envelopes cannot be replayed into each other.
"""

from urllib.parse import quote

import pytest

from cara.encryption.Crypt import Crypt
from cara.exceptions import EncryptionException


def _crypt(current: str = "k1", **extra: str) -> Crypt:
    keys = {"k1": "a" * 32, **extra}
    return Crypt(keys=keys, current_key_id=current)


def test_round_trips_through_the_urlsafe_envelope():
    crypt = _crypt()
    token = crypt.encrypt_urlsafe("yengshiu@example.com")

    assert token.startswith("u2~k1~")
    assert crypt.decrypt_urlsafe(token) == "yengshiu@example.com"


def test_token_needs_no_percent_encoding():
    """The whole point: a mail client, a Location header and a query string
    all pass it through byte-for-byte."""
    crypt = _crypt()
    token = crypt.encrypt_urlsafe('{"e":"someone@example.com","x":1790000000}')

    assert quote(token, safe="") == token
    assert "=" not in token  # padding stripped; `=` is reserved in a query


def test_v2_and_urlsafe_envelopes_cannot_be_replayed_into_each_other():
    """The version tag is authenticated as AEAD associated data, so one
    transport's ciphertext is not accepted by the other even under the same
    key."""
    crypt = _crypt()

    with pytest.raises(EncryptionException):
        crypt.decrypt(crypt.encrypt_urlsafe("secret"))
    with pytest.raises(EncryptionException):
        crypt.decrypt_urlsafe(crypt.encrypt("secret"))


def test_tampering_is_detected():
    crypt = _crypt()
    token = crypt.encrypt_urlsafe("secret")
    flipped = token[:-1] + ("A" if token[-1] != "A" else "B")

    with pytest.raises(EncryptionException):
        crypt.decrypt_urlsafe(flipped)


def test_key_id_header_is_authenticated():
    crypt = _crypt(**{"k2": "b" * 32})
    token = crypt.encrypt_urlsafe("secret")

    with pytest.raises(EncryptionException):
        crypt.decrypt_urlsafe(token.replace("u2~k1~", "u2~k2~", 1))


def test_retired_key_stays_readable_after_rotation():
    old = Crypt(keys={"old": "o" * 32}, current_key_id="old")
    token = old.encrypt_urlsafe("secret")
    rotated = Crypt(keys={"old": "o" * 32, "new": "n" * 32}, current_key_id="new")

    assert rotated.decrypt_urlsafe(token) == "secret"
    assert rotated.encrypt_urlsafe("next").startswith("u2~new~")


def test_unknown_key_is_refused_rather_than_guessed():
    minted = Crypt(keys={"gone": "g" * 32}, current_key_id="gone").encrypt_urlsafe("x")

    with pytest.raises(EncryptionException):
        _crypt().decrypt_urlsafe(minted)
