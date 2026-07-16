import pytest

from cara.encryption.Crypt import Crypt
from cara.exceptions import EncryptionException


def test_ciphertext_is_versioned_and_round_trips():
    crypt = Crypt(
        keys={"2026-07": "a" * 32},
        current_key_id="2026-07",
    )

    token = crypt.encrypt("seller refresh token")

    assert token.startswith("v2:2026-07:")
    assert crypt.decrypt(token) == "seller refresh token"


def test_old_key_remains_readable_after_current_key_rotates():
    old = Crypt(keys={"old": "o" * 32}, current_key_id="old")
    token = old.encrypt("credential")
    rotated = Crypt(
        keys={"old": "o" * 32, "new": "n" * 32},
        current_key_id="new",
    )

    assert rotated.decrypt(token) == "credential"
    assert rotated.encrypt("next").startswith("v2:new:")


def test_key_id_header_is_authenticated():
    crypt = Crypt(
        keys={"a": "a" * 32, "b": "b" * 32},
        current_key_id="a",
    )
    token = crypt.encrypt("credential")

    with pytest.raises(EncryptionException):
        crypt.decrypt(token.replace("v2:a:", "v2:b:", 1))


def test_unversioned_ciphertext_is_rejected():
    crypt = Crypt(keys={"current": "x" * 32}, current_key_id="current")

    with pytest.raises(EncryptionException, match="Unsupported ciphertext"):
        crypt.decrypt("unversioned-base64")


def test_missing_historical_key_fails_closed():
    old = Crypt(keys={"old": "o" * 32}, current_key_id="old")
    token = old.encrypt("credential")
    current = Crypt(keys={"new": "n" * 32}, current_key_id="new")

    with pytest.raises(EncryptionException, match="unavailable"):
        current.decrypt(token)
