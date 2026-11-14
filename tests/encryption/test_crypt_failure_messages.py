"""A refused ciphertext names why: AESGCM's InvalidTag has an empty message,
and "Decryption failed: " with nothing after it reached a dashboard list."""

import base64
import os

import pytest

from cara.encryption.Crypt import Crypt
from cara.exceptions import EncryptionException


def _crypt() -> Crypt:
    key = "base64:" + base64.b64encode(os.urandom(32)).decode()
    return Crypt(key)


def test_wrong_row_binding_names_the_reason():
    crypt = _crypt()
    token = crypt.encrypt("secret", context="channel:1:a")
    with pytest.raises(EncryptionException) as raised:
        crypt.decrypt(token, context="channel:2:b")
    message = str(raised.value)
    assert "does not open for this row" in message
    assert "re-seal" in message


def test_unbound_value_read_with_a_binding_is_explained():
    crypt = _crypt()
    token = crypt.encrypt("secret")
    with pytest.raises(EncryptionException) as raised:
        crypt.decrypt(token, context="channel:1:a")
    assert str(raised.value).strip().endswith(")")
    assert "Decryption failed: " != str(raised.value)
