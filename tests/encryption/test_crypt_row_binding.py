"""Row binding: a ciphertext opens only under the context it was sealed with."""

from __future__ import annotations

import json

import pytest

from cara.eloquent.casts.EncryptedJsonCast import EncryptedJsonCast
from cara.eloquent.models import Model
from cara.encryption.Crypt import Crypt
from cara.exceptions import EncryptionException


def _crypt() -> Crypt:
    return Crypt(keys={"k1": "k" * 32}, current_key_id="k1")


def test_bound_ciphertext_opens_only_under_its_context():
    crypt = _crypt()
    token = crypt.encrypt(
        "refresh", context="channel.credentials|tenant_id=1|public_id=CHN_A"
    )

    assert (
        crypt.decrypt(token, context="channel.credentials|tenant_id=1|public_id=CHN_A")
        == "refresh"
    )
    with pytest.raises(EncryptionException):
        crypt.decrypt(token, context="channel.credentials|tenant_id=2|public_id=CHN_A")
    with pytest.raises(EncryptionException):
        crypt.decrypt(token, context="channel.credentials|tenant_id=1|public_id=CHN_B")
    with pytest.raises(EncryptionException):
        crypt.decrypt(token)


def test_unbound_ciphertext_does_not_open_under_a_context():
    crypt = _crypt()
    token = crypt.encrypt("refresh")

    with pytest.raises(EncryptionException):
        crypt.decrypt(token, context="channel.credentials|tenant_id=1|public_id=CHN_A")
    assert crypt.decrypt(token) == "refresh"


def test_urlsafe_envelope_binds_the_same_way():
    crypt = _crypt()
    token = crypt.encrypt_urlsafe("link", context="ctx")

    assert crypt.decrypt_urlsafe(token, context="ctx") == "link"
    with pytest.raises(EncryptionException):
        crypt.decrypt_urlsafe(token, context="other")


def test_empty_context_is_refused():
    with pytest.raises(EncryptionException):
        _crypt().encrypt("x", context="")


def test_key_id_of_reads_the_envelope_header():
    token = _crypt().encrypt("x", context="ctx")
    assert Crypt.key_id_of(token) == "k1"
    with pytest.raises(EncryptionException):
        Crypt.key_id_of("not-an-envelope")


class _SealedChannel(Model):
    __table__ = "channel"
    __casts__ = {"credentials": "encrypted_json"}
    __seal_context__ = {"credentials": ("tenant_id", "public_id")}


@pytest.fixture
def _bound_crypt(monkeypatch):
    crypt = _crypt()
    # Rebind the module name (not the facade attribute): patching a Cara
    # facade can boot the application.
    import importlib

    module = importlib.import_module("cara.eloquent.casts.EncryptedCast")
    monkeypatch.setattr(module, "Crypt", crypt)
    # No database here: hydrate boots a builder, which needs a connection.
    from unittest.mock import MagicMock

    monkeypatch.setattr(_SealedChannel, "get_builder", lambda _self: MagicMock())
    return crypt


def test_model_seals_a_column_to_its_row(_bound_crypt):
    stored = _SealedChannel.cast_value(
        "credentials", {"refresh_token": "r"}, {"tenant_id": 7, "public_id": "CHN_A"}
    )
    token = EncryptedJsonCast.token_of(stored)

    assert _bound_crypt.decrypt(
        token,
        context=_SealedChannel.seal_context(
            "credentials", {"tenant_id": 7, "public_id": "CHN_A"}
        ),
    ) == json.dumps({"refresh_token": "r"})

    row = _SealedChannel.hydrate(
        {"id": 1, "tenant_id": 7, "public_id": "CHN_A", "credentials": stored}
    )
    assert row.credentials == {"refresh_token": "r"}


def test_a_document_copied_onto_another_row_refuses_to_open(_bound_crypt):
    stored = _SealedChannel.cast_value(
        "credentials", {"refresh_token": "r"}, {"tenant_id": 7, "public_id": "CHN_A"}
    )
    other_row = _SealedChannel.hydrate(
        {"id": 2, "tenant_id": 7, "public_id": "CHN_B", "credentials": stored}
    )
    other_tenant = _SealedChannel.hydrate(
        {"id": 1, "tenant_id": 8, "public_id": "CHN_A", "credentials": stored}
    )

    with pytest.raises(EncryptionException):
        _ = other_row.credentials
    with pytest.raises(EncryptionException):
        _ = other_tenant.credentials


def test_sealing_without_the_binding_fields_fails_closed(_bound_crypt):
    with pytest.raises(EncryptionException, match="sealed to public_id"):
        _SealedChannel.cast_value("credentials", {"a": 1}, {"tenant_id": 7})
    with pytest.raises(EncryptionException, match="sealed to tenant_id"):
        _SealedChannel.cast_value("credentials", {"a": 1})


def test_assignment_on_a_loaded_row_seals_to_that_row(_bound_crypt):
    row = _SealedChannel.hydrate({"id": 1, "tenant_id": 7, "public_id": "CHN_A"})
    row.credentials = {"refresh_token": "new"}

    assert row.credentials == {"refresh_token": "new"}
    token = EncryptedJsonCast.token_of(row.__dirty_attributes__["credentials"])
    with pytest.raises(EncryptionException):
        _bound_crypt.decrypt(token)


def test_a_sealed_column_needs_an_encrypting_cast():
    class _Wrong(Model):
        __table__ = "wrong"
        __casts__ = {"settings": "json"}
        __seal_context__ = {"settings": ("tenant_id",)}

    with pytest.raises(EncryptionException, match="does not bind a row"):
        _Wrong.cast_value("settings", {"a": 1}, {"tenant_id": 1})


class _Secret(Model):
    __table__ = "secret_row"
    __casts__ = {"secret": "encrypted"}


def test_a_new_instance_save_encrypts_its_column_exactly_once(monkeypatch):
    """``save()`` on a NEW instance hands ``create`` its ``all_attributes()``
    with ``cast=True`` although ``__setattr__`` already set-cast the value.
    That is one layer, not two: ``all_attributes`` reads each cast column
    back through its GET cast, so ``create`` seals the plaintext once."""
    crypt = _crypt()
    import importlib
    from unittest.mock import MagicMock

    module = importlib.import_module("cara.eloquent.casts.EncryptedCast")
    monkeypatch.setattr(module, "Crypt", crypt)
    monkeypatch.setattr(_Secret, "get_builder", lambda _self: MagicMock())

    row = _Secret()
    row.secret = "plain"
    stored = _Secret.cast_values(dict(row.all_attributes()))["secret"]

    assert crypt.decrypt(stored) == "plain"
