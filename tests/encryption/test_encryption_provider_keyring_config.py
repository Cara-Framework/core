"""``EncryptionProvider`` must read the versioned keyring config paths.

``tests/encryption/test_crypt_keyring.py`` covers the ``Crypt`` primitive
directly (it constructs ``Crypt(keys=..., current_key_id=...)`` by hand), so
nothing pinned the *config path* the provider uses to build that keyring.

That gap has bitten this file before: the provider once looked the secret up at
``config("application.key")``, a path that exists in no config tree (there is no
``application.py``), so it raised "Application key is not set" even with
``APP_KEY`` present. Since ``EncryptionProvider`` is a ``DeferredProvider`` it
only fires when ``hash``/``crypt`` is resolved, which makes such a typo latent
until the first encrypted column is added.

The live contract, since the versioned-keyring rewrite, is:

    keys           = config("encryption.keys", {})
    current_key_id = config("encryption.current_key_id")

populated by each app's ``config/encryption.py`` (``KEYS`` / ``CURRENT_KEY_ID``).
These tests pin those exact paths so the lookup cannot silently drift again.
"""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from cara.encryption import EncryptionProvider
from cara.exceptions import EncryptionException

# Pull the MODULE (not the class re-exported by the package ``__init__``) out of
# sys.modules, so ``patch.object(mod, "config")`` targets the module-level
# ``config`` binding that ``register()`` actually calls.
__import__("cara.encryption.EncryptionProvider", fromlist=["EncryptionProvider"])
_PROVIDER_MOD = sys.modules["cara.encryption.EncryptionProvider"]
assert hasattr(_PROVIDER_MOD, "config"), (
    f"resolved {_PROVIDER_MOD!r} is not the module — sys.modules drift"
)

_SECRET = "k" * 32


class _StubApp:
    """Minimal container — records what the provider binds."""

    def __init__(self) -> None:
        self.bindings: dict[str, object] = {}

    def bind(self, name: str, factory):
        self.bindings[name] = factory

    def has(self, _name: str) -> bool:
        return False

    def make(self, _name: str):
        raise KeyError(_name)


def _stub_config(values: dict[str, object | None]):
    """Patch the provider's ``config`` to consult a fixture dict, and record
    every key the provider asks for."""
    asked: list[str] = []

    def fake_config(key, default=None):
        asked.append(key)
        return values.get(key, default)

    return patch.object(_PROVIDER_MOD, "config", new=fake_config), asked


class TestEncryptionProviderReadsKeyringConfigPaths:
    def test_binds_crypt_from_keyring_config(self) -> None:
        app = _StubApp()
        provider = EncryptionProvider(application=app)

        patcher, asked = _stub_config(
            {
                "encryption.keys": {"local": _SECRET},
                "encryption.current_key_id": "local",
            }
        )
        with patcher:
            provider.register()

        assert "encryption.keys" in asked, (
            f"provider must read the keyring from 'encryption.keys'; it asked "
            f"for {asked!r}"
        )
        assert "encryption.current_key_id" in asked, (
            f"provider must read the active key id from "
            f"'encryption.current_key_id'; it asked for {asked!r}"
        )
        assert app.bindings.keys() == {"hash", "crypt"}

    def test_bound_crypt_round_trips_through_the_configured_key(self) -> None:
        """The bound factory must build a working Crypt from the config values —
        not merely read them."""
        app = _StubApp()
        provider = EncryptionProvider(application=app)

        patcher, _ = _stub_config(
            {
                "encryption.keys": {"local": _SECRET},
                "encryption.current_key_id": "local",
            }
        )
        with patcher:
            provider.register()

        crypt = app.bindings["crypt"]()
        token = crypt.encrypt("plaintext")
        assert token.startswith("v2:local:"), (
            f"ciphertext must carry the configured key id; got {token!r}"
        )
        assert crypt.decrypt(token) == "plaintext"


class TestEncryptionProviderFailsClosedWhenKeyringMissing:
    """A missing keyring must raise, not bind a half-configured Crypt."""

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param({}, id="nothing-configured"),
            pytest.param({"encryption.current_key_id": "local"}, id="keys-missing"),
            pytest.param(
                {"encryption.keys": {"local": _SECRET}}, id="current-key-id-missing"
            ),
            pytest.param(
                {"encryption.keys": {}, "encryption.current_key_id": "local"},
                id="empty-keyring",
            ),
        ],
    )
    def test_missing_keyring_raises(self, values) -> None:
        app = _StubApp()
        provider = EncryptionProvider(application=app)

        patcher, _ = _stub_config(values)
        with patcher, pytest.raises(EncryptionException):
            provider.register()

        assert app.bindings == {}, (
            "provider must not bind anything when the keyring is unusable"
        )

    def test_legacy_single_key_paths_are_not_consulted(self) -> None:
        """``app.key`` / ``encryption.key`` / ``application.key`` were the
        pre-rewrite lookups. They are dead — a tree that only sets those must
        fail closed rather than quietly encrypt under a stale single key."""
        app = _StubApp()
        provider = EncryptionProvider(application=app)

        patcher, _ = _stub_config(
            {
                "app.key": _SECRET,
                "encryption.key": _SECRET,
                "application.key": _SECRET,
            }
        )
        with patcher, pytest.raises(EncryptionException):
            provider.register()
