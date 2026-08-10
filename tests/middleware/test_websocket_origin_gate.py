"""The WebSocket Origin gate must not have a second, silent path to "allow".

``_origin_is_allowed`` wrapped its config read in ``except Exception:
allowed = []`` — and an empty allowlist means "no check". So an ops team
that DID configure ``broadcasting.websocket.allowed_origins`` had it
silently dropped to permissive if the read raised, and nothing logged. A
gate whose failure mode and whose default are both "allow" is decoration,
not defence-in-depth.

``Configuration.get`` returns the default on a missing key and never raises,
so the handler could only ever hide a real fault. The empty-allowlist path
survives deliberately — it is the development posture (localhost ports vary
per surface, non-browser clients send no Origin at all); production is held
to a configured allowlist at BOOT by the deployable that serves the upgrade.
"""

from __future__ import annotations

import sys

import pytest

from cara.middleware.ws.Authenticate import Authenticate

# The package barrel shadows the submodule attribute, so the module object
# has to come from ``sys.modules`` rather than ``import ... as``.
module = sys.modules["cara.middleware.ws.Authenticate"]


class _Socket:
    def __init__(self, origin: str | None = None) -> None:
        headers = []
        if origin is not None:
            headers.append((b"origin", origin.encode()))
        self.scope = {"headers": headers}


def _middleware() -> Authenticate:
    """Build without the provider boot the base ``Middleware.__init__``
    triggers."""
    return Authenticate.__new__(Authenticate)


class TestConfigReadFailureIsNotSwallowed:
    def test_a_raising_config_read_propagates_instead_of_allowing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pinned wrong behaviour: this returned ``True`` — the handshake
        was accepted from any origin because reading the allowlist failed."""

        def _boom(key, default=None):
            raise RuntimeError("config not booted")

        monkeypatch.setattr(module, "config", _boom)

        with pytest.raises(RuntimeError, match="config not booted"):
            _middleware()._origin_is_allowed(_Socket("https://evil.example"))


class TestConfiguredAllowlistIsEnforced:
    def test_a_foreign_origin_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(module, "config", lambda *_a, **_k: ["https://app.example"])

        assert _middleware()._origin_is_allowed(_Socket("https://evil.example")) is False

    def test_an_allowed_origin_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(module, "config", lambda *_a, **_k: ["https://app.example"])

        assert _middleware()._origin_is_allowed(_Socket("https://app.example")) is True

    def test_a_non_browser_client_without_origin_still_passes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """curl/Postman send no Origin and there is no clean way to tell
        them from a malicious browser without UA fingerprinting."""
        monkeypatch.setattr(module, "config", lambda *_a, **_k: ["https://app.example"])

        assert _middleware()._origin_is_allowed(_Socket()) is True


class TestEmptyAllowlistRemainsTheDevelopmentPosture:
    def test_no_configured_origins_performs_no_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(module, "config", lambda *_a, **_k: None)

        assert _middleware()._origin_is_allowed(_Socket("http://localhost:3400")) is True
