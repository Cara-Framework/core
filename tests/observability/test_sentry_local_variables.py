"""Sentry events must never carry stack-frame locals.

Frame locals hold tenant credentials nested inside generically named
variables (an OAuth refresh request's ``data`` carries the refresh token and
the client secret), and the SDK's default scrubber only matches top-level,
exact key names. The guard is therefore structural: no ``vars`` at all.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest

sentry = importlib.import_module("cara.observability.Sentry")

_REFRESH_TOKEN = "Atzr|refresh-token-that-must-not-leave"
_CLIENT_SECRET = "client-secret-that-must-not-leave"


def _fake_sdk(monkeypatch) -> list[dict]:
    calls: list[dict] = []
    sdk = types.ModuleType("sentry_sdk")
    sdk.init = lambda **kwargs: calls.append(kwargs)
    integrations = types.ModuleType("sentry_sdk.integrations")
    logging_module = types.ModuleType("sentry_sdk.integrations.logging")
    logging_module.LoggingIntegration = lambda **_kwargs: object()
    threading_module = types.ModuleType("sentry_sdk.integrations.threading")
    threading_module.ThreadingIntegration = lambda **_kwargs: object()
    for name, module in (
        ("sentry_sdk", sdk),
        ("sentry_sdk.integrations", integrations),
        ("sentry_sdk.integrations.logging", logging_module),
        ("sentry_sdk.integrations.threading", threading_module),
    ):
        monkeypatch.setitem(sys.modules, name, module)
    return calls


def test_sentry_init_disables_frame_local_capture(monkeypatch) -> None:
    calls = _fake_sdk(monkeypatch)

    sentry._init_sentry(
        service_name="services",
        dsn="https://public@example.invalid/1",
        environment="test",
        traces_rate=0.0,
        release="dev",
    )

    assert len(calls) == 1
    assert calls[0]["include_local_variables"] is False
    assert calls[0]["send_default_pii"] is False


def _refresh_access_token(refresh_token: str) -> None:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_secret": _CLIENT_SECRET,
    }
    raise ConnectionError(f"token endpoint unavailable ({len(data)} fields)")


def _verify_credentials() -> None:
    try:
        _refresh_access_token(_REFRESH_TOKEN)
    except ConnectionError as exc:
        raise RuntimeError("Could not resolve the seller identity.") from exc


def test_chained_exception_event_carries_no_credential_locals(monkeypatch) -> None:
    sentry_sdk = pytest.importorskip("sentry_sdk")
    events: list[dict] = []
    real_init = sentry_sdk.init

    def init_capturing(**kwargs):
        def capture(event, _hint):
            events.append(event)
            return None

        return real_init(**kwargs, before_send=capture)

    monkeypatch.setattr(sentry_sdk, "init", init_capturing)
    sentry._init_sentry(
        service_name="services",
        dsn="https://public@example.invalid/1",
        environment="test",
        traces_rate=0.0,
        release="dev",
    )
    try:
        try:
            _verify_credentials()
        except RuntimeError as exc:
            sentry_sdk.capture_exception(exc)
    finally:
        sentry_sdk.get_client().close()
        real_init()

    assert len(events) == 1
    payload = repr(events[0])
    assert _REFRESH_TOKEN not in payload
    assert _CLIENT_SECRET not in payload
    frames = [
        frame
        for value in events[0]["exception"]["values"]
        for frame in (value.get("stacktrace") or {}).get("frames", [])
    ]
    assert frames, "the chained exception must still carry its stack trace"
    assert all("vars" not in frame for frame in frames)
