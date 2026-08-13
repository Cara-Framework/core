from __future__ import annotations

import importlib

import pytest

sentry = importlib.import_module("cara.observability.Sentry")
tracing = importlib.import_module("cara.observability.Tracing")


@pytest.fixture(autouse=True)
def _reset_setup_state():
    sentry._setup_done = False
    tracing._setup_done = False
    yield
    sentry._setup_done = False
    tracing._setup_done = False


def test_disabled_observability_is_an_explicit_noop(monkeypatch) -> None:
    monkeypatch.setattr(
        sentry,
        "_init_sentry",
        lambda **values: pytest.fail(f"unexpected Sentry init: {values}"),
    )
    monkeypatch.setattr(
        tracing,
        "_init_tracing",
        lambda **values: pytest.fail(f"unexpected tracing init: {values}"),
    )

    sentry.setup_sentry(
        service_name="api",
        dsn="",
        environment="test",
        traces_rate=0.0,
    )
    tracing.setup_tracing(
        service_name="api",
        enabled=False,
        endpoint=None,
        environment="test",
        sample_ratio=0.0,
    )


def test_configured_setup_failure_blocks_boot_and_can_be_retried(monkeypatch) -> None:
    calls = []

    def fail(**values):
        calls.append(values)
        raise RuntimeError("telemetry unavailable")

    monkeypatch.setattr(sentry, "_init_sentry", fail)
    values = {
        "service_name": "api",
        "dsn": "https://public@example.test/1",
        "environment": "test",
        "traces_rate": 0.25,
        "release": "abc123",
    }

    for _ in range(2):
        with pytest.raises(RuntimeError, match="telemetry unavailable"):
            sentry.setup_sentry(**values)
    assert len(calls) == 2


@pytest.mark.parametrize("value", [True, "0.5", float("nan"), -0.1, 1.1])
def test_sample_ratios_are_exact_and_bounded(value) -> None:
    with pytest.raises((TypeError, ValueError)):
        tracing.setup_tracing(
            service_name="api",
            enabled=False,
            endpoint=None,
            environment="test",
            sample_ratio=value,
        )


def test_enabled_tracing_requires_an_explicit_endpoint() -> None:
    with pytest.raises(ValueError, match="endpoint is required"):
        tracing.setup_tracing(
            service_name="api",
            enabled=True,
            endpoint=None,
            environment="test",
            sample_ratio=0.1,
        )
