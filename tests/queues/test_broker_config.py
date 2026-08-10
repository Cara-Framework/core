"""Broker configuration guards: every one of them must fail at boot.

These are not preferences. A guest credential, a shared vhost, a plaintext
transport to an external broker and a superuser identity all produce a system
that works perfectly until it is someone else's. The only place to notice them
is the process refusing to start, so each test here pins a REFUSAL.
"""

from __future__ import annotations

import pytest

from cara.queues.BrokerConfig import (
    AMQP_MAX_PRIORITY,
    AMQP_PRIORITY_LEVELS,
    queue_signing_keyring,
    rabbit_broker_access,
    rabbit_credentials,
    rabbit_scheme,
    require_isolated_vhost,
)


@pytest.fixture(autouse=True)
def _clean_broker_env(monkeypatch: pytest.MonkeyPatch):
    for name in (
        "APP_ENV",
        "APP_KEY",
        "CACHE_SIGNING_KEY",
        "QUEUE_BROKER_ACCESS",
        "QUEUE_SIGNING_KEY",
        "QUEUE_SIGNING_KEY_ID",
        "QUEUE_SIGNING_PREVIOUS_KEYS",
        "RABBIT_HOST",
        "RABBIT_PASSWORD",
        "RABBIT_SCHEME",
        "RABBIT_USERNAME",
        "RABBIT_VHOST",
    ):
        monkeypatch.delenv(name, raising=False)


# ── priority vocabulary ──────────────────────────────────────────────────
def test_priority_vocabulary_is_covered_by_max_priority() -> None:
    assert set(AMQP_PRIORITY_LEVELS) == {"critical", "high", "default", "low"}
    assert max(AMQP_PRIORITY_LEVELS.values()) == AMQP_MAX_PRIORITY


# ── broker capability ────────────────────────────────────────────────────
def test_broker_access_rejects_an_unknown_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUEUE_BROKER_ACCESS", "admin")
    with pytest.raises(RuntimeError, match="QUEUE_BROKER_ACCESS must be"):
        rabbit_broker_access()


def test_broker_access_forbids_the_catch_all_identity_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("QUEUE_BROKER_ACCESS", "full")
    with pytest.raises(RuntimeError, match="forbidden in production"):
        rabbit_broker_access()


def test_broker_access_allows_full_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "local")
    assert rabbit_broker_access() == "full"


# ── credentials ──────────────────────────────────────────────────────────
def test_credentials_refuse_the_guest_default_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(RuntimeError, match="must not be left at the"):
        rabbit_credentials()


def test_credentials_allow_the_guest_default_outside_production() -> None:
    assert rabbit_credentials() == ("guest", "guest")


def test_credentials_without_an_access_argument_ignore_the_capability_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Opt-in: a product that has not provisioned capability users keeps the
    # plain guest guard and is not handed a sentinel it cannot connect with.
    monkeypatch.setenv("QUEUE_BROKER_ACCESS", "none")
    monkeypatch.setenv("RABBIT_USERNAME", "app")
    monkeypatch.setenv("RABBIT_PASSWORD", "secret")
    assert rabbit_credentials() == ("app", "secret")


def test_credentials_for_a_no_broker_process_cannot_authenticate() -> None:
    username, password = rabbit_credentials(access="none")
    assert username == password
    assert username not in {"guest", ""}


# ── vhost isolation ──────────────────────────────────────────────────────
def test_vhost_defaults_to_the_product_name() -> None:
    assert require_isolated_vhost("widgets") == "widgets"


def test_vhost_production_pins_the_product_vhost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RABBIT_VHOST", "/")
    with pytest.raises(RuntimeError, match="isolated 'widgets' vhost in production"):
        require_isolated_vhost("widgets")


def test_a_blank_env_value_falls_back_to_the_product_vhost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ``env()`` treats a whitespace-only value as unset, so a blank
    # RABBIT_VHOST lands on the product vhost rather than on the root one.
    monkeypatch.setenv("RABBIT_VHOST", "   ")
    assert require_isolated_vhost("widgets") == "widgets"


def test_vhost_refuses_an_empty_resolved_value() -> None:
    with pytest.raises(RuntimeError, match="RABBIT_VHOST must not be empty"):
        require_isolated_vhost("widgets", default="")


def test_vhost_refuses_an_empty_product() -> None:
    with pytest.raises(RuntimeError, match="non-empty product vhost"):
        require_isolated_vhost("  ")


def test_vhost_default_lets_a_product_keep_a_legacy_root_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "local")
    assert require_isolated_vhost("widgets", default="/") == "/"


# ── transport ────────────────────────────────────────────────────────────
def test_scheme_rejects_an_unknown_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RABBIT_SCHEME", "http")
    with pytest.raises(RuntimeError, match="must be either 'amqp' or 'amqps'"):
        rabbit_scheme()


def test_scheme_requires_tls_for_an_external_production_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RABBIT_HOST", "broker.example.net")
    with pytest.raises(RuntimeError, match="amqps is required"):
        rabbit_scheme()


def test_scheme_allows_plaintext_to_a_private_production_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("RABBIT_HOST", "rabbitmq")
    assert rabbit_scheme() == "amqp"


# ── signing keyring ──────────────────────────────────────────────────────
def test_signing_keyring_refuses_to_boot_without_a_key() -> None:
    with pytest.raises(RuntimeError):
        queue_signing_keyring()


def test_signing_keyring_refuses_a_key_reused_from_another_subsystem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = "k" * 48
    monkeypatch.setenv("QUEUE_SIGNING_KEY_ID", "v1")
    monkeypatch.setenv("QUEUE_SIGNING_KEY", shared)
    monkeypatch.setenv("QUEUE_SIGNING_PREVIOUS_KEYS", "{}")
    monkeypatch.setenv("APP_KEY", shared)
    with pytest.raises(RuntimeError):
        queue_signing_keyring()


def test_signing_keyring_returns_the_active_key_and_ring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUEUE_SIGNING_KEY_ID", "v1")
    monkeypatch.setenv("QUEUE_SIGNING_KEY", "q" * 48)
    monkeypatch.setenv("QUEUE_SIGNING_PREVIOUS_KEYS", "{}")
    key_id, keyring = queue_signing_keyring()
    assert key_id == "v1"
    assert keyring["v1"] == "q" * 48
