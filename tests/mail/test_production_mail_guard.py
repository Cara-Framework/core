"""Fail-closed production mail guard at the framework boot seam.

``MailProvider.register()`` must refuse to bind the ``mail`` service in
a production deploy (``APP_ENV`` in ``production`` / ``prod``, any case)
when the configured transport cannot actually deliver email:

1. Default driver is a discarding dev/test driver (``log`` / ``array``)
   — every email is written to the logger / kept in memory and never
   reaches a user. Password resets, magic links and alerts silently
   vanish while the app looks healthy.
2. Default driver is the SMTP transport (or a registered SMTP-derived
   driver) but the RAW environment carries no ``MAIL_HOST`` /
   ``MAIL_USERNAME``. The check reads the raw env on purpose: config
   files ship dev sandbox defaults (``smtp.mailtrap.io``), and a
   default value must never satisfy a production boot.

Non-production environments (dev, test, staging, unset) never raise.

This is the framework-owned twin of the app-level ``config/mail.py``
guards in the consuming products — additive defense at the subsystem's
registration seam, so a consumer without the app-level guard still
fails closed.
"""

from __future__ import annotations

import pytest

from cara.configuration import Configuration
from cara.mail.drivers import SmtpDriver
from cara.mail.Mail import Mail
from cara.mail.MailProvider import MailProvider


class StubApplication:
    """Minimal container surface for MailProvider.register()."""

    def __init__(self) -> None:
        self.bindings: dict[str, object] = {}

    def bind(self, key: str, value: object) -> None:
        self.bindings[key] = value


@pytest.fixture
def mail_config(monkeypatch: pytest.MonkeyPatch):
    """Seed the Configuration singleton with a mail section and return a
    setter for the default driver. ``monkeypatch.setitem`` restores the
    singleton's dict after each test, so no cross-test pollution."""
    Configuration()  # ensure the bare singleton exists
    config_store = Configuration._instance._config

    def _set(default_driver: str) -> None:
        monkeypatch.setitem(config_store, "mail.default", default_driver)
        monkeypatch.setitem(config_store, "mail.from_address", "noreply@app.example")
        monkeypatch.setitem(
            config_store,
            "mail.drivers",
            {
                # Dev sandbox defaults on purpose: the guard must NOT be
                # satisfied by config values — only by the raw env.
                "smtp": {"host": "smtp.mailtrap.io", "port": 587},
                "log": {"channel": "mail"},
                "array": {"store_path": "storage/mail/array"},
            },
        )

    return _set


@pytest.fixture
def clean_mail_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Start every test from an unset APP_ENV / MAIL_* environment."""
    for name in ("APP_ENV", "MAIL_HOST", "MAIL_USERNAME", "MAIL_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


class TestProductionRefusesDiscardingDrivers:
    @pytest.mark.parametrize("app_env", ["production", "prod", "Production"])
    @pytest.mark.parametrize("driver", ["log", "array"])
    def test_production_with_discarding_driver_raises(
        self, mail_config, clean_mail_env, app_env: str, driver: str
    ) -> None:
        mail_config(driver)
        clean_mail_env.setenv("APP_ENV", app_env)

        with pytest.raises(RuntimeError, match="dev/test mail driver"):
            MailProvider(StubApplication()).register()


class TestProductionRequiresRawSmtpEnv:
    def test_production_smtp_without_raw_env_raises(
        self, mail_config, clean_mail_env
    ) -> None:
        """Config carries mailtrap defaults, but the RAW env has no
        MAIL_HOST / MAIL_USERNAME — the defaults must not save the boot."""
        mail_config("smtp")
        clean_mail_env.setenv("APP_ENV", "production")

        with pytest.raises(RuntimeError, match="MAIL_HOST / MAIL_USERNAME"):
            MailProvider(StubApplication()).register()

    def test_production_smtp_with_host_but_no_username_raises(
        self, mail_config, clean_mail_env
    ) -> None:
        mail_config("smtp")
        clean_mail_env.setenv("APP_ENV", "production")
        clean_mail_env.setenv("MAIL_HOST", "smtp.real-provider.example")

        with pytest.raises(RuntimeError, match="MAIL_HOST / MAIL_USERNAME"):
            MailProvider(StubApplication()).register()

    def test_production_smtp_fully_configured_boots(
        self, mail_config, clean_mail_env
    ) -> None:
        mail_config("smtp")
        clean_mail_env.setenv("APP_ENV", "production")
        clean_mail_env.setenv("MAIL_HOST", "smtp.real-provider.example")
        clean_mail_env.setenv("MAIL_USERNAME", "apikey")

        application = StubApplication()
        MailProvider(application).register()

        assert isinstance(application.bindings["mail"], Mail)
        assert application.bindings["mail"].default_driver == "smtp"

    def test_smtp_derivative_driver_is_also_guarded(self, clean_mail_env) -> None:
        """A gmail-style SMTP subclass registered under another name gets
        the same raw-env requirement (guard checked directly — register()
        only wires the built-in driver set)."""

        class GmailDriver(SmtpDriver):
            driver_name = "gmail"

        manager = Mail(StubApplication(), "gmail", {})
        manager.add_driver("gmail", GmailDriver)
        clean_mail_env.setenv("APP_ENV", "prod")

        with pytest.raises(RuntimeError, match="MAIL_HOST / MAIL_USERNAME"):
            MailProvider._guard_production_transport(manager)


class TestNonProductionNeverRaises:
    @pytest.mark.parametrize("app_env", [None, "", "local", "dev", "test", "staging"])
    @pytest.mark.parametrize("driver", ["log", "array", "smtp"])
    def test_non_production_boots_without_mail_env(
        self, mail_config, clean_mail_env, app_env: str | None, driver: str
    ) -> None:
        mail_config(driver)
        if app_env is not None:
            clean_mail_env.setenv("APP_ENV", app_env)

        application = StubApplication()
        MailProvider(application).register()

        assert isinstance(application.bindings["mail"], Mail)
