"""
Mail Provider for Cara Framework.

This module provides the deferred service provider that configures and registers the mail
subsystem, including SMTP, Log, Array, and Mailgun mail drivers.
"""

from __future__ import annotations

from cara.configuration import config
from cara.environment import env
from cara.exceptions import MailConfigurationException
from cara.foundation import DeferredProvider
from cara.mail.drivers import ArrayDriver, LogDriver, MailgunDriver, SmtpDriver
from cara.mail.Mail import Mail

#: APP_ENV values that mean "this is a real production deploy". Normalised
#: case + the ``prod`` alias so a stray ``APP_ENV=Production`` still counts.
_PRODUCTION_ENVS = ("production", "prod")

#: Drivers that never deliver mail — ``log`` writes every email to the
#: logger, ``array`` keeps them in memory. Fine for dev/test, catastrophic
#: in production: password resets, magic links and alerts all silently
#: disappear while the app looks healthy.
_DISCARDING_DRIVERS = ("log", "array")


class MailProvider(DeferredProvider):
    @classmethod
    def provides(cls) -> list[str]:
        return ["mail"]

    def __init__(self, application):
        """
        Initialize mail provider.
        """
        self.application = application

    def register(self) -> None:
        """Register mail service and drivers with configuration."""
        default_driver = config("mail.default", "log")

        from_address = config("mail.from_address")
        if not from_address:
            raise MailConfigurationException(
                "FROM_ADDRESS is required in mail configuration. "
                "Please set MAIL_FROM_ADDRESS environment variable."
            )

        drivers_config = config("mail.drivers", {})
        for driver_config in (drivers_config or {}).values():
            if isinstance(driver_config, dict) and "from_address" not in driver_config:
                driver_config["from_address"] = from_address

        mail_manager = Mail(self.application, default_driver, drivers_config)

        mail_manager.add_driver(SmtpDriver.driver_name, SmtpDriver)
        mail_manager.add_driver(LogDriver.driver_name, LogDriver)
        mail_manager.add_driver(ArrayDriver.driver_name, ArrayDriver)
        mail_manager.add_driver(MailgunDriver.driver_name, MailgunDriver)

        self._guard_production_transport(mail_manager)

        self.application.bind("mail", mail_manager)

    @staticmethod
    def _guard_production_transport(mail_manager: Mail) -> None:
        """Fail closed: refuse to register a production mail subsystem
        that cannot actually deliver email.

        Framework-owned twin of the app-level ``config/mail.py`` guards
        (same posture as the products' CORS / broadcasting production
        guards). Two legs:

        1. The default driver is a discarding dev/test driver
           (``log`` / ``array``) — every email is thrown away.
        2. The default driver is the SMTP transport (or a registered
           SMTP derivative, e.g. a gmail-style subclass) but the RAW
           environment carries no ``MAIL_HOST`` / ``MAIL_USERNAME``.
           Read from the raw env on purpose: config files ship dev
           sandbox defaults (``smtp.mailtrap.io``), and a default value
           must never satisfy a production boot.

        Non-production environments never raise — this guard only fires
        when ``APP_ENV`` is ``production`` / ``prod`` (any case).
        """
        app_env = str(env("APP_ENV", "") or "").strip().lower()
        if app_env not in _PRODUCTION_ENVS:
            return

        driver_name = mail_manager.default_driver
        if driver_name in _DISCARDING_DRIVERS:
            raise RuntimeError(
                f"MAIL_DRIVER={driver_name!r} is a dev/test mail driver — "
                "emails are discarded, not delivered. Refusing to boot in "
                f"APP_ENV={app_env!r}. Set MAIL_DRIVER to a real transport "
                "(e.g. smtp / mailgun) before deploying."
            )

        driver_class = mail_manager.drivers.get(driver_name)
        is_smtp_transport = isinstance(driver_class, type) and issubclass(
            driver_class, SmtpDriver
        )
        if is_smtp_transport and not (env("MAIL_HOST", "") and env("MAIL_USERNAME", "")):
            raise RuntimeError(
                "SMTP transport is not configured (MAIL_HOST / MAIL_USERNAME "
                "unset in the environment) — every outgoing email would be "
                "silently skipped or sent to a sandbox default. Refusing to "
                f"boot in APP_ENV={app_env!r}. Set MAIL_HOST, MAIL_USERNAME "
                "and MAIL_PASSWORD before deploying."
            )
