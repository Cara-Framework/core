"""
Mail Provider for Cara Framework.

This module provides the deferred service provider that configures and registers the mail
subsystem, including SMTP, Log, Array, and Mailgun mail drivers.
"""

from cara.configuration import config
from cara.exceptions import MailConfigurationException
from cara.foundation import DeferredProvider
from cara.mail import Mail
from cara.mail.drivers import ArrayDriver, LogDriver, MailgunDriver, SmtpDriver


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
        settings = config("mail", {})
        default_driver = settings.get("default", "log")

        # Validate required global mail settings
        from_address = settings.get("from_address")
        if not from_address:
            raise MailConfigurationException(
                "FROM_ADDRESS is required in mail configuration. Please set MAIL_FROM_ADDRESS environment variable."
            )

        # Pass from_address to all drivers
        drivers_config = settings.get("drivers", {})
        for driver_name, driver_config in drivers_config.items():
            if "from_address" not in driver_config:
                driver_config["from_address"] = from_address

        mail_manager = Mail(self.application, default_driver, drivers_config)

        # Register mail drivers
        self._add_smtp_driver(mail_manager, settings)
        self._add_log_driver(mail_manager, settings)
        self._add_array_driver(mail_manager, settings)
        self._add_mailgun_driver(mail_manager, settings)

        self.application.bind("mail", mail_manager)

    def _add_smtp_driver(self, mail_manager: Mail, settings: dict) -> None:
        """Register SMTP driver class with configuration."""
        mail_manager.add_driver(SmtpDriver.driver_name, SmtpDriver)

    def _add_log_driver(self, mail_manager: Mail, settings: dict) -> None:
        """Register Log driver class with configuration."""
        mail_manager.add_driver(LogDriver.driver_name, LogDriver)

    def _add_array_driver(self, mail_manager: Mail, settings: dict) -> None:
        """Register Array driver class with configuration."""
        mail_manager.add_driver(ArrayDriver.driver_name, ArrayDriver)

    def _add_mailgun_driver(self, mail_manager: Mail, settings: dict) -> None:
        """Register Mailgun driver class with configuration."""
        mail_manager.add_driver(MailgunDriver.driver_name, MailgunDriver)
