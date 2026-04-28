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

        self.application.bind("mail", mail_manager)
