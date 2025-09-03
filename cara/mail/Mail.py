"""
Mail Manager for Cara Framework.

This module provides the main mail management functionality,
handling different drivers and mail sending operations.
"""

from typing import Optional, Type

from cara.facades import Log, Queue
from cara.mail import Mailable
from cara.mail.contracts import Mail
from cara.queues.contracts import ShouldQueue


class Mail:
    """
    Mail manager handles different mail drivers and sending operations.

    This class provides a unified interface for sending emails through
    different drivers (SMTP, etc.) following Laravel-style conventions.
    """

    def __init__(
        self,
        application,
        default_driver: str = "log",
        drivers_config: Optional[dict] = None,
    ):
        """
        Initialize mail manager.

        Args:
            application: Application instance
            default_driver: Default driver name
            drivers_config: Mail drivers configuration
        """
        self.application = application
        self.drivers: dict[str, Type[Mail]] = {}
        self.driver_instances: dict[str, Mail] = {}
        self.default_driver = default_driver
        self.drivers_config = drivers_config or {}

    def add_driver(self, name: str, driver_class: Type[Mail]) -> None:
        """
        Add a driver to the manager.

        Args:
            name: Driver name
            driver_class: Driver class that implements Mail
        """
        self.drivers[name] = driver_class

    def driver(self, name: Optional[str] = None) -> Mail:
        """
        Get mail driver instance.

        Args:
            name: Driver name, uses default if None

        Returns:
            Driver instance that implements Mail
        """
        driver_name = name or self.default_driver

        if driver_name not in self.drivers:
            raise ValueError(f"Unsupported mail driver: {driver_name}")

        # Return cached instance if exists
        if driver_name in self.driver_instances:
            return self.driver_instances[driver_name]

        # Get driver config from mail manager
        filtered_config = self._get_driver_config(driver_name)

        driver_class = self.drivers[driver_name]
        driver_instance = driver_class(filtered_config)

        # Cache the instance
        self.driver_instances[driver_name] = driver_instance

        return driver_instance

    def send(self, mailable: Mailable, driver_name: Optional[str] = None) -> bool:
        """
        Send a mailable.

        Laravel-style: If mailable implements ShouldQueue, dispatch to queue.
        Otherwise, send immediately.

        Args:
            mailable: Mailable instance to send
            driver_name: Optional driver name

        Returns:
            True if sent/queued successfully, False otherwise
        """
        # Laravel-style queue check: If mailable implements ShouldQueue, queue it
        if self._should_queue(mailable):
            return self._queue_mailable(mailable, driver_name)

        # Otherwise send immediately
        return self._send_now(mailable, driver_name)

    def _should_queue(self, mailable: Mailable) -> bool:
        """
        Check if mailable should be queued (Laravel-style ShouldQueue interface).

        Args:
            mailable: The mailable instance

        Returns:
            True if mailable implements ShouldQueue, False otherwise
        """
        return isinstance(mailable, ShouldQueue)

    def _queue_mailable(
        self, mailable: Mailable, driver_name: Optional[str] = None
    ) -> bool:
        """
        Queue a mailable for background processing.

        Args:
            mailable: The mailable to queue
            driver_name: Optional driver name

        Returns:
            True if queued successfully, False otherwise
        """
        try:
            # Create a job to send the mailable
            from cara.mail.jobs import SendMailableJob

            job = SendMailableJob(mailable, driver_name)

            # Dispatch to queue using facade
            Queue.dispatch(job)
            return True

        except Exception as e:
            # Log error using facade
            Log.error(f"Failed to queue mailable: {e}")
            return False

    def _send_now(self, mailable: Mailable, driver_name: Optional[str] = None) -> bool:
        """
        Send a mailable immediately (synchronously).

        Args:
            mailable: Mailable instance to send
            driver_name: Optional driver name

        Returns:
            True if sent successfully, False otherwise
        """
        # Set application for view rendering
        mailable.set_application(self.application)

        # Build mailable first
        mailable.build()

        # Get driver
        mail_driver = self.driver(driver_name)

        # Convert mailable to dict
        mailable_data = mailable.to_dict()

        # Send using driver
        return mail_driver.send(mailable_data)

    def mailable(self, mailable: Mailable):
        """
        Create pending mail send operation.

        Args:
            mailable: Mailable to send

        Returns:
            MailPendingSend instance for chaining
        """
        from cara.mail.MailPendingSend import MailPendingSend

        return MailPendingSend(self, mailable)

    def to(self, addresses):
        """
        Begin building a mail message.

        Args:
            addresses: Recipient address(es)

        Returns:
            MailMessage instance for chaining
        """
        from cara.mail.MailMessage import MailMessage

        return MailMessage(self).to(addresses)

    def set_default_driver(self, name: str) -> None:
        """Set the default mail driver."""
        self.default_driver = name

    def _get_driver_config(self, driver_name: str) -> dict:
        """
        Get configuration for a driver from the passed configuration.
        """
        driver_config = self.drivers_config.get(driver_name, {})
        # Filter out None values to avoid unexpected keyword arguments
        return {k: v for k, v in driver_config.items() if v is not None}
