"""
Notification Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the notification
subsystem, including mail, database, slack and log notification channels.
"""

from cara.configuration import config
from cara.foundation import DeferredProvider
from cara.notifications import Notification
from cara.notifications.channels import (
    DatabaseChannel,
    LogChannel,
    MailChannel,
    SlackChannel,
)


class NotificationProvider(DeferredProvider):
    """
    Deferred provider for the notification subsystem.

    Reads configuration and registers the Notification manager and its channels.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["notification"]

    def register(self) -> None:
        """Register notification services with configuration."""
        settings = config("notifications", {})
        default_channels = settings.get("default", ["mail", "database"])

        notification_manager = Notification(self.application, default_channels)

        # Register notification channels
        self._add_mail_channel(notification_manager, settings)
        self._add_database_channel(notification_manager, settings)
        self._add_slack_channel(notification_manager, settings)
        self._add_log_channel(notification_manager, settings)

        self.application.bind("notification", notification_manager)

    def _add_mail_channel(
        self, notification_manager: Notification, settings: dict
    ) -> None:
        """Register mail notification channel with configuration."""
        mail_settings = settings.get("channels", {}).get("mail", {})

        try:
            mail_manager = self.application.make("mail")
            channel = MailChannel(
                mail_manager=mail_manager,
                from_address=mail_settings.get("from_address"),
                from_name=mail_settings.get("from_name"),
                reply_to=mail_settings.get("reply_to"),
            )
            notification_manager.add_channel(MailChannel.channel_name, channel)
        except Exception:
            # Mail service not available, skip
            pass

    def _add_database_channel(
        self, notification_manager: Notification, settings: dict
    ) -> None:
        """Register database notification channel with configuration."""
        database_settings = settings.get("channels", {}).get("database", {})

        query_builder = self.application.make("DB").query()
        channel = DatabaseChannel(
            database_manager=query_builder,
            table_name=database_settings.get("table", "notifications"),
        )
        notification_manager.add_channel(DatabaseChannel.channel_name, channel)

    def _add_slack_channel(
        self, notification_manager: Notification, settings: dict
    ) -> None:
        """Register Slack notification channel with configuration."""
        slack_settings = settings.get("channels", {}).get("slack", {})

        if not slack_settings:
            return  # Slack is optional

        webhook_url = slack_settings.get("webhook_url")
        if not webhook_url:
            return  # No webhook URL configured

        channel = SlackChannel(
            webhook_url=webhook_url,
            default_channel=slack_settings.get("channel"),
            username=slack_settings.get("username", "Cara Bot"),
            icon=slack_settings.get("icon", ":robot_face:"),
        )
        notification_manager.add_channel(SlackChannel.channel_name, channel)

    def _add_log_channel(
        self, notification_manager: Notification, settings: dict
    ) -> None:
        """Register log notification channel with configuration."""
        log_settings = settings.get("channels", {}).get("log", {})

        channel = LogChannel(
            log_file=log_settings.get("file", "notifications.log"),
            log_level=log_settings.get("level", "info"),
        )
        notification_manager.add_channel(LogChannel.channel_name, channel)
