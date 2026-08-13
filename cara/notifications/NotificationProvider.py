"""
Notification Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the notification
subsystem, including mail, database, slack and log notification channels.
"""

from __future__ import annotations

from cara.configuration import config
from cara.foundation import DeferredProvider
from cara.notifications.channels import (
    DatabaseChannel,
    LogChannel,
    MailChannel,
    SlackChannel,
)
from cara.notifications.Notification import Notification


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
        notification_manager = Notification()

        self._add_mail_channel(notification_manager)
        self._add_database_channel(notification_manager)
        self._add_slack_channel(notification_manager)
        self._add_log_channel(notification_manager)

        self.application.bind("notification", notification_manager)

    def _add_mail_channel(self, notification_manager: Notification) -> None:
        """Register mail notification channel with configuration."""
        channel = MailChannel(
            mail_manager=self.application.make("mail"),
            from_address=config("notifications.channels.mail.from_address"),
            from_name=config("notifications.channels.mail.from_name"),
            reply_to=config("notifications.channels.mail.reply_to"),
            link_settings={
                key: config(key) or None
                for key in (
                    "app.frontend_url",
                    "app.preferences_url",
                    "app.unsubscribe_confirm_url",
                    "app.unsubscribe_secret",
                    "app.unsubscribe_url",
                )
            },
        )
        notification_manager.add_channel(MailChannel.channel_name, channel)

    def _add_database_channel(self, notification_manager: Notification) -> None:
        """Register database notification channel with configuration.

        A missing DB binding is provider-order damage and blocks boot. A
        partially registered manager would only defer the outage until the
        first notification asks for the absent channel.
        """
        channel = DatabaseChannel(
            database_manager=self.application.make("DB").query(),
            table_name=config(
                "notifications.channels.database.table",
                "notifications",
            ),
        )
        notification_manager.add_channel(DatabaseChannel.channel_name, channel)

    def _add_slack_channel(self, notification_manager: Notification) -> None:
        """Register Slack notification channel with configuration.

        An absent webhook disables this optional channel. A present malformed
        webhook blocks boot instead of leaving a requested channel unavailable.
        """
        webhook_url = config("notifications.channels.slack.webhook_url")
        if webhook_url is None or webhook_url == "":
            return

        channel = SlackChannel(
            webhook_url=webhook_url,
            default_channel=config("notifications.channels.slack.channel"),
            username=config("notifications.channels.slack.username", "Cara Bot"),
            icon=config("notifications.channels.slack.icon", ":robot_face:"),
        )
        notification_manager.add_channel(SlackChannel.channel_name, channel)

    def _add_log_channel(self, notification_manager: Notification) -> None:
        """Register log notification channel with configuration.

        Configuration and registry errors are boot failures.
        """
        channel = LogChannel(
            log_level=config("notifications.channels.log.level", "info"),
        )
        notification_manager.add_channel(LogChannel.channel_name, channel)
