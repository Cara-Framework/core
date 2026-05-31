"""
Notification Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the notification
subsystem, including mail, database, slack and log notification channels.
"""

from cara.configuration import config
from cara.facades import Log
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
        default_channels = config("notifications.default", ["mail", "database"])
        notification_manager = Notification(self.application, default_channels)

        self._add_mail_channel(notification_manager)
        self._add_database_channel(notification_manager)
        self._add_slack_channel(notification_manager)
        self._add_log_channel(notification_manager)

        self.application.bind("notification", notification_manager)

    def _add_mail_channel(self, notification_manager: Notification) -> None:
        """Register mail notification channel with configuration."""
        try:
            mail_manager = self.application.make("mail")
            channel = MailChannel(
                mail_manager=mail_manager,
                from_address=config("notifications.channels.mail.from_address"),
                from_name=config("notifications.channels.mail.from_name"),
                reply_to=config("notifications.channels.mail.reply_to"),
            )
            notification_manager.add_channel(MailChannel.channel_name, channel)
        except Exception as e:
            Log.warning(f"[NotificationProvider] Mail channel registration failed: {e}")

    def _add_database_channel(self, notification_manager: Notification) -> None:
        """Register database notification channel with configuration.

        Wrapped in try/except for parity with ``_add_mail_channel``
        and the slack/log siblings. Pre-fix this method called
        ``self.application.make("DB").query()`` bare — when the DB
        provider hadn't booted (rare ordering edge case during
        boot-time failure recovery, or a test harness that swaps
        providers in custom orders) the missing binding raised
        ``MissingContainerBindingException`` and aborted the whole
        notification subsystem registration. The user-visible
        symptom was that mail/slack/log channels ALSO got skipped
        because of the early raise — one missing dependency turned
        into total notification blackout. Logged at warning level
        so ops sees the binding gap; the rest of the channels
        register normally.
        """
        try:
            query_builder = self.application.make("DB").query()
            channel = DatabaseChannel(
                database_manager=query_builder,
                table_name=config(
                    "notifications.channels.database.table", "notifications",
                ),
            )
            notification_manager.add_channel(DatabaseChannel.channel_name, channel)
        except Exception as e:
            Log.warning(
                f"[NotificationProvider] Database channel registration "
                f"failed: {e}",
            )

    def _add_slack_channel(self, notification_manager: Notification) -> None:
        """Register Slack notification channel with configuration.

        Wrapped for parity with the mail/database siblings — see
        ``_add_database_channel`` for the channel-isolation rationale.
        ``SlackChannel.__init__`` can raise on a malformed webhook,
        and ``add_channel`` can raise on a name collision; without
        the wrap, either would abort ``register()`` and silently skip
        the ``_add_log_channel`` registration below.
        """
        try:
            webhook_url = config("notifications.channels.slack.webhook_url")
            if not webhook_url:
                return

            channel = SlackChannel(
                webhook_url=webhook_url,
                default_channel=config("notifications.channels.slack.channel"),
                username=config("notifications.channels.slack.username", "Cara Bot"),
                icon=config("notifications.channels.slack.icon", ":robot_face:"),
            )
            notification_manager.add_channel(SlackChannel.channel_name, channel)
        except Exception as e:
            Log.warning(
                f"[NotificationProvider] Slack channel registration "
                f"failed: {e}",
            )

    def _add_log_channel(self, notification_manager: Notification) -> None:
        """Register log notification channel with configuration.

        Wrapped for parity — see ``_add_database_channel``. The log
        channel is the smallest dependency footprint of the four,
        but ``add_channel`` can still raise on a name collision and
        a config() call can still raise during boot-time recovery.
        Fail-soft so an upstream sibling's failure doesn't drop
        this one too.
        """
        try:
            channel = LogChannel(
                log_file=config(
                    "notifications.channels.log.file", "notifications.log",
                ),
                log_level=config("notifications.channels.log.level", "info"),
            )
            notification_manager.add_channel(LogChannel.channel_name, channel)
        except Exception as e:
            Log.warning(
                f"[NotificationProvider] Log channel registration "
                f"failed: {e}",
            )
