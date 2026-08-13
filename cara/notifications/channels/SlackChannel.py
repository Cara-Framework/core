"""
Slack Channel for Cara Notifications.

This module provides Slack notification channel functionality,
sending notifications to Slack webhooks.
"""

from __future__ import annotations

import urllib.parse
import urllib.request
from typing import Any

from cara.exceptions import (
    CaraException,
    ConfigurationException,
    InvalidArgumentException,
)
from cara.notifications.channels.BaseChannel import BaseChannel
from cara.support import json_dumps


class SlackChannel(BaseChannel):
    """
    Slack channel for sending notifications to Slack webhooks.

    This channel sends notifications to Slack using webhook URLs.
    """

    channel_name = "slack"
    _MESSAGE_KEYS = frozenset(
        {"attachments", "blocks", "channel", "icon_emoji", "text", "username"}
    )

    def __init__(
        self,
        webhook_url: str,
        default_channel: str | None = None,
        username: str = "Cara Bot",
        icon: str = ":robot_face:",
    ):
        """
        Initialize Slack channel.

        Args:
            webhook_url: Slack webhook URL
            default_channel: Default channel to send to
            username: Bot username
            icon: Bot icon
        """
        self.webhook_url = self._validated_webhook(webhook_url)
        self.default_channel = self._optional_text(default_channel, "default_channel")
        self.username = self._required_text(username, "username")
        self.icon = self._required_text(icon, "icon")

    def send(self, notifiable, notification) -> bool:
        """
        Send the notification to Slack.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if sent successfully, False otherwise
        """
        renderer = getattr(notification, "to_slack", None)
        if not callable(renderer):
            raise InvalidArgumentException(
                "Slack notifications must implement to_slack()."
            )
        slack_message = renderer(notifiable)
        if slack_message is None:
            raise InvalidArgumentException(
                "A notification routed to Slack must render a Slack payload."
            )

        webhook_url = self._get_webhook_url(notifiable)
        payload = self._prepare_payload(slack_message)
        return self._send_to_slack(webhook_url, payload)

    def _get_webhook_url(self, notifiable) -> str:
        """
        Get the Slack webhook URL.

        Args:
            notifiable: The notifiable entity
        Returns:
            Validated webhook URL
        """
        route = getattr(notifiable, "route_notification_for", None)
        webhook = route("slack") if callable(route) else None
        return self._validated_webhook(webhook or self.webhook_url)

    def _prepare_payload(self, slack_message: dict[str, Any]) -> dict[str, Any]:
        """
        Prepare the Slack payload.

        Args:
            slack_message: Slack message data

        Returns:
            Slack payload
        """
        if isinstance(slack_message, str):
            # Simple text message
            return {
                "text": slack_message,
                "username": self.username,
                "icon_emoji": self.icon,
            }

        if isinstance(slack_message, dict):
            unknown = set(slack_message) - self._MESSAGE_KEYS
            if unknown:
                raise InvalidArgumentException(
                    "Unknown Slack notification fields: " + ", ".join(sorted(unknown))
                )
            # Rich message
            payload = {
                "username": self._required_text(
                    slack_message.get("username", self.username), "username"
                ),
                "icon_emoji": self._required_text(
                    slack_message.get("icon_emoji", self.icon), "icon_emoji"
                ),
            }

            # Add channel if specified
            if "channel" in slack_message:
                payload["channel"] = self._required_text(
                    slack_message["channel"], "channel"
                )
            elif self.default_channel:
                payload["channel"] = self.default_channel

            # Add text
            if "text" in slack_message:
                payload["text"] = self._required_text(slack_message["text"], "text")

            # Add attachments
            if "attachments" in slack_message:
                payload["attachments"] = self._required_list(
                    slack_message["attachments"], "attachments"
                )

            # Add blocks (for rich formatting)
            if "blocks" in slack_message:
                payload["blocks"] = self._required_list(slack_message["blocks"], "blocks")

            if not any(key in payload for key in ("attachments", "blocks", "text")):
                raise InvalidArgumentException(
                    "Slack payloads require text, attachments, or blocks."
                )

            return payload

        raise InvalidArgumentException(
            "Slack notifications must render a string or dictionary payload."
        )

    def _send_to_slack(self, webhook_url: str, payload: dict[str, Any]) -> bool:
        """
        Send payload to Slack webhook.

        Args:
            webhook_url: Slack webhook URL
            payload: Message payload

        Returns:
            True if sent successfully, False otherwise
        """
        data = json_dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url, data=data, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status != 200:
                raise CaraException(
                    f"Slack webhook returned unexpected HTTP {response.status}."
                )
        return True

    @staticmethod
    def _validated_webhook(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ConfigurationException(
                "Slack notification webhook must be a non-empty HTTPS URL."
            )
        candidate = value.strip()
        parsed = urllib.parse.urlsplit(candidate)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or bool(parsed.fragment)
        ):
            raise ConfigurationException(
                "Slack notification webhook must be an HTTPS URL without "
                "credentials or fragments."
            )
        return candidate

    @staticmethod
    def _required_text(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise InvalidArgumentException(
                f"Slack notification {field} must be a non-empty string."
            )
        return value.strip()

    @classmethod
    def _optional_text(cls, value: Any, field: str) -> str | None:
        if value is None:
            return None
        return cls._required_text(value, field)

    @staticmethod
    def _required_list(value: Any, field: str) -> list[Any]:
        if not isinstance(value, list) or not value:
            raise InvalidArgumentException(
                f"Slack notification {field} must be a non-empty list."
            )
        return value

    def format_simple_message(
        self, title: str, message: str, color: str = "good"
    ) -> dict[str, Any]:
        """
        Format a simple Slack message with attachment.

        Args:
            title: Message title
            message: Message text
            color: Attachment color (good, warning, danger, or hex)

        Returns:
            Formatted Slack message
        """
        return {
            "attachments": [
                {
                    "title": title,
                    "text": message,
                    "color": color,
                    "mrkdwn_in": ["text", "pretext"],
                }
            ]
        }

    def format_rich_message(self, blocks: list) -> dict[str, Any]:
        """
        Format a rich Slack message with blocks.

        Args:
            blocks: List of Slack block elements

        Returns:
            Formatted Slack message
        """
        return {"blocks": blocks}
