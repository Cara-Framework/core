"""
Slack Channel for Cara Notifications.

This module provides Slack notification channel functionality,
sending notifications to Slack webhooks.
"""

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional

from cara.notifications.channels import BaseChannel


class SlackChannel(BaseChannel):
    """
    Slack channel for sending notifications to Slack webhooks.

    This channel sends notifications to Slack using webhook URLs.
    """

    channel_name = "slack"

    def __init__(
        self,
        webhook_url: str,
        default_channel: str = None,
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
        self.webhook_url = webhook_url
        self.default_channel = default_channel
        self.username = username
        self.icon = icon

    def send(self, notifiable, notification) -> bool:
        """
        Send the notification to Slack.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Get the Slack representation
            slack_message = notification.to_slack(notifiable)

            if slack_message is None:
                return False

            # Get the webhook URL
            webhook_url = self._get_webhook_url(notifiable, slack_message)
            if not webhook_url:
                return False

            # Prepare the payload
            payload = self._prepare_payload(slack_message)

            # Send to Slack
            return self._send_to_slack(webhook_url, payload)

        except Exception as e:
            # Log error
            print(f"Slack channel error: {e}")
            return False

    def _get_webhook_url(
        self, notifiable, slack_message: Dict[str, Any]
    ) -> Optional[str]:
        """
        Get the Slack webhook URL.

        Args:
            notifiable: The notifiable entity
            slack_message: Slack message data

        Returns:
            Webhook URL or None
        """
        # Check if webhook URL is in the message
        if isinstance(slack_message, dict) and "webhook_url" in slack_message:
            return slack_message["webhook_url"]

        # Try to get routing information from notifiable
        if hasattr(notifiable, "route_notification_for"):
            webhook = notifiable.route_notification_for("slack")
            if webhook:
                return webhook

        # Fallback to configured webhook
        return self.webhook_url

    def _prepare_payload(self, slack_message: Dict[str, Any]) -> Dict[str, Any]:
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

        elif isinstance(slack_message, dict):
            # Rich message
            payload = {
                "username": slack_message.get("username", self.username),
                "icon_emoji": slack_message.get("icon_emoji", self.icon),
            }

            # Add channel if specified
            if "channel" in slack_message:
                payload["channel"] = slack_message["channel"]
            elif self.default_channel:
                payload["channel"] = self.default_channel

            # Add text
            if "text" in slack_message:
                payload["text"] = slack_message["text"]

            # Add attachments
            if "attachments" in slack_message:
                payload["attachments"] = slack_message["attachments"]

            # Add blocks (for rich formatting)
            if "blocks" in slack_message:
                payload["blocks"] = slack_message["blocks"]

            return payload

        return {"text": str(slack_message)}

    def _send_to_slack(self, webhook_url: str, payload: Dict[str, Any]) -> bool:
        """
        Send payload to Slack webhook.

        Args:
            webhook_url: Slack webhook URL
            payload: Message payload

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Prepare request data
            data = json.dumps(payload).encode("utf-8")

            # Create request
            req = urllib.request.Request(
                webhook_url, data=data, headers={"Content-Type": "application/json"}
            )

            # Send request
            with urllib.request.urlopen(req) as response:
                return response.status == 200

        except Exception as e:
            print(f"Slack send error: {e}")
            return False

    def format_simple_message(
        self, title: str, message: str, color: str = "good"
    ) -> Dict[str, Any]:
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

    def format_rich_message(self, blocks: list) -> Dict[str, Any]:
        """
        Format a rich Slack message with blocks.

        Args:
            blocks: List of Slack block elements

        Returns:
            Formatted Slack message
        """
        return {"blocks": blocks}
