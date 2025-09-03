"""
Slack Logging Channel for the Cara framework.

This module provides a logging channel that sends log records to a Slack webhook.
"""

import json
from typing import Any, Dict

import requests


class SlackChannel:
    """Sends ERROR+ messages to a Slack channel via webhook."""

    def __init__(self, slack_cfg: Dict[str, Any], webhook_url: str) -> None:
        self._slack_cfg = slack_cfg
        self._webhook_url = webhook_url

    def write(self, message: Any) -> None:
        """
        Called by Loguru.

        Posts `message` to Slack via webhook.
        """
        payload = {
            "channel": self._slack_cfg.get("CHANNEL", "#errors"),
            "username": self._slack_cfg.get("USERNAME", "app-logger"),
            "icon_emoji": self._slack_cfg.get("ICON_EMOJI", ":warning:"),
            "text": message.strip(),
        }
        try:
            requests.post(
                self._webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=2,
            )
        except Exception:
            pass

    def flush(self) -> None:
        """No-op."""
        pass
