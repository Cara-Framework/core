"""
Log Channel for Cara Notifications.

This module provides log notification channel functionality,
logging notifications instead of sending them, useful for debugging.
"""

from __future__ import annotations

from typing import Any

import pendulum

from cara.exceptions import ConfigurationException
from cara.facades import Log
from cara.notifications.channels.BaseChannel import BaseChannel
from cara.support import json_dumps


class LogChannel(BaseChannel):
    """
    Log channel for logging notifications instead of sending them.

    This channel is useful for development and debugging purposes.
    """

    channel_name = "log"
    _LEVELS = frozenset({"critical", "debug", "error", "info", "warning"})

    def __init__(self, log_level: str = "info"):
        """
        Initialize log channel.

        Args:
            log_level: Log level
        """
        if not isinstance(log_level, str) or log_level not in self._LEVELS:
            raise ConfigurationException(
                "Notification log level must be one of: "
                + ", ".join(sorted(self._LEVELS))
            )
        self.log_level = log_level

    def send(self, notifiable, notification) -> bool:
        """
        Log the notification instead of sending it.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True after the logger accepts the record
        """
        renderer = getattr(notification, "to_log", None)
        data = renderer(notifiable) if callable(renderer) else None
        if data is None:
            fallback = getattr(notification, "to_array", None)
            if not callable(fallback):
                raise ConfigurationException(
                    "Log notifications must implement to_log() or to_array()."
                )
            data = fallback(notifiable)
        if not isinstance(data, dict):
            raise ConfigurationException(
                "Log notifications must render a dictionary payload."
            )

        log_data: dict[str, Any] = {
            "timestamp": pendulum.now("UTC").isoformat(),
            "notification_type": type(notification).__name__,
            "notifiable_type": self.notifiable_type(notifiable),
            "notifiable_id": self.notifiable_id(notifiable),
            "data": data,
        }
        writer = getattr(Log, self.log_level)
        writer(
            "[NOTIFICATION] %s",
            json_dumps(log_data, sort_keys=True),
            category="cara.notifications.log",
        )
        return True
