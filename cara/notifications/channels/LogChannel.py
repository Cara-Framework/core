"""
Log Channel for Cara Notifications.

This module provides log notification channel functionality,
logging notifications instead of sending them, useful for debugging.
"""

import json
from datetime import datetime
from typing import Any, Dict

from cara.notifications.channels import BaseChannel


class LogChannel(BaseChannel):
    """
    Log channel for logging notifications instead of sending them.

    This channel is useful for development and debugging purposes.
    """

    channel_name = "log"

    def __init__(self, log_file: str = "notifications.log", log_level: str = "info"):
        """
        Initialize log channel.

        Args:
            log_file: Path to log file
            log_level: Log level
        """
        self.log_file = log_file
        self.log_level = log_level

    def send(self, notifiable, notification) -> bool:
        """
        Log the notification instead of sending it.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True (always successful for logging)
        """
        try:
            # Prepare log data
            log_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "notification_type": notification.__class__.__name__,
                "notifiable_type": notifiable.__class__.__name__,
                "notifiable_id": self._get_notifiable_id(notifiable),
                "channels": notification.via(notifiable),
                "data": notification.to_array(notifiable),
            }

            # Add specific channel data if available
            if hasattr(notification, "to_mail") and notification.to_mail(notifiable):
                log_data["mail_data"] = self._serialize_data(
                    notification.to_mail(notifiable)
                )

            if hasattr(notification, "to_database") and notification.to_database(
                notifiable
            ):
                log_data["database_data"] = notification.to_database(notifiable)

            if hasattr(notification, "to_slack") and notification.to_slack(notifiable):
                log_data["slack_data"] = notification.to_slack(notifiable)

            # Log the notification
            self._log_notification(log_data)

            return True

        except Exception as e:
            print(f"Log channel error: {e}")
            return True  # Don't fail for logging errors

    def _get_notifiable_id(self, notifiable) -> Any:
        """
        Get the notifiable entity ID.

        Args:
            notifiable: The notifiable entity

        Returns:
            Entity ID
        """
        if hasattr(notifiable, "get_notification_key"):
            return notifiable.get_notification_key()
        elif hasattr(notifiable, "id"):
            return notifiable.id
        elif hasattr(notifiable, "pk"):
            return notifiable.pk
        else:
            return id(notifiable)

    def _serialize_data(self, data: Any) -> Any:
        """
        Serialize data for logging.

        Args:
            data: Data to serialize

        Returns:
            Serialized data
        """
        if isinstance(data, (dict, list, str, int, float, bool)) or data is None:
            return data
        elif hasattr(data, "__dict__"):
            return data.__dict__
        else:
            return str(data)

    def _log_notification(self, log_data: Dict[str, Any]) -> None:
        """
        Log the notification data.

        Args:
            log_data: Data to log
        """
        try:
            log_message = f"[NOTIFICATION] {json.dumps(log_data, indent=2, default=str)}"

            # Try to use Cara's logging if available
            try:
                from cara.facades import Log

                Log.info(log_message)
            except ImportError:
                # Fallback to print/file logging
                print(log_message)

                # Also write to file if specified
                if self.log_file:
                    try:
                        with open(self.log_file, "a", encoding="utf-8") as f:
                            f.write(f"{log_message}\n")
                    except Exception:
                        pass  # Ignore file write errors

        except Exception as e:
            print(f"Log write error: {e}")

    def clear_log(self) -> bool:
        """
        Clear the notification log file.

        Returns:
            True if cleared successfully, False otherwise
        """
        try:
            if self.log_file:
                with open(self.log_file, "w", encoding="utf-8") as f:
                    f.write("")
                return True
            return False
        except Exception:
            return False

    def get_log_contents(self) -> str:
        """
        Get the contents of the log file.

        Returns:
            Log file contents
        """
        try:
            if self.log_file:
                with open(self.log_file, "r", encoding="utf-8") as f:
                    return f.read()
            return ""
        except Exception:
            return ""
