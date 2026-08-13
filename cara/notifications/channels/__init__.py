"""
Notification Channels for Cara Framework.

This module provides various notification channels for delivering notifications
through different mediums like email, database, Slack, etc.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseChannel": (".BaseChannel", "BaseChannel"),
    "DatabaseChannel": (".DatabaseChannel", "DatabaseChannel"),
    "LogChannel": (".LogChannel", "LogChannel"),
    "MailChannel": (".MailChannel", "MailChannel"),
    "SlackChannel": (".SlackChannel", "SlackChannel"),
}

__all__ = [
    "BaseChannel",
    "DatabaseChannel",
    "LogChannel",
    "MailChannel",
    "SlackChannel",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
