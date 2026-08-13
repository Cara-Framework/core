"""Notifications — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseChannel": (".channels", "BaseChannel"),
    "BaseNotification": (".BaseNotification", "BaseNotification"),
    "DatabaseChannel": (".channels", "DatabaseChannel"),
    "LogChannel": (".channels", "LogChannel"),
    "MailChannel": (".channels", "MailChannel"),
    "Notifiable": (".Notifiable", "Notifiable"),
    "NotifiableContract": (".contracts", "NotifiableContract"),
    "Notification": (".Notification", "Notification"),
    "NotificationChannel": (".contracts", "NotificationChannel"),
    "NotificationProvider": (".NotificationProvider", "NotificationProvider"),
    "SendNotificationJob": (".jobs", "SendNotificationJob"),
    "SlackChannel": (".channels", "SlackChannel"),
    "matches": (".UnsubscribeToken", "matches"),
    "mint": (".UnsubscribeToken", "mint"),
}

__all__ = [
    "BaseChannel",
    "BaseNotification",
    "DatabaseChannel",
    "LogChannel",
    "MailChannel",
    "Notifiable",
    "NotifiableContract",
    "Notification",
    "NotificationChannel",
    "NotificationProvider",
    "SendNotificationJob",
    "SlackChannel",
    "matches",
    "mint",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
