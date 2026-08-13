"""In-memory fake implementations of Cara facades for testing.

Each fake exposes:
- The methods the real facade exposes (so production code calls work
  unchanged), and
- Recording/assertion helpers (``assert_sent``, ``recorded``, ``count``).

Use via :class:`cara.testing.TestCase`'s ``fake_*`` helpers, or
directly via ``Mail.fake()`` once the testing module patches the
facade.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CacheFake": (".CacheFake", "CacheFake"),
    "DispatchedEvent": (".DispatchedEvent", "DispatchedEvent"),
    "EventFake": (".EventFake", "EventFake"),
    "LogFake": (".LogFake", "LogFake"),
    "LogRecord": (".LogRecord", "LogRecord"),
    "MailFake": (".MailFake", "MailFake"),
    "NotificationFake": (".NotificationFake", "NotificationFake"),
    "QueueFake": (".QueueFake", "QueueFake"),
    "QueuedJob": (".QueuedJob", "QueuedJob"),
    "SentMail": (".SentMail", "SentMail"),
    "SentNotification": (".SentNotification", "SentNotification"),
}

__all__ = [
    "CacheFake",
    "DispatchedEvent",
    "EventFake",
    "LogFake",
    "LogRecord",
    "MailFake",
    "NotificationFake",
    "QueueFake",
    "QueuedJob",
    "SentMail",
    "SentNotification",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
