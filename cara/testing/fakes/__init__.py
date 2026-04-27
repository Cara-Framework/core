"""In-memory fake implementations of Cara facades for testing.

Each fake exposes:
- The methods the real facade exposes (so production code calls work
  unchanged), and
- Recording/assertion helpers (``assert_sent``, ``recorded``, ``count``).

Use via :class:`cara.testing.TestCase`'s ``fake_*`` helpers, or
directly via ``Mail.fake()`` once the testing module patches the
facade.
"""

from .LogFake import LogFake
from .MailFake import MailFake
from .QueueFake import QueueFake
from .EventFake import EventFake
from .CacheFake import CacheFake
from .NotificationFake import NotificationFake

__all__ = [
    "LogFake",
    "MailFake",
    "QueueFake",
    "EventFake",
    "CacheFake",
    "NotificationFake",
]
