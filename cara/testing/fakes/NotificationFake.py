"""In-memory fake for the ``Notification`` facade."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .SentNotification import SentNotification


class NotificationFake:
    def __init__(self) -> None:
        self.sent: list[SentNotification] = []

    # Production-side surface — every method that the real
    # ``Notification`` facade exposes (commons/cara/cara/notifications/Notification.py)
    # MUST exist here too, otherwise production code that calls the
    # missing method gets ``AttributeError`` only in tests. Real
    # methods all return ``bool`` — the fake mirrors that.
    #
    # The reverse holds too: the fake answers NOTHING the real facade does
    # not. It used to take ``send(..., channels=[...])`` and expose
    # ``route(...)`` / ``notify(...)``, none of which ``Notification`` has
    # (channels come from the notification's own ``via()``), so a call spelled
    # that way passed its test and raised on the first real send — the
    # ``Mail.raw`` outage in another facade (see ``MailFake``).
    def send(self, notifiable: Any, notification: Any) -> bool:
        # ONE notifiable, recorded as given. ``Notification.send`` does not fan
        # a list out (it hands the list to ``via()`` and the channels as one
        # notifiable), so the fake no longer does either: a test that passed a
        # list used to see one send per element and pass, while production
        # delivered to none of them.
        self.sent.append(
            SentNotification(notifiable=notifiable, notification=notification)
        )
        return True

    def send_now(self, notifiable: Any, notification: Any) -> bool:
        """Mirror ``Notification.send_now`` — sync delivery path.

        Production callers (``NotificationDeliveryService``) reach for
        ``send_now`` on critical alerts that mustn't sit in the queue;
        the previous fake omitted this and any test that hit that
        path crashed with AttributeError.
        """
        self.sent.append(
            SentNotification(notifiable=notifiable, notification=notification)
        )
        return True

    def send_delayed(
        self, notifiable: Any, notification: Any, delay_seconds: int
    ) -> bool:
        """Mirror ``Notification.send_delayed`` — queued delivery."""
        self.sent.append(
            SentNotification(notifiable=notifiable, notification=notification)
        )
        return True

    def channel(self, channel_name: str) -> NotificationFake:
        """Mirror ``Notification.channel(name)`` — returns a channel.

        Real implementation returns a ``NotificationChannel``; the
        fake returns self so a chained ``Notification.channel('mail').send(...)``
        round-trips through the same recorder.
        """
        return self

    # ── Assertions ───────────────────────────────────────────────────

    def all(self) -> list[SentNotification]:
        return list(self.sent)

    def count(self, of_type: type | None = None) -> int:
        if of_type is None:
            return len(self.sent)
        return len([n for n in self.sent if isinstance(n.notification, of_type)])

    def assert_sent_to(
        self,
        notifiable: Any,
        of_type: type,
        *,
        where: Callable[[SentNotification], bool] | None = None,
    ) -> None:
        matches = [
            n
            for n in self.sent
            if n.notifiable == notifiable and isinstance(n.notification, of_type)
        ]
        if where is not None:
            matches = [n for n in matches if where(n)]
        if not matches:
            raise AssertionError(
                f"Expected {of_type.__name__} notification to {notifiable!r}; none matched"
            )

    def assert_sent(self, of_type: type, times: int | None = None) -> None:
        matches = [n for n in self.sent if isinstance(n.notification, of_type)]
        if times is not None and len(matches) != times:
            raise AssertionError(
                f"Expected {of_type.__name__} sent {times}x, got {len(matches)}"
            )
        if times is None and not matches:
            raise AssertionError(f"Expected {of_type.__name__} to be sent; none matched")

    def assert_nothing_sent(self) -> None:
        if self.sent:
            raise AssertionError(f"Expected no notifications, got {len(self.sent)}")

    def clear(self) -> None:
        self.sent.clear()
