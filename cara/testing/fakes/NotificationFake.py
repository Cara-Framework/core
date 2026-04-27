"""In-memory fake for the ``Notification`` facade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Union


@dataclass
class SentNotification:
    notifiable: Any
    notification: Any
    channels: Optional[List[str]] = None


class NotificationFake:
    def __init__(self) -> None:
        self.sent: List[SentNotification] = []

    # Production-side surface
    def send(
        self,
        notifiable: Union[Any, Iterable[Any]],
        notification: Any,
        channels: Optional[List[str]] = None,
    ) -> None:
        targets = (
            list(notifiable)
            if isinstance(notifiable, (list, tuple, set))
            else [notifiable]
        )
        for target in targets:
            self.sent.append(
                SentNotification(
                    notifiable=target, notification=notification, channels=channels
                )
            )

    def route(self, *args: Any, **kwargs: Any) -> "NotificationFake":
        # ``Notification.route('mail', 'foo@x').notify(...)`` — return self
        # so chained ``.notify`` lands here.
        return self

    def notify(self, notification: Any) -> None:
        self.sent.append(SentNotification(notifiable=None, notification=notification))

    # ── Assertions ───────────────────────────────────────────────────

    def all(self) -> List[SentNotification]:
        return list(self.sent)

    def count(self, of_type: Optional[type] = None) -> int:
        if of_type is None:
            return len(self.sent)
        return len([n for n in self.sent if isinstance(n.notification, of_type)])

    def assert_sent_to(
        self,
        notifiable: Any,
        of_type: type,
        *,
        where: Optional[Callable[[SentNotification], bool]] = None,
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

    def assert_sent(self, of_type: type, times: Optional[int] = None) -> None:
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
