"""In-memory fake for the ``Event`` facade — records dispatched events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List, Optional


@dataclass
class DispatchedEvent:
    event: Any
    payload: Optional[dict] = None


class EventFake:
    def __init__(self) -> None:
        self.events: List[DispatchedEvent] = []

    # Production surface — accept the common names so any caller works.
    #
    # ``async def`` matches the real ``Event.fire`` / ``Event.dispatch``
    # signatures (commons/cara/cara/events/Event.py:204, :437).
    # Production code does ``await Event.fire(...)`` everywhere; if the
    # fake stayed sync, ``await EventFake.fire(...)`` would crash with
    # ``TypeError: 'NoneType' object is not awaitable``. The body is a
    # plain list append so it's still safe to ``asyncio.run`` in
    # synchronous test cases.
    async def dispatch(self, event: Any, payload: Optional[dict] = None) -> None:
        self.events.append(DispatchedEvent(event=event, payload=payload))

    async def fire(self, event: Any, payload: Optional[dict] = None) -> None:
        await self.dispatch(event, payload)

    async def emit(self, event: Any, payload: Optional[dict] = None) -> None:
        await self.dispatch(event, payload)

    def listen(self, *args: Any, **kwargs: Any) -> None:
        # No-op in tests — listeners aren't invoked under the fake.
        return None

    # ── Assertions ───────────────────────────────────────────────────

    def dispatched(self, of_type: type) -> List[DispatchedEvent]:
        return [e for e in self.events if isinstance(e.event, of_type)]

    def count(self, of_type: Optional[type] = None) -> int:
        if of_type is None:
            return len(self.events)
        return len(self.dispatched(of_type))

    def assert_dispatched(
        self,
        of_type: type,
        *,
        where: Optional[Callable[[DispatchedEvent], bool]] = None,
        times: Optional[int] = None,
    ) -> None:
        matches = self.dispatched(of_type)
        if where is not None:
            matches = [e for e in matches if where(e)]
        if times is not None and len(matches) != times:
            raise AssertionError(
                f"Expected {of_type.__name__} dispatched {times}x, got {len(matches)}"
            )
        if times is None and not matches:
            raise AssertionError(f"Expected {of_type.__name__} to be dispatched; none were")

    def assert_not_dispatched(self, of_type: type) -> None:
        matches = self.dispatched(of_type)
        if matches:
            raise AssertionError(
                f"Expected no {of_type.__name__} events, got {len(matches)}"
            )

    def assert_nothing_dispatched(self) -> None:
        if self.events:
            raise AssertionError(f"Expected no events, got {len(self.events)}")

    def clear(self) -> None:
        self.events.clear()
