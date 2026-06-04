"""Round-2 dispatcher contracts: dedup, unsubscribe, queue-path
validate_payload gate.

The first dispatcher audit (test_event_dispatcher) covered
registration order, stop-propagation, and the
``validate_payload``-skip path for malformed events. This file
pins three CONCRETE gaps the second audit surfaced:

1. **Duplicate listener guard** — pre-fix the same listener
   instance subscribed twice fired twice per event. The dedup is
   identity-based (``is``, not ``==``) so a listener class with a
   custom ``__eq__`` doesn't accidentally collapse two distinct
   instances into one bucket entry.

2. **unsubscribe()** — pre-fix the dispatcher was append-only.
   Tests setting up per-case listeners had to bypass the API; a
   request-scoped listener could not detach itself at request
   end. The new method removes by identity, drops the empty
   bucket entry so ``has_listeners`` reads cleanly, and returns
   ``False`` on a no-op rather than raising.

3. **HandleListenerJob.validate_payload after rehydrate** —
   pre-fix the queue path skipped the gate the sync dispatcher
   applies at fire time. A payload the sync dispatcher would
   have rejected silently round-tripped through serialization
   and reached the listener with missing required fields. The
   gap was worse than the sync path because the error surfaced
   at the listener's first deref (hours later, in a worker log)
   instead of at the dispatch site. The job now refuses to
   invoke the listener and raises ``ValueError`` so the queue
   runner's retry+DLQ path owns recovery.
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.events.Event import Event as EventDispatcher
from cara.events.contracts import Listener
from cara.events.jobs.HandleListenerJob import _instantiate_event


# ── Fixtures ────────────────────────────────────────────────────────


class _RecordingListener(Listener):
    """Counts how many times ``handle`` was called."""

    def __init__(self) -> None:
        self.calls = 0

    def handle(self, event: Any) -> None:
        self.calls += 1


@pytest.fixture
def dispatcher() -> EventDispatcher:
    return EventDispatcher()


# ── Duplicate listener guard ────────────────────────────────────────


class TestSubscribeDeduplication:
    def test_same_listener_subscribed_twice_only_invoked_once(
        self,
        dispatcher: EventDispatcher,
    ):
        listener = _RecordingListener()
        dispatcher.subscribe("user.registered", listener)
        dispatcher.subscribe("user.registered", listener)

        bucket = dispatcher._listeners["user.registered"]
        assert len(bucket) == 1, (
            f"Double-subscribe of the same listener instance must "
            f"collapse to one bucket entry; got {len(bucket)}. The "
            f"silent doubling shape is a real bug class — every "
            f"double-subscribe sends two notifications / two DB "
            f"writes / two job dispatches per event."
        )

    def test_dedup_is_by_identity_not_equality(
        self,
        dispatcher: EventDispatcher,
    ):
        # A listener class with custom ``__eq__`` (e.g. tagging two
        # listeners "equal" because they target the same channel)
        # must NOT collapse two distinct instances. Identity is the
        # contract — eq could legitimately mean "same target" while
        # the consumer wants both instances registered for fan-out.
        class _EqListener(Listener):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            def __eq__(self, other: object) -> bool:
                # Pathological: every instance equal to every other.
                return isinstance(other, _EqListener)

            def __hash__(self) -> int:
                return 0  # Required when __eq__ overridden

            def handle(self, event: Any) -> None: ...

        a = _EqListener("a")
        b = _EqListener("b")
        dispatcher.subscribe("x.y", a)
        dispatcher.subscribe("x.y", b)

        bucket = dispatcher._listeners["x.y"]
        assert len(bucket) == 2, (
            f"Identity comparison MUST allow two distinct instances "
            f"of an __eq__-overriding listener to register. Got "
            f"{len(bucket)} — dedup is collapsing on equality, "
            f"breaking legitimate fan-out registrations."
        )

    def test_wildcard_double_subscribe_also_deduplicated(
        self,
        dispatcher: EventDispatcher,
    ):
        listener = _RecordingListener()
        dispatcher.subscribe("user.*", listener)
        dispatcher.subscribe("user.*", listener)

        bucket = dispatcher._wildcard_listeners["user.*"]
        assert len(bucket) == 1, (
            f"Wildcard-bucket dedup must mirror the direct-bucket "
            f"path; got {len(bucket)} entries."
        )


# ── unsubscribe() ───────────────────────────────────────────────────


class TestUnsubscribe:
    def test_removes_subscribed_listener_by_identity(
        self,
        dispatcher: EventDispatcher,
    ):
        listener = _RecordingListener()
        dispatcher.subscribe("evt", listener)
        assert dispatcher.has_listeners("evt") is True

        removed = dispatcher.unsubscribe("evt", listener)
        assert removed is True
        assert dispatcher.has_listeners("evt") is False, (
            "Removing the only listener must drop the bucket entry "
            "so has_listeners() returns False — leaving an empty "
            "list around is a footgun for debug logs that read the "
            "bucket count as 'subscribed'."
        )

    def test_returns_false_on_no_op_remove(self, dispatcher: EventDispatcher):
        # Removing a listener that was never subscribed must NOT
        # raise (caller may be in a cleanup hook that doesn't know
        # whether subscribe ran). Returns False so the caller can
        # branch if it cares.
        listener = _RecordingListener()
        assert dispatcher.unsubscribe("never.subscribed", listener) is False

    def test_unsubscribe_one_keeps_siblings(self, dispatcher: EventDispatcher):
        a = _RecordingListener()
        b = _RecordingListener()
        dispatcher.subscribe("evt", a)
        dispatcher.subscribe("evt", b)

        assert dispatcher.unsubscribe("evt", a) is True
        bucket = dispatcher._listeners["evt"]
        assert len(bucket) == 1
        assert bucket[0] is b, "Sibling listener must survive the targeted remove"

    def test_unsubscribe_supports_wildcard_buckets(
        self,
        dispatcher: EventDispatcher,
    ):
        listener = _RecordingListener()
        dispatcher.subscribe("user.*", listener)
        assert dispatcher.unsubscribe("user.*", listener) is True
        assert "user.*" not in dispatcher._wildcard_listeners

    def test_unsubscribe_pattern_does_not_walk_buckets(
        self,
        dispatcher: EventDispatcher,
    ):
        # Contract: unsubscribe(pattern) MUST match the exact pattern
        # the consumer subscribed with — it does NOT walk wildcards
        # looking for the listener elsewhere. Documents the boundary
        # so a future "smart matcher" refactor is a deliberate
        # change, not a slip.
        listener = _RecordingListener()
        dispatcher.subscribe("user.registered", listener)
        # Trying to remove via a wildcard that WOULD have matched on
        # dispatch must NOT find the direct registration.
        assert dispatcher.unsubscribe("user.*", listener) is False
        assert dispatcher.has_listeners("user.registered") is True


# ── HandleListenerJob queue-path validation gate ────────────────────


class _ValidatedEvent:
    """Stand-in event class shipping the same ``validate_payload``
    contract the in-process dispatcher's gate expects."""

    REQUIRED_FIELDS = ("user_id", "email")

    def __init__(self, user_id: int | None = None, email: str | None = None):
        self.user_id = user_id
        self.email = email

    @property
    def name(self) -> str:
        return "validated.event"

    def validate_payload(self) -> list[str]:
        missing = []
        for f in self.REQUIRED_FIELDS:
            if getattr(self, f, None) in (None, "", []):
                missing.append(f)
        return missing


class TestHandleListenerJobRehydrateValidation:
    def test_payload_with_required_fields_passes(self):
        # Sanity: the happy path still works — well-formed payload
        # rehydrates and returns an event the listener can use.
        event = _instantiate_event(
            _ValidatedEvent,
            {"user_id": 42, "email": "a@b.com"},
        )
        assert event.user_id == 42
        assert event.email == "a@b.com"

    def test_payload_with_missing_required_field_refuses(self):
        # Pre-fix the queue path skipped validate_payload entirely
        # and handed a half-populated event to the listener; the
        # listener crashed at first attribute deref, hours later in
        # the worker log with no link to the originating fire. Now
        # the job refuses with a ValueError naming the missing
        # fields so the queue runner's retry+DLQ path can own
        # recovery.
        with pytest.raises(ValueError, match="missing/invalid fields"):
            _instantiate_event(
                _ValidatedEvent,
                {"user_id": 42},  # email missing
            )

    def test_validator_raising_is_surfaced_with_event_class_name(self):
        # If the validator itself raises (a typo'd attribute access,
        # an import that failed), surface the exception WITH the
        # event class name so the worker log points at the bug. Pre-
        # fix this would bubble the raw exception with no anchor to
        # the originating event type.
        class _ExplodingEvent:
            def validate_payload(self) -> Any:
                raise RuntimeError("validator bug")

        with pytest.raises(ValueError, match="_ExplodingEvent"):
            _instantiate_event(_ExplodingEvent, {})

    def test_event_without_validate_payload_method_is_fine(self):
        # Validate-payload is OPT-IN — events that don't ship the
        # method must round-trip unchanged. The pre-fix path didn't
        # call validate_payload at all; the new path must continue
        # to no-op when the hook is absent.
        class _PlainEvent:
            def __init__(self, x: int) -> None:
                self.x = x

        event = _instantiate_event(_PlainEvent, {"x": 99})
        assert event.x == 99
