import pytest
from unittest.mock import patch, MagicMock

from cara.events.Event import Event as EventDispatcher, EventSubscriber
from cara.events.UserRegisteredEvent import UserRegisteredEvent


class SimpleListener:
    def __init__(self, log):
        self.log = log

    def handle(self, event):
        self.log.append("simple")


class OrderedListener:
    def __init__(self, tag, log):
        self.tag = tag
        self.log = log

    def handle(self, event):
        self.log.append(self.tag)


class StoppingListener:
    def __init__(self, log):
        self.log = log

    def handle(self, event):
        self.log.append("stopper")
        event.stop_propagation()


class AfterStopListener:
    def __init__(self, log):
        self.log = log

    def handle(self, event):
        self.log.append("after-stop")


@pytest.fixture
def dispatcher():
    return EventDispatcher()


# --- Registration and dispatching ---

@pytest.mark.asyncio
async def test_subscribe_and_dispatch(dispatcher):
    log = []
    listener = SimpleListener(log)
    dispatcher.subscribe("user.registered", listener)

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_listen_with_callback(dispatcher):
    log = []
    dispatcher.listen("user.registered", lambda e: log.append(e.email))

    event = UserRegisteredEvent(user_id=1, email="test@example.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["test@example.com"]


@pytest.mark.asyncio
async def test_dispatch_no_listeners_is_noop(dispatcher):
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)  # should not raise


# --- Listener ordering ---

@pytest.mark.asyncio
async def test_listeners_called_in_registration_order(dispatcher):
    log = []
    dispatcher.subscribe("order.test", OrderedListener("first", log))
    dispatcher.subscribe("order.test", OrderedListener("second", log))
    dispatcher.subscribe("order.test", OrderedListener("third", log))

    event = MagicMock()
    event.name = "order.test"
    event.is_propagation_stopped = False

    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["first", "second", "third"]


# --- Stop propagation ---

@pytest.mark.asyncio
async def test_stop_propagation(dispatcher):
    log = []
    dispatcher.subscribe("user.registered", OrderedListener("before", log))
    dispatcher.subscribe("user.registered", StoppingListener(log))
    dispatcher.subscribe("user.registered", AfterStopListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert "before" in log
    assert "stopper" in log
    assert "after-stop" not in log


# --- Wildcard listeners ---

@pytest.mark.asyncio
async def test_wildcard_trailing(dispatcher):
    log = []
    dispatcher.subscribe("user.*", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_leading(dispatcher):
    log = []
    dispatcher.subscribe("*.registered", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_does_not_match_unrelated(dispatcher):
    log = []
    dispatcher.subscribe("order.*", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == []


# --- has_listeners ---

def test_has_listeners_true(dispatcher):
    log = []
    dispatcher.subscribe("user.registered", SimpleListener(log))
    assert dispatcher.has_listeners("user.registered")


def test_has_listeners_false(dispatcher):
    assert not dispatcher.has_listeners("nonexistent")


def test_has_listeners_wildcard(dispatcher):
    log = []
    dispatcher.subscribe("user.*", SimpleListener(log))
    assert dispatcher.has_listeners("user.registered")


# --- EventSubscriber ---

@pytest.mark.asyncio
async def test_event_subscriber(dispatcher):
    log = []

    class TestSubscriber(EventSubscriber):
        def subscribe(self, d):
            d.listen("user.registered", lambda e: log.append("sub-registered"))
            d.listen("user.deleted", lambda e: log.append("sub-deleted"))

    dispatcher.subscribe(TestSubscriber)

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["sub-registered"]


# --- UserRegisteredEvent ---

def test_user_registered_event_payload():
    event = UserRegisteredEvent(user_id=42, email="alice@example.com", role="admin")
    payload = event.payload()
    assert payload["user_id"] == 42
    assert payload["email"] == "alice@example.com"
    assert payload["role"] == "admin"


def test_user_registered_event_name():
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    assert event.name == "user.registered"


def test_user_registered_event_propagation():
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    assert not event.is_propagation_stopped
    event.stop_propagation()
    assert event.is_propagation_stopped


def test_user_registered_event_to_dict():
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    assert event.to_dict() == {"user_id": 1, "email": "a@b.com"}


# --- Listener dedup: direct + wildcard match ---


@pytest.mark.asyncio
async def test_listener_subscribed_to_direct_and_matching_wildcard_fires_once(
    dispatcher,
):
    """A single listener instance subscribed to both ``"user.registered"``
    and the matching wildcard ``"user.*"`` must fire exactly once per
    dispatch. The dispatcher dedups by listener identity before
    invoking, so adding a wildcard fallback for the same listener
    cannot accidentally double-trigger side effects (re-indexing,
    webhooks, notifications).
    """
    log = []
    listener = SimpleListener(log)
    dispatcher.subscribe("user.registered", listener)
    dispatcher.subscribe("user.*", listener)

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_distinct_listeners_with_overlapping_subscriptions_all_fire(dispatcher):
    """Dedup must key on identity, not class — two separate instances
    of the same listener class (e.g. one for direct, one for wildcard)
    both fire."""
    log = []
    a = OrderedListener("direct", log)
    b = OrderedListener("wildcard", log)
    dispatcher.subscribe("user.registered", a)
    dispatcher.subscribe("user.*", b)

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["direct", "wildcard"]


# --- Cycle detection ---


@pytest.mark.asyncio
async def test_self_dispatch_inside_listener_raises_cycle_exception(dispatcher):
    """A listener that re-dispatches the event it is handling must
    raise ``EventDispatchCycleException`` instead of recursing until
    the Python stack overflows."""
    from cara.exceptions import EventDispatchCycleException

    invocations = []

    class SelfDispatchingListener:
        propagate_failures = True  # surface the cycle error to the caller

        async def handle(self, event):
            invocations.append(1)
            await dispatcher.dispatch(event)

    dispatcher.subscribe("user.registered", SelfDispatchingListener())
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with (
        patch(
            "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
        ),
        pytest.raises(EventDispatchCycleException),
    ):
        await dispatcher.dispatch(event)

    assert len(invocations) == 1


@pytest.mark.asyncio
async def test_transitive_cycle_raises_with_full_chain(dispatcher):
    """A.handle dispatches B, B.handle dispatches A — the cycle must
    be detected on the second A dispatch, and the exception message
    must include the chain so the responsible listener is locatable."""
    from cara.exceptions import EventDispatchCycleException

    class EventA:
        name = "topic.a"

    class EventB:
        name = "topic.b"

    class ListenerA:
        propagate_failures = True

        async def handle(self, event):
            await dispatcher.dispatch(EventB())

    class ListenerB:
        propagate_failures = True

        async def handle(self, event):
            await dispatcher.dispatch(EventA())

    dispatcher.subscribe("topic.a", ListenerA())
    dispatcher.subscribe("topic.b", ListenerB())

    with (
        patch(
            "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
        ),
        pytest.raises(EventDispatchCycleException) as exc_info,
    ):
        await dispatcher.dispatch(EventA())

    msg = str(exc_info.value)
    assert "topic.a" in msg
    assert "topic.b" in msg
    assert "->" in msg


@pytest.mark.asyncio
async def test_sequential_dispatches_of_same_event_do_not_trip_cycle_guard(dispatcher):
    """The dispatch stack must be popped when dispatch returns —
    otherwise two back-to-back dispatches of the same event from the
    same task would falsely report a cycle on the second call."""
    log = []
    dispatcher.subscribe("user.registered", SimpleListener(log))

    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(UserRegisteredEvent(user_id=1, email="a@b.com"))
        await dispatcher.dispatch(UserRegisteredEvent(user_id=2, email="b@c.com"))

    assert log == ["simple", "simple"]


@pytest.mark.asyncio
async def test_listener_chain_to_distinct_event_does_not_trip_cycle_guard(dispatcher):
    """A → B is the normal cascade pattern across the pipeline and
    must not be mistaken for a cycle."""

    class EventA:
        name = "topic.a"

    class EventB:
        name = "topic.b"

    log = []

    class AHandler:
        propagate_failures = True

        async def handle(self, event):
            log.append("a")
            await dispatcher.dispatch(EventB())

    class BHandler:
        propagate_failures = True

        def handle(self, event):
            log.append("b")

    dispatcher.subscribe("topic.a", AHandler())
    dispatcher.subscribe("topic.b", BHandler())

    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(EventA())

    assert log == ["a", "b"]


@pytest.mark.asyncio
async def test_concurrent_dispatches_have_independent_cycle_stacks():
    """Two asyncio tasks each dispatching the same event concurrently
    must not see each other's stack — the cycle guard is per-task
    (ContextVar), not global."""
    dispatcher = EventDispatcher()
    log = []

    class SlowListener:
        async def handle(self, event):
            log.append(("start", event.user_id))
            import asyncio as _a

            await _a.sleep(0)
            log.append(("done", event.user_id))

    dispatcher.subscribe("user.registered", SlowListener())

    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        import asyncio as _a

        await _a.gather(
            dispatcher.dispatch(UserRegisteredEvent(user_id=1, email="a@b.com")),
            dispatcher.dispatch(UserRegisteredEvent(user_id=2, email="b@c.com")),
        )

    # Both completed without raising — independent stacks.
    completions = sorted(uid for tag, uid in log if tag == "done")
    assert completions == [1, 2]


# --- Concurrent subscribe + dispatch ---


@pytest.mark.asyncio
async def test_concurrent_subscribe_during_dispatch_does_not_raise(dispatcher):
    """A subscribe() from another thread while dispatch() iterates
    wildcard listeners must not raise
    ``RuntimeError: dictionary changed size during iteration`` —
    dispatch snapshots both buckets under the lock first.

    Pre-fix this test fails intermittently with a RuntimeError on the
    background thread; after the fix the snapshot under ``_lock``
    makes both buckets observation-safe.
    """
    import threading

    log = []
    dispatcher.subscribe("user.*", SimpleListener(log))

    errors: list[BaseException] = []
    barrier = threading.Barrier(2)
    iterations = 100

    def churner():
        barrier.wait()
        for i in range(iterations):
            try:
                dispatcher.subscribe(f"churn{i}.*", SimpleListener([]))
            except BaseException as e:  # noqa: BLE001
                errors.append(e)
                return

    t = threading.Thread(target=churner, daemon=True)
    t.start()
    barrier.wait()
    try:
        with patch(
            "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
        ):
            for _ in range(iterations):
                await dispatcher.dispatch(
                    UserRegisteredEvent(user_id=1, email="a@b.com")
                )
    finally:
        t.join(timeout=5)
        assert not t.is_alive(), "Churner thread did not complete"

    assert not errors, f"Concurrent subscribe raised: {errors!r}"
    # The "user.*" listener fired on every dispatch — no listeners lost.
    assert len(log) == iterations


# --- Exception isolation ---


@pytest.mark.asyncio
async def test_non_propagating_listener_failure_does_not_block_chain(dispatcher):
    """Default isolation: when an observability-style listener raises
    (no ``propagate_failures``), subsequent listeners still run."""
    log = []

    class BoomListener:
        def handle(self, event):
            raise RuntimeError("boom")

    dispatcher.subscribe("user.registered", OrderedListener("first", log))
    dispatcher.subscribe("user.registered", BoomListener())
    dispatcher.subscribe("user.registered", OrderedListener("third", log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["first", "third"]


@pytest.mark.asyncio
async def test_propagating_listener_failure_short_circuits(dispatcher):
    """Pipeline-critical listeners opt in via ``propagate_failures =
    True``. When such a listener raises, the dispatcher re-raises
    immediately so the upstream queue job retries — subsequent
    listeners must NOT run with stale post-failure state."""
    log = []

    class CriticalBoom:
        propagate_failures = True

        def handle(self, event):
            log.append("critical")
            raise RuntimeError("critical-failure")

    dispatcher.subscribe("user.registered", CriticalBoom())
    dispatcher.subscribe("user.registered", OrderedListener("after", log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with (
        patch(
            "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
        ),
        pytest.raises(RuntimeError, match="critical-failure"),
    ):
        await dispatcher.dispatch(event)

    assert log == ["critical"]


# --- Ordering: direct before wildcard ---


@pytest.mark.asyncio
async def test_direct_listeners_fire_before_wildcard_listeners(dispatcher):
    """Listener ordering must be deterministic: direct subscriptions
    fire in registration order, then wildcard subscriptions in
    registration order. Listeners that assume this order (e.g.
    ``PriceAlertListener`` persists before ``PriceAlertNotificationListener``
    notifies) rely on it."""
    log = []
    dispatcher.subscribe("user.registered", OrderedListener("direct-a", log))
    dispatcher.subscribe("user.*", OrderedListener("wildcard-a", log))
    dispatcher.subscribe("user.registered", OrderedListener("direct-b", log))
    dispatcher.subscribe("*.registered", OrderedListener("wildcard-b", log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["direct-a", "direct-b", "wildcard-a", "wildcard-b"]
