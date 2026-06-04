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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_listen_with_callback(dispatcher):
    log = []
    dispatcher.listen("user.registered", lambda e: log.append(e.email))

    event = UserRegisteredEvent(user_id=1, email="test@example.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["test@example.com"]


@pytest.mark.asyncio
async def test_dispatch_no_listeners_is_noop(dispatcher):
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    # The dispatcher gates dispatch on ``event.validate_payload()``
    # returning a falsy "missing fields" report (the gate landed
    # AFTER this test was originally authored — see Event.py:318).
    # Bare MagicMock auto-creates ``validate_payload`` as a method
    # whose return value is another MagicMock, which is truthy, so
    # the gate would log "failed validate_payload(); Skipping
    # dispatch." and the listeners never run. Explicitly set to
    # None so ``callable(validator)`` is False and the gate skips.
    event.validate_payload = None

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_leading(dispatcher):
    log = []
    dispatcher.subscribe("*.registered", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_does_not_match_unrelated(dispatcher):
    log = []
    dispatcher.subscribe("order.*", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(EventA())

    assert log == ["a", "b"]


@pytest.mark.asyncio
async def test_fresh_dispatch_scope_isolates_child_event_chain(dispatcher):
    """``fresh_dispatch_scope`` resets the in-flight event stack so a
    sync-dispatched child job (whose own ``handle()`` fires events) is
    treated as a fresh top-level dispatch — not a continuation of the
    parent listener's event chain.

    Real-world shape (variation discovery):

      1. Parent product fires ``product.collected``.
      2. ``AmazonPostCollectionListener`` runs as a listener for that
         event — so ``_dispatch_stack`` is ``("product.collected",)``.
      3. The listener calls ``Bus.dispatch(CollectProductJob(sibling))``;
         in ``--sync`` mode the child job runs INLINE in the same
         async context (same contextvar bindings).
      4. The child job completes and fires ``product.collected`` for
         the sibling product.
      5. Pre-fix: cycle detector sees ``"product.collected"`` already
         in the stack and raises ``EventDispatchCycleException`` even
         though the two dispatches are for DIFFERENT entities — a
         legitimate fan-out tree, not a recursive loop.

    The fix exposes a public ``fresh_dispatch_scope()`` that callers
    crossing a job boundary use to clear the stack. ``Bus._run_sync_with_tracking``
    wraps job execution in this scope; queued mode is already fine
    because each worker has its own contextvar context.
    """
    from cara.events.Event import fresh_dispatch_scope

    log = []

    class ChildJob:
        """Simulates a queued job that runs synchronously inline. Its
        ``handle()`` fires ``user.registered`` for a SIBLING user
        (different id from the parent that triggered this dispatch).
        Real-world equivalent: ``CollectProductJob`` for a variation
        ASIN, whose ``handle()`` ends with ``Event.fire(ProductCollected(...))``
        for the sibling product."""

        async def handle(self, sibling_id):
            with fresh_dispatch_scope():
                await dispatcher.dispatch(
                    UserRegisteredEvent(
                        user_id=sibling_id, email=f"sib{sibling_id}@x.com"
                    )
                )

    class SiblingDispatcher:
        """Listener mirroring ``AmazonPostCollectionListener``: only
        dispatches sibling work when handling the ROOT event (user_id=1
        is the parent; user_id>=2 is a sibling that already has its
        own listener pass and must NOT recurse). Gating on the parent
        id is the natural termination — sibling discovery in the real
        listener gates on ``listing.metadata.variation_asins`` and the
        ``already_exists`` set, which is the same shape."""

        propagate_failures = True

        async def handle(self, event):
            log.append(f"sibling_listener:{event.user_id}")
            # Only the root parent (user_id=1) has unseen siblings to
            # dispatch — siblings themselves terminate the fan-out.
            if event.user_id == 1:
                await ChildJob().handle(sibling_id=2)

    class CollectionAuditListener:
        """Plain listener subscribed to the same topic — pinned here
        so we can verify the SIBLING dispatch reached the listener
        chain (cycle guard would have suppressed it pre-fix)."""

        async def handle(self, event):
            log.append(f"audit:{event.user_id}")

    dispatcher.subscribe("user.registered", SiblingDispatcher())
    dispatcher.subscribe("user.registered", CollectionAuditListener())

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync",
        return_value=True,
    ):
        await dispatcher.dispatch(UserRegisteredEvent(user_id=1, email="parent@x.com"))

    # Parent listener ran for user 1, child fan-out dispatched user 2,
    # and the audit listener saw BOTH ids — no cycle exception.
    assert "sibling_listener:1" in log
    assert "audit:1" in log
    assert "audit:2" in log, (
        f"FAIL: sibling-style event dispatch was suppressed. log={log!r}. "
        "Likely ``fresh_dispatch_scope`` isn't isolating the child "
        "dispatch — the cycle detector still sees the parent's "
        "in-flight ``user.registered`` and the child's listeners "
        "never run."
    )


@pytest.mark.asyncio
async def test_fresh_dispatch_scope_restores_outer_stack_on_exit():
    """The scope must be a strict stack push/pop — leaving the scope
    restores the outer dispatch stack. Otherwise a sync job that uses
    the helper would leak an empty stack back into its caller and
    break the cycle detector for the rest of the caller's listener
    chain."""
    from cara.events.Event import _dispatch_stack, fresh_dispatch_scope

    token = _dispatch_stack.set(("outer.event",))
    try:
        assert _dispatch_stack.get() == ("outer.event",)
        with fresh_dispatch_scope():
            assert _dispatch_stack.get() == (), (
                "fresh_dispatch_scope must clear the stack inside the block"
            )
        assert _dispatch_stack.get() == ("outer.event",), (
            "fresh_dispatch_scope must restore the prior stack on exit"
        )
    finally:
        _dispatch_stack.reset(token)


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

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
                await dispatcher.dispatch(UserRegisteredEvent(user_id=1, email="a@b.com"))
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
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
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
    ``PriceAlertTriggeredListener`` persists before ``PriceAlertNotificationListener``
    notifies) rely on it."""
    log = []
    dispatcher.subscribe("user.registered", OrderedListener("direct-a", log))
    dispatcher.subscribe("user.*", OrderedListener("wildcard-a", log))
    dispatcher.subscribe("user.registered", OrderedListener("direct-b", log))
    dispatcher.subscribe("*.registered", OrderedListener("wildcard-b", log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["direct-a", "direct-b", "wildcard-a", "wildcard-b"]


# ── Stress tests ────────────────────────────────────────────────────
# These exercise the cycle guard, RLock + snapshot, and dedup paths
# beyond the trivial-case coverage above. Each one is meant to fail
# loudly if any of those primitives regresses — a deadlock, dropped
# listener, false-positive cycle, or "dict changed size during
# iteration" surfaces here long before it reaches production.


@pytest.mark.asyncio
async def test_deep_nested_cascade_does_not_trip_cycle_guard(dispatcher):
    """A → B → C → D → E → F → G → H → I → J normal cascade. Each
    handler dispatches a DIFFERENT event, so the cycle guard must
    treat the chain as benign. Pre-fix worry: if the dispatch stack
    grows but never unwinds across awaits, deep but legal cascades
    would false-positive."""
    depth = 10
    log: list[str] = []

    class StepEvent:
        def __init__(self, idx: int):
            self.name = f"step.{idx}"
            self.idx = idx
            self.is_propagation_stopped = False

    def make_handler(next_idx: int | None):
        class StepListener:
            propagate_failures = True

            async def handle(self, event):
                log.append(event.name)
                if next_idx is not None:
                    await dispatcher.dispatch(StepEvent(next_idx))

        return StepListener()

    for i in range(depth):
        next_i = i + 1 if i + 1 < depth else None
        dispatcher.subscribe(f"step.{i}", make_handler(next_i))

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(StepEvent(0))

    assert log == [f"step.{i}" for i in range(depth)]


@pytest.mark.asyncio
async def test_listener_in_three_overlapping_wildcards_fires_once(dispatcher):
    """One listener instance subscribed to a direct name AND TWO
    wildcards that both match. Dedup must collapse to a single
    invocation — regression would re-trigger side effects 3x."""
    log = []
    listener = SimpleListener(log)
    dispatcher.subscribe("user.registered", listener)
    dispatcher.subscribe("user.*", listener)
    dispatcher.subscribe("*.registered", listener)
    dispatcher.subscribe("*", listener)  # matches everything too

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


def test_threaded_concurrent_dispatch_independent_event_loops():
    """Each thread runs its OWN ``asyncio.run`` and dispatches the
    same event 50 times against a shared dispatcher. The cycle guard
    is a ContextVar so each task starts fresh; the snapshot under
    RLock keeps the shared listener buckets safe to iterate while
    the other threads may still be subscribing.

    This is the multi-thread variant of
    ``test_concurrent_dispatches_have_independent_cycle_stacks`` (which
    runs on one loop), and the higher-volume sibling of
    ``test_concurrent_subscribe_during_dispatch_does_not_raise``."""
    import asyncio as _a
    import threading

    dispatcher = EventDispatcher()
    log_lock = threading.Lock()
    log: list[int] = []
    threads_n = 8
    per_thread = 50
    barrier = threading.Barrier(threads_n)
    errors: list[BaseException] = []

    class ThreadLogger:
        def handle(self, event):
            with log_lock:
                log.append(event.user_id)

    dispatcher.subscribe("user.registered", ThreadLogger())
    # A few wildcard subs so the dispatcher exercises the wildcard
    # snapshot path under contention, not just direct lookup.
    dispatcher.subscribe("user.*", ThreadLogger())
    dispatcher.subscribe("*.registered", ThreadLogger())

    def worker(tid: int):
        async def runner():
            with patch(
                "cara.context.ExecutionContext.ExecutionContext.is_sync",
                return_value=True,
            ):
                for i in range(per_thread):
                    await dispatcher.dispatch(
                        UserRegisteredEvent(user_id=tid * 1000 + i, email=f"{tid}@x.com")
                    )

        try:
            barrier.wait()
            _a.run(runner())
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive(), "Worker thread hung — possible deadlock"

    assert not errors, f"Concurrent dispatch raised: {errors!r}"
    # Each dispatch fires the deduped listener exactly once per registration
    # of distinct ThreadLogger instances — we registered 3, but they are
    # 3 distinct objects so all 3 fire per dispatch.
    assert len(log) == threads_n * per_thread * 3


def test_thread_subscribes_while_thread_dispatches_high_volume():
    """High-volume churn variant of
    ``test_concurrent_subscribe_during_dispatch_does_not_raise``:
    1000 subscribe ops racing against 1000 dispatch ops, both
    touching wildcard buckets. Pre-fix this surfaced
    ``RuntimeError: dictionary changed size during iteration``
    quickly under load."""
    import asyncio as _a
    import threading

    dispatcher = EventDispatcher()
    fixed_log: list[str] = []
    dispatcher.subscribe("user.*", SimpleListener(fixed_log))

    errors: list[BaseException] = []
    iterations = 1000
    barrier = threading.Barrier(2)

    def churner():
        try:
            barrier.wait()
            for i in range(iterations):
                dispatcher.subscribe(f"churn{i}.*", SimpleListener([]))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    def dispatcher_thread():
        async def runner():
            with patch(
                "cara.context.ExecutionContext.ExecutionContext.is_sync",
                return_value=True,
            ):
                for _ in range(iterations):
                    await dispatcher.dispatch(
                        UserRegisteredEvent(user_id=1, email="a@b.com")
                    )

        try:
            barrier.wait()
            _a.run(runner())
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    t1 = threading.Thread(target=churner, daemon=True)
    t2 = threading.Thread(target=dispatcher_thread, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=20)
    t2.join(timeout=20)

    assert not t1.is_alive() and not t2.is_alive(), "Worker hung"
    assert not errors, f"High-volume race raised: {errors!r}"
    # The pinned "user.*" listener fired on every dispatch.
    assert len(fixed_log) == iterations


@pytest.mark.asyncio
async def test_cycle_stack_resets_when_listener_raises(dispatcher):
    """If a listener raises (and re-raises via propagate_failures),
    the cycle stack MUST be popped — otherwise the next dispatch of
    the same event in the same task would false-positive as a cycle.
    Tests the ContextVar reset is in the right ``finally`` block."""
    from cara.exceptions import EventDispatchCycleException  # noqa: F401

    class Boomer:
        propagate_failures = True

        def handle(self, event):
            raise RuntimeError("boom")

    dispatcher.subscribe("user.registered", Boomer())

    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await dispatcher.dispatch(UserRegisteredEvent(user_id=1, email="a@b.com"))

        # Second dispatch in the same task: should also reach the
        # listener and raise — NOT EventDispatchCycleException.
        with pytest.raises(RuntimeError, match="boom"):
            await dispatcher.dispatch(UserRegisteredEvent(user_id=2, email="b@c.com"))


@pytest.mark.asyncio
async def test_wildcard_matching_multiple_patterns_preserves_order(dispatcher):
    """An event name that matches three wildcard patterns: registration
    order must be preserved across patterns (insertion order of the
    wildcard dict)."""
    log = []
    dispatcher.subscribe("user.*", OrderedListener("a", log))
    dispatcher.subscribe("*.registered", OrderedListener("b", log))
    dispatcher.subscribe("*", OrderedListener("c", log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch(
        "cara.context.ExecutionContext.ExecutionContext.is_sync", return_value=True
    ):
        await dispatcher.dispatch(event)

    # All three patterns match — order = registration order.
    assert log == ["a", "b", "c"]
