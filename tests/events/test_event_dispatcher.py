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
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_listen_with_callback(dispatcher):
    log = []
    dispatcher.listen("user.registered", lambda e: log.append(e.email))

    event = UserRegisteredEvent(user_id=1, email="test@example.com")
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["test@example.com"]


@pytest.mark.asyncio
async def test_dispatch_no_listeners_is_noop(dispatcher):
    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
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

    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
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
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
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
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_leading(dispatcher):
    log = []
    dispatcher.subscribe("*.registered", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
        await dispatcher.dispatch(event)

    assert log == ["simple"]


@pytest.mark.asyncio
async def test_wildcard_does_not_match_unrelated(dispatcher):
    log = []
    dispatcher.subscribe("order.*", SimpleListener(log))

    event = UserRegisteredEvent(user_id=1, email="a@b.com")
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
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
    with patch("cara.context.ExecutionContext.is_sync", return_value=True):
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
