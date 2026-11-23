"""No fake may accept a call its facade rejects.

``test_mail_fake_parity.py`` pins this for ``MailFake`` after a real outage:
the fake answered ``Mail.raw(...)``, the real ``Mail`` never had it, and three
operator alerts raised ``AttributeError`` into an ``except`` on every send
while their tests stayed green. The same hole was open in every other fake:

* ``CacheFake`` answered ``set`` / ``delete`` (the facade spells them ``put``
  / ``forget``) and took ``remember(..., factory=fn)``;
* ``EventFake`` answered ``emit`` and took a second ``payload`` argument on
  ``fire`` / ``dispatch`` / ``fire_sync``;
* ``NotificationFake`` answered ``route`` / ``notify`` and took
  ``send(..., channels=[...])``;
* ``LogFake`` took ANY keyword, so ``Log.error("failed", order_id=7)`` was
  recorded while the real logger raised ``TypeError`` on the error path.

The rule is stated once here, over every fake, so the next "convenience"
method or parameter fails in this file first. Being NARROWER than the facade
stays legal: that fails loudly at the call site.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from cara.cache.Cache import Cache
from cara.events.Event import Event
from cara.logging import Logger
from cara.mail import Mail
from cara.notifications.Notification import Notification
from cara.queues.Queue import Queue
from cara.testing import (
    CacheFake,
    EventFake,
    LogFake,
    MailFake,
    NotificationFake,
    QueueFake,
)

#: fake -> (the object the facade resolves, helpers only a TEST calls).
FAKES: dict[type, tuple[type, frozenset[str]]] = {
    CacheFake: (
        Cache,
        frozenset({"all", "assert_has", "assert_missing", "clear", "ttl_of"}),
    ),
    EventFake: (
        Event,
        frozenset(
            {"assert_dispatched", "assert_not_dispatched", "clear", "count", "dispatched"}
        ),
    ),
    LogFake: (
        Logger,
        frozenset(
            {
                "assert_logged",
                "assert_nothing_logged",
                "clear",
                "count",
                "has",
                "recorded",
            }
        ),
    ),
    MailFake: (
        Mail,
        frozenset({"all", "assert_nothing_sent", "assert_sent", "clear", "count"}),
    ),
    NotificationFake: (
        Notification,
        frozenset(
            {
                "all",
                "assert_nothing_sent",
                "assert_sent",
                "assert_sent_to",
                "clear",
                "count",
            }
        ),
    ),
    QueueFake: (
        Queue,
        frozenset({"all", "assert_pushed", "clear", "count", "pushed"}),
    ),
}

LOG_LEVELS = ("debug", "info", "warning", "error", "critical", "exception")


def _public(obj: type) -> set[str]:
    return {
        name
        for name in dir(obj)
        if not name.startswith("_") and callable(getattr(obj, name, None))
    }


def _parameters(owner: type, name: str) -> list[inspect.Parameter]:
    """``owner.name``'s parameters as a CALLER sees them (no ``self``)."""
    parameters = list(inspect.signature(getattr(owner, name)).parameters.values())
    if not isinstance(inspect.getattr_static(owner, name), staticmethod):
        parameters = parameters[1:]
    return parameters


def _wider_than_real(fake: type, real: type, name: str) -> list[str]:
    """Every way ``fake.name`` binds a call that ``real.name`` would refuse."""
    theirs = _parameters(real, name)
    real_names = {parameter.name for parameter in theirs}
    real_keywords = any(p.kind is p.VAR_KEYWORD for p in theirs)
    real_positionals = any(p.kind is p.VAR_POSITIONAL for p in theirs)
    positional = (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )
    problems: list[str] = []
    ours = _parameters(fake, name)
    for parameter in ours:
        if parameter.kind is parameter.VAR_KEYWORD:
            if not real_keywords:
                problems.append("takes **kwargs")
        elif parameter.kind is parameter.VAR_POSITIONAL:
            if not real_positionals:
                problems.append("takes *args")
        elif parameter.name not in real_names and not real_keywords:
            problems.append(f"takes {parameter.name!r}")
    if not real_positionals and sum(p.kind in positional for p in ours) > sum(
        p.kind in positional for p in theirs
    ):
        problems.append("takes more positional arguments")
    return problems


@pytest.mark.parametrize("fake", FAKES, ids=lambda fake: fake.__name__)
def test_the_fake_answers_no_call_its_facade_lacks(fake: type) -> None:
    real, test_only = FAKES[fake]
    extra = sorted(_public(fake) - _public(real) - test_only)
    assert not extra, (
        f"{fake.__name__} answers {extra}, which {real.__name__} does not have. "
        "Production code written against the fake passes its test and raises "
        "AttributeError on the first real call."
    )
    stale = sorted(test_only - _public(fake))
    assert not stale, f"{fake.__name__} no longer has the test-only helpers {stale}"


@pytest.mark.parametrize("fake", FAKES, ids=lambda fake: fake.__name__)
def test_the_fake_binds_no_argument_its_facade_refuses(fake: type) -> None:
    real, _test_only = FAKES[fake]
    wider = {
        name: problems
        for name in sorted(_public(fake) & _public(real))
        if (problems := _wider_than_real(fake, real, name))
    }
    assert not wider, (
        f"{fake.__name__} accepts calls {real.__name__} raises TypeError for: {wider}"
    )


@pytest.mark.parametrize("level", LOG_LEVELS)
def test_a_log_level_takes_exactly_the_real_loggers_keywords(level: str) -> None:
    """Equal, not merely narrower: ``Log.error(..., exception=exc)`` is a real
    call, and a fake without the keyword cannot rehearse it."""

    def keywords(owner: type) -> set[str]:
        return {p.name for p in _parameters(owner, level) if p.kind is p.KEYWORD_ONLY}

    assert keywords(LogFake) == keywords(Logger)


def test_log_fake_rejects_the_keyword_the_real_logger_rejects() -> None:
    log = LogFake()
    with pytest.raises(TypeError, match="order_id"):
        log.error("payment failed", order_id=7)
    # The scoped logger forwards its keywords, as the real one does.
    with pytest.raises(TypeError, match="order_id"):
        log.with_context(job_id="J1").warning("slow", order_id=7)
    assert log.records == []

    log.error("payment failed: %s", "declined", category="billing", exc_info=True)
    log.exception("boom", context={"order": 7})
    assert [(r.level, r.message, r.category) for r in log.records] == [
        ("error", "payment failed: declined", "billing"),
        ("exception", "boom", None),
    ]
    assert log.records[1].extra == {"context": {"order": 7}}


def test_cache_fake_has_neither_set_nor_delete() -> None:
    cache = CacheFake()
    assert not hasattr(cache, "set")
    assert not hasattr(cache, "delete")
    # The facade's own spellings still round-trip.
    cache.put("k", 1, 60)
    assert cache.remember("k", 60, callback=lambda: 2) == 1
    assert cache.forget("k") is True


def test_event_fake_takes_one_event_and_has_no_emit() -> None:
    events = EventFake()
    assert not hasattr(events, "emit")
    with pytest.raises(TypeError):
        events.fire_sync("order.created", {"id": 1})
    with pytest.raises(TypeError):
        asyncio.run(events.fire("order.created", {"id": 1}))
    assert events.events == []

    marker = object()
    events.fire_sync(marker)
    asyncio.run(events.fire(marker))
    asyncio.run(events.dispatch(marker))
    assert [recorded.event for recorded in events.events] == [marker] * 3


def test_notification_fake_has_no_route_notify_or_channels_argument() -> None:
    notifications = NotificationFake()
    assert not hasattr(notifications, "route")
    assert not hasattr(notifications, "notify")
    with pytest.raises(TypeError):
        notifications.send(object(), object(), ["mail"])
    assert notifications.sent == []
