"""SMTP transient/permanent classification and bounded retry.

Every product that used this driver had to add the same thing on top:
a dropped socket deserves another attempt, a refused recipient does
not. Getting that backwards either loses deliverable mail on one blip
or sends the same message three times to an address that will never
accept it.
"""

from __future__ import annotations

import smtplib

import pytest

from cara.mail.drivers.SmtpDriver import SmtpDriver


class _Server:
    """Context-manager stand-in for ``smtplib.SMTP``."""

    def __init__(self, outcomes: list) -> None:
        self._outcomes = outcomes
        self.sent: list = []
        self.logins = 0

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def login(self, *_a):
        self.logins += 1

    def send_message(self, msg):
        outcome = self._outcomes.pop(0) if self._outcomes else None
        if isinstance(outcome, Exception):
            raise outcome
        self.sent.append(msg)


def _driver(monkeypatch, outcomes: list, **config) -> tuple[SmtpDriver, _Server]:
    server = _Server(outcomes)
    driver = SmtpDriver(
        {
            "host": "smtp.example",
            "port": 587,
            "from_address": "from@example",
            "retry_base_delay": 0,  # keep the suite fast; backoff is pinned below
            **config,
        }
    )
    monkeypatch.setattr(driver, "_get_connection", lambda: server)
    return driver, server


def _payload() -> dict:
    return {"to": ["to@example"], "subject": "hi", "text": "body"}


def test_a_clean_send_succeeds_on_the_first_attempt(monkeypatch) -> None:
    driver, server = _driver(monkeypatch, [])
    assert driver.send(_payload()) is True
    assert len(server.sent) == 1


def test_a_dropped_connection_is_retried_and_can_succeed(monkeypatch) -> None:
    driver, server = _driver(
        monkeypatch, [smtplib.SMTPServerDisconnected("bye"), None]
    )
    assert driver.send(_payload()) is True
    assert len(server.sent) == 1, "the successful attempt sent exactly once"


@pytest.mark.parametrize(
    "error",
    [
        smtplib.SMTPRecipientsRefused({"to@example": (550, b"no such user")}),
        smtplib.SMTPSenderRefused(550, b"bad sender", "from@example"),
        smtplib.SMTPDataError(554, b"message rejected"),
        smtplib.SMTPAuthenticationError(535, b"bad credentials"),
    ],
)
def test_permanent_failures_are_not_retried(monkeypatch, error) -> None:
    """Retrying a refusal cannot fix it — and would deliver duplicates
    if the peer refused only the response, not the message."""
    attempts = {"n": 0}

    class _Counting(_Server):
        def send_message(self, msg):
            attempts["n"] += 1
            raise error

    server = _Counting([])
    driver = SmtpDriver({"host": "smtp.example", "retry_base_delay": 0})
    monkeypatch.setattr(driver, "_get_connection", lambda: server)

    assert driver.send(_payload()) is False
    assert attempts["n"] == 1


def test_a_persistent_transient_failure_exhausts_the_budget(monkeypatch) -> None:
    attempts = {"n": 0}

    class _AlwaysDown(_Server):
        def send_message(self, msg):
            attempts["n"] += 1
            raise smtplib.SMTPServerDisconnected("still down")

    server = _AlwaysDown([])
    driver = SmtpDriver(
        {"host": "smtp.example", "retry_base_delay": 0, "max_attempts": 4}
    )
    monkeypatch.setattr(driver, "_get_connection", lambda: server)

    assert driver.send(_payload()) is False
    assert attempts["n"] == 4


def test_backoff_is_exponential_between_attempts(monkeypatch) -> None:
    # Reach the MODULE through sys.modules: the drivers barrel re-exports
    # the class under the same name, so an `import ... as module` binds
    # the class instead (the name/submodule shadowing footgun).
    import sys

    module = sys.modules["cara.mail.drivers.SmtpDriver"]

    slept: list[float] = []
    monkeypatch.setattr(module.time, "sleep", lambda s: slept.append(s))

    class _AlwaysDown(_Server):
        def send_message(self, msg):
            raise smtplib.SMTPServerDisconnected("down")

    server = _AlwaysDown([])
    driver = SmtpDriver(
        {"host": "smtp.example", "retry_base_delay": 1.0, "max_attempts": 3}
    )
    monkeypatch.setattr(driver, "_get_connection", lambda: server)

    assert driver.send(_payload()) is False
    # Two gaps for three attempts, and no sleep after the last one.
    assert slept == [1.0, 2.0]


def test_attempt_budget_is_floored_at_one(monkeypatch) -> None:
    driver, server = _driver(monkeypatch, [], max_attempts=0)
    assert driver.max_attempts == 1
    assert driver.send(_payload()) is True


def test_an_unbuildable_message_fails_without_touching_the_network(
    monkeypatch,
) -> None:
    driver, server = _driver(monkeypatch, [])
    monkeypatch.setattr(
        driver, "_create_message", lambda _data: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert driver.send(_payload()) is False
    assert server.sent == []
