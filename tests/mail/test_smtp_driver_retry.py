"""``SmtpDriver``: the ``Mail`` manager's SMTP driver rides the at-most-once ladder.

Every product that used this driver had to add the same thing on top:
a dropped socket deserves another attempt, a refused recipient does
not. Getting that backwards either loses deliverable mail on one blip
or sends the same message three times to an address that will never
accept it. The driver now hands the envelope to
``cara.mail.SmtpTransport.send_smtp_envelope``, so an accepted message is
never sent again, 4xx retries and 5xx stops, Bcc is mailed, and a message
that cannot be built (an empty template render, a missing attachment)
fails before any connection.
"""

from __future__ import annotations

import smtplib
import sys
from email import policy
from email.parser import BytesParser

import pytest

from cara.mail.drivers.SmtpDriver import SmtpDriver

# Through sys.modules: the barrel re-exports a same-named function/class,
# so ``import ... as module`` would bind that instead of the module.
transport = sys.modules["cara.mail.SmtpTransport"]


class _Server:
    """Phase-by-phase stand-in for ``smtplib.SMTP`` — no network."""

    def __init__(self) -> None:
        self.logins = 0
        self.rcpts: list[str] = []
        self.payloads: list[bytes] = []
        self.quits = 0

    def ehlo(self):
        return 250, b"hello"

    def starttls(self, **_kw):
        return 220, b"go"

    def login(self, *_a):
        self.logins += 1
        return 235, b"ok"

    def mail(self, *_a):
        return 250, b"ok"

    def rcpt(self, address, *_a):
        self.rcpts.append(address)
        return 250, b"ok"

    def docmd(self, command, *_a):
        assert command == "DATA"
        return 354, b"go ahead"

    def send(self, payload):
        self.payloads.append(payload)

    def getreply(self):
        return 250, b"queued"

    def quit(self):
        self.quits += 1
        return 221, b"bye"


@pytest.fixture
def slept(monkeypatch) -> list[float]:
    recorded: list[float] = []
    monkeypatch.setattr(transport.time, "sleep", recorded.append)
    monkeypatch.setattr(transport.Log, "error", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(transport.Log, "info", lambda *_a, **_k: None, raising=False)
    return recorded


def _install(monkeypatch, factory) -> list:
    opened: list = []

    def _open(*_a, **_k):
        server = factory()
        opened.append(server)
        return server

    monkeypatch.setattr(transport.smtplib, "SMTP", _open)
    return opened


def _driver(**config) -> SmtpDriver:
    return SmtpDriver(
        {
            "host": "smtp.example",
            "port": 587,
            "encryption": "none",
            "from_address": "from@example.com",
            **config,
        }
    )


def _payload(**overrides) -> dict:
    return {"to": ["to@example.com"], "subject": "hi", "text": "body", **overrides}


def _parsed(raw: bytes):
    return BytesParser(policy=policy.default).parsebytes(raw[: -len(b".\r\n")])


def test_a_clean_send_succeeds_on_the_first_attempt(monkeypatch, slept) -> None:
    opened = _install(monkeypatch, _Server)
    assert _driver().send(_payload()) is True
    assert len(opened) == 1 and len(opened[0].payloads) == 1


def test_a_dropped_connection_before_data_is_retried_and_can_succeed(
    monkeypatch, slept
) -> None:
    calls = {"n": 0}

    class _FlakyOnce(_Server):
        def mail(self, *_a):
            calls["n"] += 1
            if calls["n"] == 1:
                raise smtplib.SMTPServerDisconnected("bye")
            return 250, b"ok"

    opened = _install(monkeypatch, _FlakyOnce)
    assert _driver(retry_base_delay=1.0).send(_payload()) is True
    assert sum(len(s.payloads) for s in opened) == 1, "sent exactly once"
    assert slept == [1.0]


@pytest.mark.parametrize("lost", ["final_reply", "quit"])
def test_an_accepted_or_maybe_accepted_message_is_never_sent_again(
    monkeypatch, slept, lost
) -> None:
    """The old ``send_message`` loop re-sent on every attempt when the 250
    (or the QUIT after it) was lost — three deliveries for one mail."""

    class _Loses(_Server):
        def getreply(self):
            if lost == "final_reply":
                raise smtplib.SMTPServerDisconnected("reply lost")
            return 250, b"queued"

        def quit(self):
            raise smtplib.SMTPServerDisconnected("quit lost")

    opened = _install(monkeypatch, _Loses)
    result = _driver().send(_payload())
    # Maybe-delivered is never claimed as sent; accepted-then-QUIT-lost is.
    assert result is (lost == "quit")
    assert len(opened) == 1 and len(opened[0].payloads) == 1


@pytest.mark.parametrize(
    ("code", "attempts", "result"),
    [(451, 3, False), (550, 1, False)],
)
def test_a_recipient_refusal_follows_its_status_family(
    monkeypatch, slept, code, attempts, result
) -> None:
    class _Refuses(_Server):
        def rcpt(self, address, *_a):
            return code, b"try later" if code < 500 else b"no such user"

    opened = _install(monkeypatch, _Refuses)
    assert _driver(retry_base_delay=0).send(_payload()) is result
    assert len(opened) == attempts, "4xx (greylisting) retries; 5xx stops"
    assert all(server.payloads == [] for server in opened)


def test_bcc_is_mailed_but_never_named_in_a_header(monkeypatch, slept) -> None:
    opened = _install(monkeypatch, _Server)
    data = _payload(
        to=["To Person <to@example.com>"],
        cc=["cc@example.com"],
        bcc=["hidden@example.com"],
    )
    assert _driver().send(data) is True
    assert opened[0].rcpts == ["to@example.com", "cc@example.com", "hidden@example.com"]
    message = _parsed(opened[0].payloads[0])
    assert message["Bcc"] is None
    assert "hidden@example.com" not in opened[0].payloads[0].decode("ascii")
    assert message["Date"] and message["Message-ID"]


def test_an_empty_template_render_fails_and_never_mails_view_data(
    monkeypatch, slept
) -> None:
    opened = _install(monkeypatch, _Server)
    data = _payload(
        text=None,
        html="",
        view="mail.notifications.digest",
        view_data={"token": "secret-token-abc"},
    )
    assert _driver().send(data) is False
    assert opened == [], "failed before any connection"


def test_attachments_ride_beside_the_body_with_a_quoted_filename(
    monkeypatch, slept, tmp_path
) -> None:
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")
    opened = _install(monkeypatch, _Server)
    data = _payload(
        html="<p>hi</p>",
        attachments=[{"name": "Q3 réport; final.pdf", "path": str(pdf)}],
    )
    assert _driver().send(data) is True
    message = _parsed(opened[0].payloads[0])
    assert message.get_content_type() == "multipart/mixed"
    body, attachment = list(message.iter_parts())
    assert body.get_content_type() == "multipart/alternative"
    assert attachment.get_filename() == "Q3 réport; final.pdf"
    assert attachment.get_content_type() == "application/pdf"
    assert attachment.get_payload(decode=True) == b"%PDF-1.4 fake"


def test_a_missing_attachment_file_fails_instead_of_sending_without_it(
    monkeypatch, slept, tmp_path
) -> None:
    opened = _install(monkeypatch, _Server)
    data = _payload(attachments=[{"name": "x.pdf", "path": str(tmp_path / "gone.pdf")}])
    assert _driver().send(data) is False
    assert opened == []


def test_a_failed_starttls_closes_the_socket(monkeypatch, slept) -> None:
    class _NoTls(_Server):
        def starttls(self, **_kw):
            raise smtplib.SMTPNotSupportedError("STARTTLS not supported")

    opened = _install(monkeypatch, _NoTls)
    assert _driver(encryption="TLS").send(_payload()) is False
    assert len(opened) == 1 and opened[0].quits == 1
    assert opened[0].payloads == []


def test_a_bad_login_is_not_retried(monkeypatch, slept) -> None:
    class _BadLogin(_Server):
        def login(self, *_a):
            raise smtplib.SMTPAuthenticationError(535, b"bad credentials")

    opened = _install(monkeypatch, _BadLogin)
    assert _driver(username="u", password="p").send(_payload()) is False
    assert len(opened) == 1


def test_a_persistent_transient_failure_exhausts_the_budget_with_backoff(
    monkeypatch, slept
) -> None:
    class _AlwaysDown(_Server):
        def mail(self, *_a):
            raise smtplib.SMTPServerDisconnected("still down")

    opened = _install(monkeypatch, _AlwaysDown)
    assert _driver(retry_base_delay=1.0, max_attempts=3).send(_payload()) is False
    assert len(opened) == 3
    assert slept == [1.0, 2.0], "two gaps for three attempts, none after the last"


def test_attempt_budget_is_floored_at_one(monkeypatch, slept) -> None:
    _install(monkeypatch, _Server)
    driver = _driver(max_attempts=0)
    assert driver.max_attempts == 1
    assert driver.send(_payload()) is True


def test_an_unbuildable_message_fails_without_touching_the_network(
    monkeypatch, slept
) -> None:
    opened = _install(monkeypatch, _Server)
    driver = _driver()
    monkeypatch.setattr(
        driver, "_create_message", lambda _data: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert driver.send(_payload()) is False
    assert opened == []
