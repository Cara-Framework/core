"""SMTP: a non-ASCII sender is permanent; encryption must be spelled out.

Live defects in ``cara/mail/SmtpTransport.py`` / ``SmtpDriver``:

1. A non-ASCII envelope sender raised ``UnicodeEncodeError`` inside
   ``smtplib`` at ``MAIL FROM`` on every attempt and came back RETRYABLE, so
   a durable mail ledger re-tried it forever. It is now refused before any
   connection as ``PERMANENT_FAILURE`` / ``sender_not_ascii``.
2. ``encryption`` was compared literally: anything but ``"ssl"`` / ``"tls"``
   — ``"TLS"``, a typo like ``"tsl"``, an empty value — opened a PLAINTEXT
   connection with AUTH in the clear. ``smtp_encryption_mode`` now accepts
   only ``tls`` / ``ssl`` / ``none`` in any case, and both
   ``open_smtp_connection`` and the ``SmtpDriver`` constructor go through it.
"""

from __future__ import annotations

from email.message import EmailMessage

import pytest

import cara.mail.SmtpTransport as transport
from cara.mail import MailDeliveryOutcome, SmtpSettings, send_smtp
from cara.mail.drivers.SmtpDriver import SmtpDriver


class _Server:
    instances: list[_Server] = []

    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[str] = []
        _Server.instances.append(self)

    def ehlo(self):
        self.calls.append("ehlo")
        return 250, b"hello"

    def starttls(self, **_kw):
        self.calls.append("starttls")
        return 220, b"go"

    def login(self, *_a):
        self.calls.append("login")
        return 235, b"ok"

    def quit(self):
        return 221, b"bye"

    def close(self):
        return None


class _SslServer(_Server):
    pass


@pytest.fixture
def servers(monkeypatch):
    _Server.instances = []
    monkeypatch.setattr(transport.smtplib, "SMTP", _Server)
    monkeypatch.setattr(transport.smtplib, "SMTP_SSL", _SslServer)
    monkeypatch.setattr(transport, "_verified_tls_context", lambda: object())
    monkeypatch.setattr(transport.Log, "error", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(transport.Log, "info", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(transport.time, "sleep", lambda _s: None)
    return _Server.instances


def _settings(encryption: str) -> SmtpSettings:
    return SmtpSettings(
        host="smtp.example",
        port=587,
        username="u",
        password="p",
        encryption=encryption,
    )


def test_non_ascii_sender_is_a_permanent_failure_before_any_connection(
    servers,
) -> None:
    message = EmailMessage()
    message["Subject"] = "hi"
    message.set_content("body")

    outcome = send_smtp(
        message,
        envelope_from="jörg@example.com",
        recipient="user@example.com",
        settings=_settings("none"),
    )

    assert outcome.status == MailDeliveryOutcome.PERMANENT_FAILURE
    assert outcome.reason == "sender_not_ascii"
    assert servers == [], "no connection is opened for an unusable sender"


@pytest.mark.parametrize("value", ["tls", "TLS", " Tls "])
def test_tls_in_any_case_upgrades_before_login(servers, value) -> None:
    transport.open_smtp_connection(_settings(value))

    (server,) = servers
    assert type(server) is _Server
    assert server.calls == ["ehlo", "starttls", "ehlo", "login"]


@pytest.mark.parametrize("value", ["ssl", "SSL"])
def test_ssl_in_any_case_opens_an_ssl_socket(servers, value) -> None:
    transport.open_smtp_connection(_settings(value))

    (server,) = servers
    assert type(server) is _SslServer


def test_plaintext_only_when_written_none(servers) -> None:
    transport.open_smtp_connection(_settings("NONE"))

    (server,) = servers
    assert "starttls" not in server.calls


@pytest.mark.parametrize("value", ["tsl", "starttls", "", "   ", None])
def test_unknown_encryption_is_refused_before_connecting(servers, value) -> None:
    with pytest.raises(ValueError, match="Unknown SMTP encryption"):
        transport.open_smtp_connection(_settings(value))
    assert servers == []


@pytest.mark.parametrize("value", ["tsl", "starttls", ""])
def test_smtp_driver_constructor_refuses_unknown_encryption(value) -> None:
    with pytest.raises(ValueError, match="Unknown SMTP encryption"):
        SmtpDriver({"host": "smtp.example", "encryption": value})


def test_smtp_driver_constructor_normalises_the_case() -> None:
    assert SmtpDriver({"encryption": "TLS"}).encryption == "tls"
    assert SmtpDriver({}).encryption == "tls"
