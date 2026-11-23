"""``cara.mail.SmtpTransport``: at-most-once SMTP, metadata-only logs.

Loss before the server's 354 is retried; loss after it is UNKNOWN and never
replayed (unless the message is replay-safe, then exactly once more); a
structured refusal follows its status family; a 5xx before RCPT judges our
own account and raises.
"""

from __future__ import annotations

import smtplib
from email import policy
from email.parser import BytesParser

import pytest

import cara.mail.SmtpTransport as transport
from cara.mail import (
    MailDeliveryOutcome,
    SmtpAccountRefusedError,
    SmtpSettings,
    build_alternative_message,
    classify_refused_recipients,
    send_smtp,
)

SETTINGS = SmtpSettings(
    host="smtp.example", port=2525, username="u", password="p", encryption="none"
)


class _Server:
    """Phase-by-phase stand-in for ``smtplib.SMTP``."""

    def __init__(self) -> None:
        self.payloads: list[bytes] = []

    def ehlo(self):
        return 250, b"hello"

    def starttls(self, **_kw):
        return 220, b"go"

    def login(self, *_a):
        return 235, b"ok"

    def mail(self, *_a):
        return 250, b"ok"

    def rcpt(self, *_a):
        return 250, b"ok"

    def docmd(self, command, *_a):
        assert command == "DATA"
        return 354, b"go ahead"

    def send(self, payload):
        self.payloads.append(payload)

    def getreply(self):
        return 250, b"queued"

    def quit(self):
        return 221, b"bye"


@pytest.fixture
def sleeps(monkeypatch):
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


def _message():
    return build_alternative_message(
        subject="Hello",
        from_name="Sender",
        from_email="from@example.com",
        to_email="owner@example.com",
        text_body="plain",
        html_body="<p>html</p>",
    )


def _send(**kw):
    return send_smtp(
        _message(),
        envelope_from="from@example.com",
        recipient="owner@example.com",
        settings=SETTINGS,
        **kw,
    )


def test_a_clean_send_is_sent_once(monkeypatch, sleeps):
    opened = _install(monkeypatch, _Server)
    assert _send() == MailDeliveryOutcome(MailDeliveryOutcome.SENT)
    assert len(opened) == 1 and len(opened[0].payloads) == 1
    assert opened[0].payloads[0].endswith(b"\r\n.\r\n")


def test_loss_after_data_is_unknown_and_never_replayed(monkeypatch, sleeps):
    class _Drops(_Server):
        def getreply(self):
            raise smtplib.SMTPServerDisconnected("lost")

    opened = _install(monkeypatch, _Drops)
    outcome = _send()
    assert outcome.status == MailDeliveryOutcome.UNKNOWN_DELIVERY
    assert len(opened) == 1 and sleeps == []


def test_a_replay_safe_message_is_sent_exactly_once_more(monkeypatch, sleeps):
    class _Drops(_Server):
        def getreply(self):
            raise smtplib.SMTPServerDisconnected("lost")

    opened = _install(monkeypatch, _Drops)
    outcome = _send(replay_safe=True)
    assert outcome.status == MailDeliveryOutcome.UNKNOWN_DELIVERY
    assert len(opened) == 2


def test_a_replay_safe_refusal_after_ambiguity_stays_unknown(monkeypatch, sleeps):
    calls = {"n": 0}

    class _Then550(_Server):
        def getreply(self):
            calls["n"] += 1
            if calls["n"] == 1:
                raise smtplib.SMTPServerDisconnected("lost")
            return 550, b"duplicate"

    _install(monkeypatch, _Then550)
    assert _send(replay_safe=True).status == MailDeliveryOutcome.UNKNOWN_DELIVERY


def test_a_quit_failure_after_acceptance_is_still_sent(monkeypatch, sleeps):
    class _QuitDrops(_Server):
        def quit(self):
            raise smtplib.SMTPServerDisconnected("quit lost")

    opened = _install(monkeypatch, _QuitDrops)
    assert _send().sent
    assert len(opened) == 1


@pytest.mark.parametrize("phase", ["mail", "rcpt", "docmd"])
def test_loss_before_data_is_retried_with_backoff(monkeypatch, sleeps, phase):
    class _Drops(_Server):
        pass

    def _boom(*_a, **_k):
        raise smtplib.SMTPServerDisconnected("dropped")

    setattr(_Drops, phase, _boom)
    opened = _install(monkeypatch, _Drops)
    outcome = _send()
    assert outcome.status == MailDeliveryOutcome.RETRYABLE_FAILURE
    assert len(opened) == transport.MAIL_SEND_MAX_ATTEMPTS
    assert sleeps == [0.5, 1.0]


@pytest.mark.parametrize(
    ("code", "status", "attempts"),
    [
        (451, MailDeliveryOutcome.RETRYABLE_FAILURE, 3),
        (550, MailDeliveryOutcome.PERMANENT_FAILURE, 1),
    ],
)
def test_a_final_reply_follows_its_status_family(
    monkeypatch, sleeps, code, status, attempts
):
    class _Replies(_Server):
        def getreply(self):
            return code, b"provider text"

    opened = _install(monkeypatch, _Replies)
    outcome = _send()
    assert outcome.status == status
    assert outcome.reason == f"SMTPDataError:{code}"
    assert len(opened) == attempts


@pytest.mark.parametrize(
    ("code", "status", "attempts"),
    [
        (450, MailDeliveryOutcome.RETRYABLE_FAILURE, 3),
        (550, MailDeliveryOutcome.PERMANENT_FAILURE, 1),
    ],
)
def test_a_recipient_refusal_follows_its_status_family(
    monkeypatch, sleeps, code, status, attempts
):
    class _Refuses(_Server):
        def rcpt(self, *_a):
            return code, b"owner@example.com unknown"

    opened = _install(monkeypatch, _Refuses)
    assert _send().status == status
    assert len(opened) == attempts


def test_a_5xx_before_any_recipient_raises_account_refused(monkeypatch, sleeps):
    class _BadLogin(_Server):
        def login(self, *_a):
            raise smtplib.SMTPAuthenticationError(535, b"bad credentials")

    _install(monkeypatch, _BadLogin)
    with pytest.raises(SmtpAccountRefusedError) as raised:
        _send()
    assert raised.value.outcome.status == MailDeliveryOutcome.PERMANENT_FAILURE


def test_logs_never_carry_the_raw_recipient(monkeypatch, sleeps):
    lines: list[str] = []
    monkeypatch.setattr(transport.Log, "error", lambda msg, *_a, **_k: lines.append(msg))

    class _Refuses(_Server):
        def rcpt(self, *_a):
            return 550, b"owner@example.com unknown"

    _install(monkeypatch, _Refuses)
    _send()
    rendered = " ".join(lines)
    assert "owner@example.com" not in rendered
    assert transport.masked_recipient("owner@example.com") in rendered


def test_partial_and_mixed_refusals_fail_closed():
    mixed = classify_refused_recipients(
        {"a@x.io": (450, b""), "b@x.io": (550, b"")}, ("a@x.io", "b@x.io")
    )
    partial = classify_refused_recipients({"a@x.io": (550, b"")}, ("a@x.io", "b@x.io"))
    assert mixed.status == MailDeliveryOutcome.PERMANENT_FAILURE
    assert partial.status == MailDeliveryOutcome.UNKNOWN_DELIVERY


def test_the_message_scrubs_crlf_and_replaces_rather_than_duplicates_headers():
    message = build_alternative_message(
        subject="Hi\r\nBcc: evil@x.io",
        from_name="Sender",
        from_email="from@example.com",
        to_email="owner@example.com\nCc: evil@x.io",
        text_body="plain",
        html_body="<p>" + "x" * 2000 + "</p>",
        reply_to="support@example.com",
        headers={"Reply-To": "other@example.com", "X-Empty": "  "},
        attachments=[("r\n.csv", "a,b\nç,é\n", "text/csv")],
    )
    from cara.mail import smtp_message_bytes

    parsed = BytesParser(policy=policy.default).parsebytes(
        smtp_message_bytes(message)[: -len(b".\r\n")]
    )
    assert parsed.get_all("Bcc") is None and parsed.get_all("Cc") is None
    assert parsed.get_all("Reply-To") == ["other@example.com"]
    assert parsed.get("X-Empty") is None
    attachment = next(parsed.iter_attachments())
    assert attachment.get_payload(decode=True).decode("utf-8") == "a,b\nç,é\n"


# --- handshake, preflight and TLS-context regressions (audit 2026-11-23) ---

TLS_SETTINGS = SmtpSettings(
    host="smtp.example", port=2525, username="u", password="p", encryption="tls"
)


class _NoEsmtp(_Server):
    """A server whose EHLO answers ``code`` and whose HELO answers 250."""

    ehlo_code = 421
    helo_calls = 0

    def ehlo(self):
        return self.ehlo_code, b"not now"

    def helo(self, *_a):
        type(self).helo_calls += 1
        return 250, b"hello"


@pytest.mark.parametrize("encryption", ["tls", "none"])
def test_a_4xx_ehlo_is_retryable_never_an_account_refusal(
    monkeypatch, sleeps, encryption
):
    class _Busy(_NoEsmtp):
        ehlo_code = 421

    opened = _install(monkeypatch, _Busy)
    outcome = send_smtp(
        _message(),
        envelope_from="from@example.com",
        recipient="owner@example.com",
        settings=SmtpSettings(
            host="smtp.example",
            port=2525,
            username="u",
            password="p",
            encryption=encryption,
        ),
    )
    assert outcome == MailDeliveryOutcome(
        MailDeliveryOutcome.RETRYABLE_FAILURE, "SMTPHeloError:421"
    )
    assert len(opened) == transport.MAIL_SEND_MAX_ATTEMPTS
    assert all(server.payloads == [] for server in opened)


def test_a_5xx_ehlo_falls_back_to_helo(monkeypatch, sleeps):
    class _Legacy(_NoEsmtp):
        ehlo_code = 502
        helo_calls = 0

    opened = _install(monkeypatch, _Legacy)
    no_auth = SmtpSettings(host="smtp.example", port=2525, encryption="none")
    outcome = send_smtp(
        _message(),
        envelope_from="from@example.com",
        recipient="owner@example.com",
        settings=no_auth,
    )
    assert outcome.sent and _Legacy.helo_calls == 1 and len(opened) == 1


def test_a_refused_helo_after_a_5xx_ehlo_is_an_account_refusal(monkeypatch, sleeps):
    class _Refuses(_NoEsmtp):
        ehlo_code = 550

        def helo(self, *_a):
            return 550, b"go away"

    _install(monkeypatch, _Refuses)
    with pytest.raises(SmtpAccountRefusedError):
        _send()


@pytest.mark.parametrize(
    ("recipient", "reason"),
    [
        ("", "recipient_blank"),
        ("   ", "recipient_blank"),
        (None, "recipient_blank"),
        ("çağrı@example.com", "recipient_not_ascii"),
        ("user@örnek.com.tr", "recipient_not_ascii"),
    ],
)
def test_an_undeliverable_recipient_fails_once_without_a_connection(
    monkeypatch, sleeps, recipient, reason
):
    opened = _install(monkeypatch, _Server)
    outcome = send_smtp(
        _message(),
        envelope_from="from@example.com",
        recipient=recipient,
        settings=SETTINGS,
    )
    assert outcome == MailDeliveryOutcome(MailDeliveryOutcome.PERMANENT_FAILURE, reason)
    assert opened == [] and sleeps == []


def test_an_unrenderable_message_is_not_sent_and_never_unknown(monkeypatch, sleeps):
    from email.mime.text import MIMEText

    message = MIMEText("body", "plain", "utf-8")
    message["From"] = "from@example.com"
    message["To"] = "owner@example.com"
    message["Subject"] = "Re: order\nstray second line"  # the generator refuses it
    opened = _install(monkeypatch, _Server)
    outcome = send_smtp(
        message,
        envelope_from="from@example.com",
        recipient="owner@example.com",
        settings=SETTINGS,
    )
    assert outcome.status == MailDeliveryOutcome.PERMANENT_FAILURE
    assert outcome.reason.startswith("message_render_failed:")
    assert opened == [], "rendered before any connection, so never past 354"


def test_every_connection_shares_one_verifying_tls_context(monkeypatch, sleeps):
    contexts: list = []

    class _Tls(_Server):
        def starttls(self, *, context=None):
            contexts.append(context)
            return 220, b"go"

    _install(monkeypatch, _Tls)
    for _ in range(2):
        send_smtp(
            _message(),
            envelope_from="from@example.com",
            recipient="owner@example.com",
            settings=TLS_SETTINGS,
        )
    assert len(contexts) == 2 and contexts[0] is contexts[1]
    shared = contexts[0]
    assert shared.verify_mode == transport.ssl.CERT_REQUIRED
    assert shared.check_hostname is True


def test_encryption_is_case_insensitive_so_tls_never_means_plaintext(monkeypatch, sleeps):
    upgraded: list = []

    class _Tls(_Server):
        def starttls(self, **_kw):
            upgraded.append(True)
            return 220, b"go"

    _install(monkeypatch, _Tls)
    send_smtp(
        _message(),
        envelope_from="from@example.com",
        recipient="owner@example.com",
        settings=SmtpSettings(host="smtp.example", encryption="TLS"),
    )
    assert upgraded == [True]


def test_a_failed_handshake_closes_the_socket(monkeypatch, sleeps):
    closed: list = []

    class _NoTls(_Server):
        def starttls(self, **_kw):
            raise smtplib.SMTPNotSupportedError("no STARTTLS")

        def quit(self):
            closed.append(True)
            return 221, b"bye"

    _install(monkeypatch, _NoTls)
    with pytest.raises(SmtpAccountRefusedError):
        send_smtp(
            _message(),
            envelope_from="from@example.com",
            recipient="owner@example.com",
            settings=TLS_SETTINGS,
        )
    assert closed == [True]


def test_a_multi_recipient_envelope_is_all_or_nothing(monkeypatch, sleeps):
    from cara.mail import send_smtp_envelope

    class _OneBad(_Server):
        def __init__(self):
            super().__init__()
            self.named: list = []

        def rcpt(self, address, *_a):
            self.named.append(address)
            return (550, b"no such user") if address.startswith("bad") else (250, b"ok")

        def docmd(self, command, *_a):
            raise AssertionError("DATA must never open after a refused recipient")

    opened = _install(monkeypatch, _OneBad)
    outcome = send_smtp_envelope(
        _message(),
        envelope_from="from@example.com",
        recipients=["good@example.com", "bad@example.com", "hidden@example.com"],
        settings=SETTINGS,
    )
    assert outcome == MailDeliveryOutcome(
        MailDeliveryOutcome.PERMANENT_FAILURE, "recipient_refused_5xx"
    )
    assert len(opened) == 1
    assert opened[0].named == [
        "good@example.com",
        "bad@example.com",
        "hidden@example.com",
    ]
