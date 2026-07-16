"""Mail builder/serialization regressions.

Two defects in the cara mail subsystem:

1. ``Mailable.to_dict()`` (the bridge to every driver and the queued
   ``SendMailableJob``) omitted ``reply_to`` / ``cc`` / ``bcc`` /
   ``priority``. Drivers read those via ``data.get(...)``, so a
   ``Mail.to(x).cc(y).reply_to(z).send()`` silently dropped cc + reply_to.

2. ``MailMessage`` didn't expose ``reply_to`` and its ``from_`` took only
   one positional arg — but ``MailChannel`` calls ``message.from_(addr,
   name)`` and ``message.reply_to(addr)``. Those raised TypeError /
   AttributeError, which ``MailChannel.send`` swallowed into a silent
   ``return False`` (the email never sent).
"""

from __future__ import annotations

from email.utils import parseaddr

import pytest

from cara.mail import Mailable, MailMessage
from cara.mail.drivers.MailgunDriver import MailgunDriver
from cara.mail.drivers.SmtpDriver import SmtpDriver


class TestMailableSerialization:
    def test_to_dict_includes_reply_to_cc_bcc_priority(self) -> None:
        m = (
            Mailable()
            .to("user@example.com")
            .from_("noreply@cheapa.io")
            .reply_to("support@cheapa.io")
            .cc("cc@example.com")
            .bcc("bcc@example.com")
            .high_priority()
        )
        d = m.to_dict()
        assert d["reply_to"] == "support@cheapa.io"
        assert d["cc"] == ["cc@example.com"]
        assert d["bcc"] == ["bcc@example.com"]
        assert d["priority"] == 1

    def test_custom_headers_round_trip_to_smtp_and_mailgun(self) -> None:
        headers = {
            "List-Unsubscribe": "<https://example.com/api/unsubscribe?a=1>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        }
        data = (
            Mailable()
            .to("user@example.com")
            .from_("noreply@synkronus.io")
            .subject("Inventory alert")
            .headers(headers)
            .to_dict()
        )

        smtp_message = SmtpDriver({})._create_message(data)
        assert smtp_message["List-Unsubscribe"] == headers["List-Unsubscribe"]
        assert (
            smtp_message["List-Unsubscribe-Post"]
            == headers["List-Unsubscribe-Post"]
        )

        mailgun_data = MailgunDriver(
            {"secret": "secret", "domain": "mail.synkronus.io"}
        )._prepare_data(data)
        assert (
            mailgun_data["h:List-Unsubscribe"]
            == headers["List-Unsubscribe"]
        )
        assert (
            mailgun_data["h:List-Unsubscribe-Post"]
            == headers["List-Unsubscribe-Post"]
        )

    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("Subject", "Injected subject"),
            ("Received", "forged transport trace"),
            ("ARC-Seal", "forged auth chain"),
            ("X-Safe", "ok\r\nBcc: victim@example.com"),
            ("Bad Header", "value"),
        ],
    )
    def test_custom_headers_reject_managed_and_injected_values(
        self, name: str, value: str
    ) -> None:
        with pytest.raises(ValueError):
            Mailable().header(name, value)


class TestMailMessageFluentApi:
    def test_from_accepts_optional_display_name(self) -> None:
        msg = MailMessage(manager=None).from_("noreply@cheapa.io", "Cheapa")
        # Encoded as "Cheapa <noreply@cheapa.io>"; the bare address is still
        # recoverable for the SMTP envelope (send_message does this itself).
        name, addr = parseaddr(msg.mailable.to_dict()["from"])
        assert name == "Cheapa"
        assert addr == "noreply@cheapa.io"

    def test_from_without_name_is_bare_address(self) -> None:
        msg = MailMessage(manager=None).from_("noreply@cheapa.io")
        assert msg.mailable.to_dict()["from"] == "noreply@cheapa.io"

    def test_reply_to_is_exposed_and_serialized(self) -> None:
        msg = MailMessage(manager=None).reply_to("support@cheapa.io")
        assert msg.mailable.to_dict()["reply_to"] == "support@cheapa.io"

    def test_custom_headers_are_exposed_and_serialized(self) -> None:
        msg = MailMessage(manager=None).header("X-Message-Class", "inventory")
        assert msg.mailable.to_dict()["headers"] == {
            "X-Message-Class": "inventory"
        }
