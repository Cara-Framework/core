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

from cara.mail import Mailable
from cara.mail import MailMessage


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
