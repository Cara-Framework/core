"""``MailFake`` may never accept a call that ``Mail`` rejects.

A test double that is more permissive than the object it stands in for
inverts the purpose of the suite: writing a test for the broken call is
what keeps it green.

This is the regression pin for a real outage. ``MailFake`` used to expose
``raw(body, to, …)``, which ``cara.mail.Mail`` has never had. Three cheapa
maintenance jobs called ``Mail.raw(...)``; each real send raised
``AttributeError`` into a surrounding ``except`` and became one warning
line, so the operator alerts they existed to deliver had never fired. The
same fake was also too NARROW — its chain object had no ``subject`` or
``text`` — so the CORRECT spelling would have failed under test as well.

The fake now delegates to the real ``MailMessage`` / ``MailPendingSend``,
which is what makes both directions impossible. These tests state that
contract so a future "convenience" method on the fake fails here first.
"""

from __future__ import annotations

import pytest

from cara.mail import Mail, MailMessage, MailPendingSend
from cara.testing import MailFake

#: Driver plumbing is deliberately absent from the fake — it has no
#: drivers. Being NARROWER is the safe direction: the call fails loudly at
#: the callsite instead of silently standing in for something real.
NOT_FAKED = {"add_driver", "driver", "set_default_driver"}


def _public(obj: type) -> set[str]:
    """Public CALLABLES only.

    The bug class is "a call the fake accepts and the real object rejects",
    so data attributes are not part of the comparison — ``Mail.application``
    is an instance attribute that never appears in the class's ``dir()``,
    and ``MailFake.sent`` is the capture list tests read.
    """
    return {
        name
        for name in dir(obj)
        if not name.startswith("_") and callable(getattr(obj, name, None))
    }


#: Assertion helpers exist only for tests to call; production never does.
FAKE_ONLY = {"all", "assert_nothing_sent", "assert_sent", "clear", "count"}


def test_fake_accepts_nothing_the_real_mail_rejects() -> None:
    """The dangerous direction: a method on the fake that Mail lacks."""
    extra = _public(MailFake) - _public(Mail) - FAKE_ONLY
    assert not extra, (
        f"MailFake exposes {sorted(extra)}, which cara.mail.Mail does not. "
        f"Production code written against the fake would raise AttributeError "
        f"at runtime while its test passes — see this module's docstring."
    )


def test_fake_covers_the_real_send_surface() -> None:
    """The narrow direction: a send path the fake cannot stand in for."""
    missing = _public(Mail) - _public(MailFake) - NOT_FAKED
    assert not missing, (
        f"cara.mail.Mail exposes {sorted(missing)} that MailFake cannot "
        f"stand in for. Add it to the fake, or to NOT_FAKED with a reason."
    )


def test_raw_is_absent_from_both() -> None:
    """The specific spelling that caused the outage stays gone.

    Guards against the guard going vacuous: if ``raw`` ever becomes real,
    this fails and the cheapa maintenance jobs should be re-pointed at it.
    """
    assert not hasattr(Mail, "raw")
    assert not hasattr(MailFake, "raw")


def test_the_chain_is_the_production_chain() -> None:
    """``to()`` / ``mailable()`` hand back the real chain objects."""
    fake = MailFake()
    assert isinstance(fake.to("a@b.co"), MailMessage)
    assert isinstance(fake.mailable(MailMessage(fake).mailable), MailPendingSend)


def test_the_chain_records_what_the_caller_set() -> None:
    fake = MailFake()

    fake.to("ops@example.com").subject("[Alert] disk").text("line1\nline2").send()

    fake.assert_sent(to="ops@example.com", subject="[Alert] disk", times=1)
    assert fake.sent[0].body == "line1\nline2"


def test_the_chain_records_cc_reply_to_and_html() -> None:
    fake = MailFake()

    (
        fake.to(["a@example.com"])
        .cc("c@example.com")
        .bcc(["d@example.com"])
        .reply_to("noreply@example.com")
        .subject("s")
        .html("<p>hi</p>")
        .send()
    )

    sent = fake.sent[0]
    assert sent.cc == ["c@example.com"]
    assert sent.bcc == ["d@example.com"]
    assert sent.body == "<p>hi</p>"


def test_a_mailable_subclass_is_built_before_capture() -> None:
    """Mirrors ``Mail._send_now``: fields set in ``build()`` are captured."""
    from cara.mail import Mailable

    class WelcomeMail(Mailable):
        def build(self):
            self.to("new@example.com")
            self.subject("Welcome")
            self.text("hello")
            return self

    fake = MailFake()
    fake.send(WelcomeMail())

    fake.assert_sent(to="new@example.com", subject="Welcome", times=1)
    assert fake.sent[0].body == "hello"


def test_send_returns_true_so_callers_branch_the_same_way() -> None:
    """Production does ``if Mail.send(...)``; a None return flips it."""
    fake = MailFake()
    assert fake.to("a@b.co").subject("s").text("b").send() is True


def test_unknown_chain_methods_still_fail() -> None:
    """The fake must not swallow a misspelled fluent call."""
    fake = MailFake()
    with pytest.raises(AttributeError):
        fake.to("a@b.co").body("this is not the spelling")  # type: ignore[attr-defined]
