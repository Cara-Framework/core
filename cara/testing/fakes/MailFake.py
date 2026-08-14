"""In-memory fake for the ``Mail`` facade.

Captures every send so tests can assert who got what without touching SMTP.

The fluent chain is the REAL ``MailMessage``; this fake does not
re-implement it. A double that re-implements a surface drifts from it, and
drift in the PERMISSIVE direction is the dangerous one — the double accepts
a call the production object rejects, so writing a test for the broken code
is what keeps it green.

That is not hypothetical. This fake used to expose ``raw(body, to, …)``,
which ``cara.mail.Mail`` has never had. Three cheapa operator-alert jobs
(scrape health, credit budget, canary drift) called ``Mail.raw(...)``; every
real send raised ``AttributeError`` into the surrounding ``except`` and
became a one-line warning, so those "wake a human, the scraper is broken"
emails had never once been delivered. The same fake was simultaneously too
NARROW — its chain object had no ``subject`` / ``text`` — so correct code
would have failed under test too.

Delegating to ``MailMessage`` makes both directions structurally impossible:
the chain IS the production chain, and ``send`` mirrors ``Mail._send_now``'s
``set_application`` → ``build`` order so a ``Mailable`` subclass populates
its fields exactly as it would in production.

Driver plumbing (``add_driver`` / ``driver`` / ``set_default_driver``) is
deliberately absent: a fake has no drivers, and being NARROWER than the real
object fails loudly at the call site instead of hiding a defect.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from cara.mail import Mailable, MailMessage, MailPendingSend

from .SentMail import SentMail


def _as_list(x: None | str | Iterable[str]) -> list[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [x]
    return list(x)


class MailFake:
    """A drop-in fake for the ``Mail`` facade."""

    #: ``MailMessage.queue()`` and ``Mail._send_now`` read the manager's
    #: application to render views. There is no container in a fake.
    application: Any = None

    def __init__(self) -> None:
        self.sent: list[SentMail] = []

    def _record(self, mail: SentMail) -> None:
        self.sent.append(mail)

    # ── Production-side surface ──────────────────────────────────────

    def to(self, addrs: str | Iterable[str]) -> MailMessage:
        """Begin the real fluent chain, bound to this fake as its manager."""
        return MailMessage(self).to(addrs)

    def mailable(self, mailable: Mailable) -> MailPendingSend:
        """Mirror ``Mail.mailable`` — the chain target that only sends."""
        return MailPendingSend(self, mailable)

    def send(self, mailable: Mailable, driver_name: str | None = None) -> bool:
        """Record the mailable instead of handing it to a driver.

        Returns ``bool`` like the real ``Mail.send`` so production callers
        that branch on ``if Mail.send(...)`` take the same path in tests.
        """
        self._record(self._capture(mailable))
        return True

    # ── Capture ──────────────────────────────────────────────────────

    def _capture(self, mailable: Any) -> SentMail:
        """Read a built mailable into a ``SentMail`` row.

        Mirrors ``Mail._send_now``: set the application, then ``build()``,
        so a ``Mailable`` subclass that fills its fields in ``build`` is
        captured with them — the same values a driver would receive.
        Fields are read off the instance rather than via ``to_dict()``
        because that renders the HTML view, which needs a real application.
        """
        if isinstance(mailable, Mailable):
            mailable.set_application(self.application)
            mailable.build()

        return SentMail(
            to=_as_list(getattr(mailable, "_to", None)),
            cc=_as_list(getattr(mailable, "_cc", None)),
            bcc=_as_list(getattr(mailable, "_bcc", None)),
            subject=getattr(mailable, "_subject", None),
            body=getattr(mailable, "_text", None) or getattr(mailable, "_html", None),
            template=getattr(mailable, "_view", None),
            context=getattr(mailable, "_view_data", None) or {},
            mailable=mailable,
        )

    # ── Assertions ───────────────────────────────────────────────────

    def all(self) -> list[SentMail]:
        return list(self.sent)

    def count(self) -> int:
        return len(self.sent)

    def assert_sent(
        self,
        *,
        to: str | None = None,
        subject: str | None = None,
        where: Callable[[SentMail], bool] | None = None,
        times: int | None = None,
    ) -> None:
        matches = self.sent
        if to is not None:
            matches = [m for m in matches if to in m.to or to in m.cc or to in m.bcc]
        if subject is not None:
            matches = [m for m in matches if m.subject == subject]
        if where is not None:
            matches = [m for m in matches if where(m)]
        if times is not None and len(matches) != times:
            raise AssertionError(
                f"Expected mail to match {times} time(s), got {len(matches)}"
            )
        if times is None and not matches:
            raise AssertionError(
                f"Expected mail to be sent (to={to!r}, subject={subject!r}); none matched"
            )

    def assert_nothing_sent(self) -> None:
        if self.sent:
            raise AssertionError(f"Expected no mail to be sent, got {len(self.sent)}")

    def clear(self) -> None:
        self.sent.clear()
