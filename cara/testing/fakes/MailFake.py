"""In-memory fake for the ``Mail`` facade.

Captures every ``send`` / ``raw`` / ``to`` chain so tests can assert
who got what without touching SMTP.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Optional, Union


@dataclass
class SentMail:
    to: List[str] = field(default_factory=list)
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    subject: Optional[str] = None
    body: Optional[str] = None
    template: Optional[str] = None
    context: dict = field(default_factory=dict)
    mailable: Optional[Any] = None


def _as_list(x: Union[None, str, Iterable[str]]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, str):
        return [x]
    return list(x)


class _PendingMail:
    """The fluent ``Mail.to(...).send(...)`` chain target."""

    def __init__(self, fake: "MailFake", to: List[str]) -> None:
        self._fake = fake
        self._to = to
        self._cc: List[str] = []
        self._bcc: List[str] = []

    def cc(self, addrs: Union[str, Iterable[str]]) -> "_PendingMail":
        self._cc.extend(_as_list(addrs))
        return self

    def bcc(self, addrs: Union[str, Iterable[str]]) -> "_PendingMail":
        self._bcc.extend(_as_list(addrs))
        return self

    def send(self, mailable: Any = None, **kwargs: Any) -> bool:
        # Real ``Mail.send`` returns ``bool`` — production code does
        # ``if Mail.send(...): ...`` to branch on delivery result. The
        # fake returning ``None`` flipped those branches to falsy in
        # tests, hiding regressions in delivery-failure handling.
        self._fake._record(
            SentMail(
                to=self._to,
                cc=self._cc,
                bcc=self._bcc,
                subject=kwargs.get("subject"),
                body=kwargs.get("body"),
                template=kwargs.get("template"),
                context=kwargs.get("context", {}),
                mailable=mailable,
            )
        )
        return True


class MailFake:
    """A drop-in fake for the ``Mail`` facade."""

    def __init__(self) -> None:
        self.sent: List[SentMail] = []

    def _record(self, mail: SentMail) -> None:
        self.sent.append(mail)

    # Production-side surface
    def to(self, addrs: Union[str, Iterable[str]]) -> _PendingMail:
        return _PendingMail(self, _as_list(addrs))

    def raw(self, body: str, to: Union[str, Iterable[str]], **kwargs: Any) -> bool:
        self._record(
            SentMail(
                to=_as_list(to),
                subject=kwargs.get("subject"),
                body=body,
            )
        )
        return True

    def send(self, mailable: Any, **kwargs: Any) -> bool:
        # Match ``Mail.send`` real return type so production callers
        # that branch on ``if Mail.send(...)`` exercise the same path
        # in tests as in prod.
        self._record(
            SentMail(
                to=_as_list(kwargs.get("to")),
                cc=_as_list(kwargs.get("cc")),
                bcc=_as_list(kwargs.get("bcc")),
                subject=kwargs.get("subject"),
                body=kwargs.get("body"),
                template=kwargs.get("template"),
                context=kwargs.get("context", {}),
                mailable=mailable,
            )
        )
        return True

    # ── Assertions ───────────────────────────────────────────────────

    def all(self) -> List[SentMail]:
        return list(self.sent)

    def count(self) -> int:
        return len(self.sent)

    def assert_sent(
        self,
        *,
        to: Optional[str] = None,
        subject: Optional[str] = None,
        where: Optional[Callable[[SentMail], bool]] = None,
        times: Optional[int] = None,
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
