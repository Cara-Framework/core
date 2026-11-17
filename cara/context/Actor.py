"""Who is acting right now — one ambient value that rides the queue hop.

A write made by a queued job used to have no actor: the request knew who
clicked (an admin, a token, a rule), the job that did the work did not, so an
audit row could record the intent at dispatch but never the outcome under
the same name. :class:`Actor` is set once at the door that knows (an auth
middleware, a rule run, a scheduler sweep), rides ``contextvars`` inside a
process and the queue envelope across the hop (:class:`~cara.context.Ambient`,
carrier ``"actor"``), and is read by whatever writes the record.

Product-blind: ``kind`` is the product's own closed word (``"admin"``,
``"user"``, ``"rule"``, ``"system"``…), ``id`` its positive id or ``None``.
No names, no e-mails: ambient values are PII-free primitives.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass

from .Ambient import Ambient

#: The envelope carrier name.
ACTOR_CARRIER = "actor"

_KIND = re.compile(r"[a-z][a-z0-9_]{0,31}")


@dataclass(frozen=True, slots=True)
class Actor:
    kind: str
    id: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or not _KIND.fullmatch(self.kind):
            raise ValueError(f"actor kind must be a short lowercase word: {self.kind!r}")
        if self.id is not None and (
            isinstance(self.id, bool) or not isinstance(self.id, int) or self.id <= 0
        ):
            raise ValueError("actor id must be a positive integer or None")


_ACTOR: ContextVar[Actor | None] = ContextVar("cara_actor", default=None)


def _encode(value: Actor) -> dict:
    return {"kind": value.kind, "id": value.id}


def _decode(raw: dict) -> Actor:
    return Actor(kind=str(raw["kind"]), id=raw.get("id"))


Ambient.register(ACTOR_CARRIER, _ACTOR, encode=_encode, decode=_decode)


def current_actor() -> Actor | None:
    """The ambient actor, or ``None`` when no door named one."""
    return _ACTOR.get()


def set_actor(value: Actor | None) -> None:
    """Name the actor for the rest of this context (a request's middleware)."""
    _ACTOR.set(value)


@contextlib.contextmanager
def acting_as(kind: str, id: int | None = None) -> Iterator[Actor]:
    """Name the actor for a block (a rule run, a sweep), then restore."""
    value = Actor(kind=kind, id=id)
    token = _ACTOR.set(value)
    try:
        yield value
    finally:
        _ACTOR.reset(token)


__all__ = [
    "ACTOR_CARRIER",
    "Actor",
    "acting_as",
    "current_actor",
    "set_actor",
]
