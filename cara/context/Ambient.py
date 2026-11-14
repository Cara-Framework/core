"""Ambient context that crosses the queue hop — like ``_tenant``, for app facts.

An application registers a named carrier: a ``ContextVar`` plus how its value
becomes a JSON primitive and back. ``capture()`` reads every registered var
that is set (dispatch time, inside the caller's context); the queue envelope
signs the result as ``_ambient``; the worker's middleware pipeline restores
it around the job body with ``restored()``. Inside one process the vars ride
``contextvars`` already (``ExecutionContext.run_in_thread`` copies the
context), so an inline ``Bus.dispatch`` needs nothing.

Values are small, PII-free primitives (ids and closed words). A carrier that
fails to encode or decode is skipped, never fatal: ambient context informs a
record, it never decides whether a job runs.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

#: Envelope key the queue signs the captured carriers under.
AMBIENT_KEY = "_ambient"
_MAX_CARRIERS = 16


@dataclass(frozen=True, slots=True)
class _Carrier:
    var: ContextVar
    encode: Callable[[Any], Any]
    decode: Callable[[Any], Any]


class Ambient:
    """Registry of context vars that ride the queue envelope."""

    _carriers: dict[str, _Carrier] = {}

    @classmethod
    def register(
        cls,
        name: str,
        var: ContextVar,
        *,
        encode: Callable[[Any], Any],
        decode: Callable[[Any], Any],
    ) -> None:
        """Declare ``var`` as carried under ``name``. Re-registering a name
        with the same var is a no-op; another var under it is an error."""
        current = cls._carriers.get(name)
        if current is not None:
            if current.var is not var:
                raise ValueError(f"Ambient carrier {name!r} is already registered.")
            return
        if len(cls._carriers) >= _MAX_CARRIERS:
            raise ValueError("Too many ambient carriers.")
        cls._carriers[name] = _Carrier(var=var, encode=encode, decode=decode)

    @classmethod
    def capture(cls) -> dict[str, Any]:
        """Every registered var that holds a value now, encoded."""
        captured: dict[str, Any] = {}
        for name, carrier in cls._carriers.items():
            value = carrier.var.get(None)
            if value is None:
                continue
            try:
                encoded = carrier.encode(value)
            except Exception:
                continue
            if encoded is not None:
                captured[name] = encoded
        return captured

    @classmethod
    @contextlib.contextmanager
    def restored(cls, captured: Any) -> Iterator[None]:
        """Set each carried var from ``captured`` for the block, then reset."""
        tokens: list[tuple[ContextVar, Any]] = []
        if isinstance(captured, dict):
            for name, raw in captured.items():
                carrier = cls._carriers.get(str(name))
                if carrier is None:
                    continue
                try:
                    value = carrier.decode(raw)
                except Exception:
                    continue
                if value is not None:
                    tokens.append((carrier.var, carrier.var.set(value)))
        try:
            yield
        finally:
            for var, token in reversed(tokens):
                var.reset(token)


__all__ = ["AMBIENT_KEY", "Ambient"]
