"""Reading the HTTP status out of whatever an outbound call handed back.

An outbound call answers in many shapes: an ``httpx``/``requests`` response
(``status_code``), an SDK exception that carries ``.status`` or an attached
``.response``, an SDK whose only signal is an integer ``.code`` (the SP-API
``SellingApi…Exception.code == 429``), or a driver's own result object with a
``status`` field. Every retry loop, breaker and classifier needs the same
number from all of them; each one reading its own subset is how one path
retries a failure another path drops.

The rules here:

- ``.status`` / ``.status_code`` on the object, then on its ``.response``;
- an integer ``.code`` in the HTTP range (100..599) — never a ``bool``, and
  never an SDK's catch-all like ``999``;
- a mapping is read through the same keys (``status`` / ``status_code``), so
  a driver that answers with a plain dict is read the same way;
- anything else is ``None``: no status, never a guess.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_STATUS_KEYS = ("status", "status_code")


def _field(obj: Any, name: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def _as_status(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        status = int(value)
    except TypeError, ValueError:
        return None
    return status if 100 <= status <= 599 else None


def http_status(answer: Any) -> int | None:
    """The HTTP status ``answer`` carries, or ``None`` when it names none."""
    if answer is None:
        return None
    for name in _STATUS_KEYS:
        status = _as_status(_field(answer, name))
        if status is not None:
            return status
    response = _field(answer, "response")
    if response is not None and response is not answer:
        for name in _STATUS_KEYS:
            status = _as_status(_field(response, name))
            if status is not None:
                return status
    code = _field(answer, "code")
    if isinstance(code, int):
        return _as_status(code)
    return None


__all__ = ["http_status"]
