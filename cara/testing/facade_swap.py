"""Swap a Cara facade out for a fake, then restore the original.

Cara facades resolve via the application container on every attribute
access (see ``cara/facades/Facade.py``). At test time we don't want a
container — we want the call to land on a recording fake. Solution:
intercept ``Facade.__getattr__`` and route to the fake when one is
registered for that facade's ``key``.

Single global registry mirrors the metaclass — there's one ``Facade``
per process, so one switchboard works for the whole test suite.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterator

from cara.facades.Facade import Facade as _FacadeMeta

# Class-level dict of ``key -> fake instance``. Empty in normal runs.
_FAKES: Dict[str, Any] = {}

# Save the original metaclass __getattr__ once on first patch so we can
# unpatch cleanly.
_ORIGINAL_GETATTR = _FacadeMeta.__getattr__


def _patched_getattr(cls: Any, attribute: str) -> Any:
    """Replacement for ``Facade.__getattr__`` that prefers fakes."""
    fake = _FAKES.get(getattr(cls, "key", ""))
    if fake is not None:
        # IPython introspection hooks land here too — let them miss
        # cleanly the same way the original does.
        if attribute.startswith("_") and attribute not in ("__call__",):
            raise AttributeError(attribute)
        return getattr(fake, attribute)
    return _ORIGINAL_GETATTR(cls, attribute)


def install_patch() -> None:
    """Install the ``__getattr__`` shim on the Facade metaclass.

    Idempotent — calling twice is a no-op.
    """
    if getattr(_FacadeMeta.__getattr__, "_cara_test_patched", False):
        return
    _patched_getattr._cara_test_patched = True  # type: ignore[attr-defined]
    _FacadeMeta.__getattr__ = _patched_getattr  # type: ignore[method-assign]


def uninstall_patch() -> None:
    """Restore the original metaclass behaviour."""
    _FacadeMeta.__getattr__ = _ORIGINAL_GETATTR  # type: ignore[method-assign]
    _FAKES.clear()


def register(key: str, fake: Any) -> None:
    """Bind ``fake`` to be served whenever facade ``key`` is accessed."""
    install_patch()
    _FAKES[key] = fake


def unregister(key: str) -> None:
    _FAKES.pop(key, None)


def reset() -> None:
    _FAKES.clear()


@contextmanager
def swap(key: str, fake: Any) -> Iterator[Any]:
    """Context-manager form: ``with swap("mail", MailFake()) as m: ...``."""
    install_patch()
    previous = _FAKES.get(key)
    _FAKES[key] = fake
    try:
        yield fake
    finally:
        if previous is None:
            _FAKES.pop(key, None)
        else:
            _FAKES[key] = previous
