"""The pickle-execution probe, owned once.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).

Every cache read path must refuse a pickle payload WITHOUT unpickling it —
``pickle.loads`` runs ``__reduce__`` before any validation a caller could
perform, so "rejected" and "rejected without executing" are different claims
and only the second one is a security property. This probe proves the second.

It lived in two copies (the codec test and the Redis-driver test); the copies
could drift apart — a gadget that stops reducing to the setter turns its test
green forever — so there is one owner.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

# A dict rather than a module global: ``__reduce__`` has to name a
# module-level callable, but the flag it sets does not have to be rebound.
_STATE = {"executed": False}


def _execute() -> None:
    _STATE["executed"] = True


class PickleGadget:
    """Unpickling this object calls ``_execute`` — nothing else does."""

    def __reduce__(self):
        return (_execute, ())


@contextmanager
def pickle_probe() -> Iterator[type[PickleGadget]]:
    """Arm the probe, yield the gadget class, and assert it never ran.

    Usage::

        with pickle_probe() as gadget:
            payload = pickle.dumps(gadget())
            ...  # the read path under test must reject ``payload``
    """
    _STATE["executed"] = False
    try:
        yield PickleGadget
    finally:
        executed = _STATE["executed"]
        _STATE["executed"] = False
    assert executed is False, "a pickle payload was unpickled and its gadget ran"
