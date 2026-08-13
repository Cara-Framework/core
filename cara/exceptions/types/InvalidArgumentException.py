"""InvalidArgumentException."""

from __future__ import annotations

from .ModelException import ModelException


class InvalidArgumentException(ModelException, ValueError):
    """Generic invalid-argument exception.

    Also subclasses the builtin ``ValueError``: an invalid argument IS a
    value error, and a large body of callers + tests catch these with
    ``pytest.raises(ValueError)`` / ``except ValueError``. The framework
    raises ``InvalidArgumentException`` (for a precise, catchable type)
    from spots that historically raised a bare ``ValueError`` — making it
    a ``ValueError`` subclass keeps every one of those call sites working
    whether they catch the specific type or the builtin. MRO is well-formed
    (``ModelException`` → ``ORMException`` → ``CaraException`` → ``Exception``
    ← ``ValueError``).
    """

    pass
