"""LazyLoadingViolation."""

from __future__ import annotations

from .ModelException import ModelException


class LazyLoadingViolation(ModelException):
    """Thrown when an accidental lazy-load is caught by the strict guard.

    Opt-in via ``Model.prevent_lazy_loading()`` (OFF by default). When
    enabled, accessing an un-eager-loaded relationship on a model that
    came from a multi-row fetch raises this instead of silently issuing
    an N+1 query — surfacing the missing ``.with_(...)`` in dev/test
    before it reaches production.
    """

    pass
