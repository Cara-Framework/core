from __future__ import annotations


class _Unset:
    """Marks an argument the caller did not pass (``None`` and ``0`` are values)."""

    def __repr__(self) -> str:
        return "<unset>"


class HavingExpression:
    """A helper class to manage having expressions."""

    # ROOT CAUSE: the two-argument form (``having(col, 5)`` → ``= 5``) was
    # detected with ``if equality and not value``, so a falsy VALUE read as
    # "not given": ``having("units", ">", 0)`` became ``units = '>'``. Only an
    # argument the caller really left out may shift; ``0``/``False`` stay.
    UNSET = _Unset()

    def __init__(self, column, equality=UNSET, value=UNSET, raw=False):
        self.column = column
        self.raw = raw

        if value is self.UNSET:
            if equality is self.UNSET or equality is None:
                equality, value = None, None
            else:
                equality, value = "=", equality

        self.equality = equality
        self.value = value
        self.value_type = "having"
