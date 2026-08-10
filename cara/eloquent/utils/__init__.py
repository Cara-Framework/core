# Eloquent Utilities - Shared, clean, DRY utilities
"""
Eloquent Utilities Package

Provides shared utility functions and classes for Eloquent ORM,
following DRY principles to avoid code duplication.

``CastManager`` used to live here: a second, unreferenced casting engine
that restated the cast vocabulary with the opposite unknown-value semantics
— ``Decimal("0")`` for an uncastable decimal, ``0.0`` for a float, ``""``
for a null string — while the live engine in ``cara/eloquent/casts/``
correctly returns ``None``. Because nothing exercised it, it could never
learn that the real engine had been fixed; it was a loaded gun aimed at
whoever reached for the obvious name and got a fake zero in a money column.
Its exception handlers did not even match its own advertised behaviour
(``Decimal(str(x))`` raises ``InvalidOperation``, an ``ArithmeticError``,
which its ``except ValueError, TypeError`` never caught). Deleted rather
than reconciled: two engines is the defect. ``DateManager`` stays — it is
the live one.
"""

from .DateManager import DateManager

__all__ = [
    "DateManager",
]
