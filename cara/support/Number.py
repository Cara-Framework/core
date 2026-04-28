"""Number helpers — generic numeric utilities used across apps.

Like ``cara.support.Str``, this module collects small, dependency-free
numeric helpers so each app doesn't grow its own ``clamp.py`` /
``round_to_step.py`` / ``percent_diff.py`` copies.
"""

from typing import Union

# NOTE: ``cara.exceptions`` is imported lazily inside ``clamp`` for the same
# reason ``cara.support.Currency`` defers ``cara.configuration`` —
# ``cara.support`` is imported during ``cara.foundation.Application`` boot
# (via ``PathManager``), and ``cara.exceptions.__init__`` itself transitively
# pulls in ``cara.foundation``. A top-level import here re-enters a
# partially-initialised ``cara.exceptions`` package.


Number = Union[int, float]


def clamp(value: Number, lo: Number, hi: Number) -> Number:
    """Clamp ``value`` into the inclusive range ``[lo, hi]``.

    Trivial helper, but sufficiently common (limit / offset bounds,
    price guards, retry counts) that every app eventually grows its
    own copy. Keeping the canonical version in cara so all callers
    agree on edge-case semantics:

    * Empty range (``lo == hi``) returns ``lo`` (== ``hi``).
    * ``value`` already in range is returned unchanged — no float
      rounding, no type coercion.
    * Inverted bounds (``lo > hi``) raise ``InvalidArgumentException``;
      silently swapping them would mask caller bugs and give surprising
      outputs in tests.

    Args:
        value: The value to clamp.
        lo: Lower bound (inclusive).
        hi: Upper bound (inclusive).

    Returns:
        ``lo`` if ``value < lo``, ``hi`` if ``value > hi``, otherwise ``value``.

    Raises:
        InvalidArgumentException: If ``lo > hi``.

    Examples:
        >>> clamp(5, 0, 10)
        5
        >>> clamp(-3, 0, 10)
        0
        >>> clamp(42, 0, 10)
        10
    """
    if lo > hi:
        from cara.exceptions import InvalidArgumentException  # lazy: see module note
        raise InvalidArgumentException(
            f"clamp(lo={lo}, hi={hi}): lower bound must be <= upper bound"
        )
    return max(lo, min(value, hi))


__all__ = ["clamp"]
