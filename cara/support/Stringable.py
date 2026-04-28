"""Stringable — fluent, chainable wrapper around a string value.

Laravel's ``Str::of("Hello World")->upper()->slug()->limit(5)`` chain
collapsed into a single Python class. Useful when a series of string
transformations would otherwise nest awkwardly::

    # Before
    title_slug = Str.slugify(Str.truncate(Str.title_case(raw), 80))

    # After
    title_slug = Str.of(raw).title_case().truncate(80).slugify().to_str()

Every method returns either a new ``Stringable`` (for further chaining)
or a primitive (for terminals like ``length()``, ``contains()``,
``is_empty()``).

Mirrors Laravel's ``Illuminate\\Support\\Stringable`` — the methods
mirror the ``Str`` static helpers but with ``self``-binding and
return-self chaining.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Union


class Stringable:
    """Fluent string wrapper. Construct via :meth:`of` or directly."""

    __slots__ = ("_value",)

    def __init__(self, value: str = "") -> None:
        self._value = "" if value is None else str(value)

    @classmethod
    def of(cls, value: str) -> "Stringable":
        """Factory — Laravel ``Str::of('hello')`` parity."""
        return cls(value)

    # ── Conversion ───────────────────────────────────────────────────

    def to_str(self) -> str:
        """Unwrap — return the underlying primitive string."""
        return self._value

    def __str__(self) -> str:  # pragma: no cover — trivial
        return self._value

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"Stringable({self._value!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Stringable):
            return self._value == other._value
        return self._value == other

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __getitem__(self, key) -> "Stringable":
        return Stringable(self._value[key])

    # ── Case transformations ─────────────────────────────────────────

    def upper(self) -> "Stringable":
        """``"hello".upper()`` → ``"HELLO"``."""
        return Stringable(self._value.upper())

    def lower(self) -> "Stringable":
        """``"HELLO".lower()`` → ``"hello"``."""
        return Stringable(self._value.lower())

    def title_case(self) -> "Stringable":
        """Title-case every word — Laravel ``Str::title``."""
        from .Str import title_case

        return Stringable(title_case(self._value))

    def snake_case(self) -> "Stringable":
        """Convert to ``snake_case`` — Laravel ``Str::snake``."""
        from .Str import snake_case

        return Stringable(snake_case(self._value))

    def kebab_case(self) -> "Stringable":
        """Convert to ``kebab-case`` — Laravel ``Str::kebab``."""
        from .Str import kebab_case

        return Stringable(kebab_case(self._value))

    def camel_case(self) -> "Stringable":
        """Convert to ``camelCase`` — Laravel ``Str::camel``."""
        from .Str import camel_case

        return Stringable(camel_case(self._value))

    def studly_case(self) -> "Stringable":
        """Convert to ``StudlyCase`` (PascalCase) — Laravel ``Str::studly``."""
        from .Str import studly_case

        return Stringable(studly_case(self._value))

    # ── Trim / pad ───────────────────────────────────────────────────

    def trim(self, chars: Optional[str] = None) -> "Stringable":
        """Strip leading + trailing whitespace (or ``chars``)."""
        return Stringable(self._value.strip(chars) if chars else self._value.strip())

    def ltrim(self, chars: Optional[str] = None) -> "Stringable":
        """Strip leading whitespace (or ``chars``)."""
        return Stringable(self._value.lstrip(chars) if chars else self._value.lstrip())

    def rtrim(self, chars: Optional[str] = None) -> "Stringable":
        """Strip trailing whitespace (or ``chars``)."""
        return Stringable(self._value.rstrip(chars) if chars else self._value.rstrip())

    def pad_left(self, length: int, pad: str = " ") -> "Stringable":
        """Right-justify to ``length`` using ``pad`` — Laravel ``padLeft``."""
        return Stringable(self._value.rjust(length, pad))

    def pad_right(self, length: int, pad: str = " ") -> "Stringable":
        """Left-justify to ``length`` using ``pad`` — Laravel ``padRight``."""
        return Stringable(self._value.ljust(length, pad))

    # ── Length / slicing ─────────────────────────────────────────────

    def length(self) -> int:
        """Return the underlying length — terminal."""
        return len(self._value)

    def is_empty(self) -> bool:
        """True if length 0 — terminal."""
        return self._value == ""

    def is_not_empty(self) -> bool:
        """Inverse of :meth:`is_empty` — terminal."""
        return self._value != ""

    def truncate(self, limit: int, suffix: str = "...") -> "Stringable":
        """Truncate to ``limit`` chars with ``suffix`` — Laravel ``Str::limit``."""
        from .Str import truncate

        return Stringable(truncate(self._value, limit, suffix))

    def limit(self, limit: int, suffix: str = "...") -> "Stringable":
        """Alias for :meth:`truncate` — Laravel name parity."""
        return self.truncate(limit, suffix)

    def substr(self, start: int, length: Optional[int] = None) -> "Stringable":
        """Substring — Laravel ``Str::substr`` parity."""
        if length is None:
            return Stringable(self._value[start:])
        end = start + length
        return Stringable(self._value[start:end])

    # ── Slugify / sanitize ───────────────────────────────────────────

    def slug(self, separator: str = "-") -> "Stringable":
        """Slugify — Laravel ``Str::slug``."""
        from .Str import slugify

        return Stringable(slugify(self._value, separator))

    def slugify(self, separator: str = "-") -> "Stringable":
        """Cara-flavored alias for :meth:`slug`."""
        return self.slug(separator)

    def sanitize(self, max_length: int = 0) -> "Stringable":
        """Strip HTML / control chars / normalize whitespace."""
        from .Str import sanitize_text

        return Stringable(sanitize_text(self._value, max_length))

    def strip_tags(self) -> "Stringable":
        """Strip HTML / dangerous block contents."""
        from .Str import strip_tags

        return Stringable(strip_tags(self._value))

    # ── Predicates (terminals) ──────────────────────────────────────

    def starts_with(self, needles: Union[str, Iterable[str]]) -> bool:
        """True if value starts with any of ``needles``."""
        from .Str import starts_with

        return starts_with(self._value, needles)

    def ends_with(self, needles: Union[str, Iterable[str]]) -> bool:
        """True if value ends with any of ``needles``."""
        from .Str import ends_with

        return ends_with(self._value, needles)

    def contains(
        self, needles: Union[str, Iterable[str]], *, ignore_case: bool = False,
    ) -> bool:
        """True if value contains any of ``needles``."""
        from .Str import contains

        return contains(self._value, needles, ignore_case=ignore_case)

    # ── Substring extraction ────────────────────────────────────────

    def before(self, needle: str) -> "Stringable":
        """Substring before first ``needle`` — Laravel ``before``."""
        from .Str import before

        return Stringable(before(self._value, needle))

    def after(self, needle: str) -> "Stringable":
        """Substring after first ``needle`` — Laravel ``after``."""
        from .Str import after

        return Stringable(after(self._value, needle))

    def between(self, start: str, end: str) -> "Stringable":
        """Substring between ``start`` and ``end``."""
        from .Str import between

        return Stringable(between(self._value, start, end))

    # ── Replace / split ─────────────────────────────────────────────

    def replace(self, search: str, replace: str) -> "Stringable":
        """Plain ``str.replace`` chain — Laravel ``replace``."""
        return Stringable(self._value.replace(search, replace))

    def replace_regex(self, pattern: str, replace: str, flags: int = 0) -> "Stringable":
        """Regex replace — Laravel ``Str::replaceMatches``."""
        return Stringable(re.sub(pattern, replace, self._value, flags=flags))

    def split(self, separator: str = " ", limit: int = -1) -> List[str]:
        """Split into list — terminal (returns plain list)."""
        return self._value.split(separator, limit) if limit >= 0 else self._value.split(separator)

    def explode(self, separator: str, limit: int = -1) -> List[str]:
        """Laravel's ``explode`` alias for :meth:`split`."""
        return self.split(separator, limit)

    # ── Mask / escape ───────────────────────────────────────────────

    def mask(self, char: str, index: int, length: int = 0) -> "Stringable":
        """Mask ``length`` chars from ``index`` with ``char``."""
        from .Str import mask

        return Stringable(mask(self._value, char, index, length))

    # ── Tap / pipe (Laravel parity) ─────────────────────────────────

    def tap(self, callback) -> "Stringable":
        """Run ``callback(self)`` and continue the chain unchanged."""
        if callback is not None:
            callback(self)
        return self

    def pipe(self, callback) -> "Stringable":
        """Pass the underlying string to ``callback`` and re-wrap result.

        Mirrors Laravel's ``Stringable::pipe`` — escape hatch for
        custom transformations that don't have a dedicated method.
        """
        result = callback(self._value)
        return Stringable(result if result is not None else "")

    def when(self, condition, callback, default=None) -> "Stringable":
        """Conditional fluent — Laravel ``Stringable::when``."""
        if callable(condition):
            condition = condition(self)
        if condition:
            if callback is not None:
                returned = callback(self)
                if returned is not None:
                    return returned if isinstance(returned, Stringable) else Stringable(returned)
        elif default is not None:
            returned = default(self)
            if returned is not None:
                return returned if isinstance(returned, Stringable) else Stringable(returned)
        return self

    def unless(self, condition, callback, default=None) -> "Stringable":
        """Inverse of :meth:`when` — Laravel ``Stringable::unless``."""
        return self.when(not condition if not callable(condition) else lambda s: not condition(s), callback, default)


__all__ = ["Stringable"]
