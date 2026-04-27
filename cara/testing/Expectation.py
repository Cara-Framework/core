"""Pest-style fluent expectations for the Cara framework.

The :class:`Expectation` class wraps a value and exposes a chainable,
read-aloud assertion API modeled on `Pest <https://pestphp.com>`_ and
Jest's `expect()`. It produces failure messages that explain *what*
failed, *what* was expected, and *what* was received — which is the
single biggest UX gap in vanilla ``unittest`` / bare ``assert``.

Example
-------
    expect(price.is_valid).to_be_true()
    expect(reason).to_equal("Price is null")
    expect(score).to_be_between(0, 100)
    expect(items).to_have_count(3).and_to_contain("apple")
    expect(lambda: service.run()).to_throw(ValueError, match="negative")

The API is intentionally large because tests should *read*. Each method
returns ``self`` so that multiple expectations can be chained on a
single subject — the ``.and_*`` aliases (``and_to_be``, ``and_to_equal``)
exist purely for fluency in chains.
"""

from __future__ import annotations

import math
import re
from typing import Any, Callable, Iterable, Optional, Pattern, Type, Union

# Sentinel marking "no value supplied" in optional kwargs without
# colliding with legitimate ``None`` arguments.
_MISSING: Any = object()


class ExpectationFailed(AssertionError):
    """Raised when an expectation fails. Subclasses ``AssertionError``
    so pytest displays it the same way as ``assert`` failures."""


def _format(value: Any, max_len: int = 200) -> str:
    """Render a value for an error message — short, readable, safe."""
    try:
        rendered = repr(value)
    except Exception:  # pragma: no cover — defensive against bad __repr__
        rendered = f"<{type(value).__name__} (unrepr-able)>"
    if len(rendered) > max_len:
        rendered = rendered[: max_len - 1] + "…"
    return rendered


class Expectation:
    """A wrapper around a subject value providing fluent assertions.

    Construct via :func:`expect` rather than directly.
    """

    __slots__ = ("_subject", "_negated", "_label")

    def __init__(self, subject: Any, *, label: Optional[str] = None) -> None:
        self._subject = subject
        self._negated = False
        self._label = label

    # ── Modifiers ────────────────────────────────────────────────────

    @property
    def not_(self) -> "Expectation":
        """Negate the next assertion. ``expect(x).not_.to_be(None)``."""
        self._negated = not self._negated
        return self

    def as_(self, label: str) -> "Expectation":
        """Attach a human label used in failure messages."""
        self._label = label
        return self

    # ── Internal: fail formatter ─────────────────────────────────────

    def _fail(self, condition: bool, message: str) -> "Expectation":
        """Apply negation and raise on failure."""
        passed = condition if not self._negated else not condition
        if passed:
            return self
        prefix = f"[{self._label}] " if self._label else ""
        negation = "NOT " if self._negated else ""
        raise ExpectationFailed(f"{prefix}Expected {negation}{message}")

    # ── Identity / equality ──────────────────────────────────────────

    def to_be(self, expected: Any) -> "Expectation":
        """Assert ``subject is expected`` (identity, not equality)."""
        return self._fail(
            self._subject is expected,
            f"value to be (is) {_format(expected)}, got {_format(self._subject)}",
        )

    def to_equal(self, expected: Any) -> "Expectation":
        """Assert ``subject == expected``."""
        return self._fail(
            self._subject == expected,
            f"value to equal {_format(expected)}, got {_format(self._subject)}",
        )

    def to_be_none(self) -> "Expectation":
        return self._fail(self._subject is None, f"value to be None, got {_format(self._subject)}")

    def to_be_true(self) -> "Expectation":
        return self._fail(self._subject is True, f"value to be True, got {_format(self._subject)}")

    def to_be_false(self) -> "Expectation":
        return self._fail(
            self._subject is False, f"value to be False, got {_format(self._subject)}"
        )

    def to_be_truthy(self) -> "Expectation":
        return self._fail(bool(self._subject), f"truthy value, got {_format(self._subject)}")

    def to_be_falsy(self) -> "Expectation":
        return self._fail(not self._subject, f"falsy value, got {_format(self._subject)}")

    # ── Numeric ──────────────────────────────────────────────────────

    def to_be_greater_than(self, n: float) -> "Expectation":
        return self._fail(
            self._subject > n,
            f"value > {_format(n)}, got {_format(self._subject)}",
        )

    def to_be_greater_than_or_equal(self, n: float) -> "Expectation":
        return self._fail(
            self._subject >= n,
            f"value >= {_format(n)}, got {_format(self._subject)}",
        )

    def to_be_less_than(self, n: float) -> "Expectation":
        return self._fail(
            self._subject < n,
            f"value < {_format(n)}, got {_format(self._subject)}",
        )

    def to_be_less_than_or_equal(self, n: float) -> "Expectation":
        return self._fail(
            self._subject <= n,
            f"value <= {_format(n)}, got {_format(self._subject)}",
        )

    def to_be_between(self, low: float, high: float) -> "Expectation":
        """Inclusive range check."""
        return self._fail(
            low <= self._subject <= high,
            f"value in [{_format(low)}, {_format(high)}], got {_format(self._subject)}",
        )

    def to_be_close_to(self, target: float, tolerance: float = 1e-9) -> "Expectation":
        """Float-friendly equality (uses ``math.isclose``)."""
        ok = math.isclose(self._subject, target, abs_tol=tolerance)
        return self._fail(
            ok,
            f"value within {tolerance} of {_format(target)}, "
            f"got {_format(self._subject)} (delta={abs(self._subject - target):g})",
        )

    # ── Type / instance ──────────────────────────────────────────────

    def to_be_instance_of(self, klass: Type[Any]) -> "Expectation":
        return self._fail(
            isinstance(self._subject, klass),
            f"instance of {klass.__name__}, got {type(self._subject).__name__}",
        )

    def to_be_a(self, klass: Type[Any]) -> "Expectation":
        """Alias for :meth:`to_be_instance_of`."""
        return self.to_be_instance_of(klass)

    # ── Containers ───────────────────────────────────────────────────

    def to_have_length(self, n: int) -> "Expectation":
        actual = len(self._subject)
        return self._fail(
            actual == n,
            f"length {n}, got length {actual}",
        )

    def to_have_count(self, n: int) -> "Expectation":
        """Alias for :meth:`to_have_length`."""
        return self.to_have_length(n)

    def to_be_empty(self) -> "Expectation":
        return self._fail(
            len(self._subject) == 0,
            f"empty container, got {_format(self._subject)}",
        )

    def to_contain(self, item: Any) -> "Expectation":
        """``item in subject``. Works for strings, lists, dicts, sets."""
        return self._fail(
            item in self._subject,
            f"container to contain {_format(item)}, got {_format(self._subject)}",
        )

    def to_contain_all(self, items: Iterable[Any]) -> "Expectation":
        items = list(items)
        missing = [i for i in items if i not in self._subject]
        return self._fail(
            not missing,
            f"container to contain all of {_format(items)}, missing {_format(missing)}",
        )

    def to_have_key(self, key: Any) -> "Expectation":
        return self._fail(
            key in self._subject,
            f"mapping to have key {_format(key)}, got keys {_format(list(self._subject.keys()))}",
        )

    def to_have_keys(self, *keys: Any) -> "Expectation":
        missing = [k for k in keys if k not in self._subject]
        return self._fail(
            not missing,
            f"mapping to have keys {_format(list(keys))}, missing {_format(missing)}",
        )

    # ── String ───────────────────────────────────────────────────────

    def to_match(self, pattern: Union[str, Pattern[str]]) -> "Expectation":
        """Regex search. ``expect(reason).to_match(r"below minimum")``."""
        compiled = re.compile(pattern) if isinstance(pattern, str) else pattern
        return self._fail(
            bool(compiled.search(self._subject)),
            f"string to match /{compiled.pattern}/, got {_format(self._subject)}",
        )

    def to_start_with(self, prefix: str) -> "Expectation":
        return self._fail(
            self._subject.startswith(prefix),
            f"string to start with {_format(prefix)}, got {_format(self._subject)}",
        )

    def to_end_with(self, suffix: str) -> "Expectation":
        return self._fail(
            self._subject.endswith(suffix),
            f"string to end with {_format(suffix)}, got {_format(self._subject)}",
        )

    # ── Callables / exceptions ───────────────────────────────────────

    def to_throw(
        self,
        exc_type: Type[BaseException] = Exception,
        *,
        match: Optional[Union[str, Pattern[str]]] = None,
    ) -> "Expectation":
        """Assert that calling ``subject()`` raises ``exc_type``.

        ``subject`` must be a zero-arg callable. If ``match`` is given,
        the exception's ``str()`` must match that regex.
        """
        if not callable(self._subject):
            raise TypeError("to_throw() requires the subject to be callable")
        try:
            self._subject()
        except exc_type as e:
            if match is not None:
                compiled = re.compile(match) if isinstance(match, str) else match
                if not compiled.search(str(e)):
                    return self._fail(
                        False,
                        f"raised {exc_type.__name__} matching /{compiled.pattern}/, "
                        f"got message {_format(str(e))}",
                    )
            return self._fail(True, "")
        except BaseException as e:
            return self._fail(
                False,
                f"to raise {exc_type.__name__}, got {type(e).__name__}: {_format(str(e))}",
            )
        return self._fail(False, f"to raise {exc_type.__name__}, but no exception was raised")

    def not_to_throw(self) -> "Expectation":
        if not callable(self._subject):
            raise TypeError("not_to_throw() requires the subject to be callable")
        try:
            self._subject()
        except BaseException as e:
            return self._fail(
                False,
                f"no exception, got {type(e).__name__}: {_format(str(e))}",
            )
        return self._fail(True, "")

    # ── Tuple convenience (services often return ``(ok, reason)``) ──

    def to_be_tuple(self, *parts: Any) -> "Expectation":
        """Assert ``subject == tuple(parts)``. Reads as
        ``expect(result).to_be_tuple(False, "Price is null")``."""
        expected = tuple(parts)
        return self._fail(
            self._subject == expected,
            f"tuple {_format(expected)}, got {_format(self._subject)}",
        )

    # ── Aliases for chaining fluency ─────────────────────────────────

    and_to_be = to_be
    and_to_equal = to_equal
    and_to_contain = to_contain
    and_to_have_length = to_have_length
    and_to_have_count = to_have_count
    and_to_have_key = to_have_key
    and_to_match = to_match


def expect(value: Any, *, label: Optional[str] = None) -> Expectation:
    """Wrap ``value`` in an :class:`Expectation`.

    Use ``label="..."`` to prefix any failure message — handy when
    multiple expectations share a subject.
    """
    return Expectation(value, label=label)
