"""Regression test for ``DecimalCast`` exception-handler portability.

Python 3.14 quietly relaxed the parser so that
``except ValueError, TypeError:`` no longer raises ``SyntaxError`` and
is silently re-interpreted as ``except (ValueError, TypeError):``.
Older Python versions (3.13 and earlier) still reject the bare form.
This was lurking in ``DecimalCast.get`` / ``DecimalCast.set`` and
would have crashed any downstream consumer that still runs 3.13 (e.g.
a CI image, a contributor's local venv, ``ruff``'s 3.13 parser, or
``mypy`` until it ships 3.14 support).

This test exercises both code paths to prove the cleaned-up
parenthesised tuple still catches every shape the original
``except`` claimed to handle.
"""

from __future__ import annotations

from decimal import Decimal

from cara.eloquent.casts.primitives import DecimalCast


def test_decimal_get_returns_none_for_unparseable_input():
    """``DecimalCast.get('xyz')`` previously triggered the ``except``
    branch via ``Decimal('xyz')`` → ``InvalidOperation``. The
    parenthesised ``except`` must still catch it."""
    assert DecimalCast(2).get("not-a-number") is None


def test_decimal_set_returns_none_for_unparseable_input():
    """Symmetric — set path must also catch InvalidOperation."""
    assert DecimalCast(2).set("not-a-number") is None


def test_decimal_set_returns_none_for_empty_string():
    """Whitespace-only and empty strings are NULL-equivalent (the
    branch the unrelated ``if value is None or str(value).strip() ==
    '': return None`` covers; here we just confirm it still fires)."""
    assert DecimalCast(2).set("") is None
    assert DecimalCast(2).set("   ") is None


def test_decimal_get_quantises_to_requested_precision():
    """Non-regression: the parenthesisation must not have altered the
    happy path. A 4-fraction input should quantise to 2 places when
    ``DecimalCast(2)`` is used."""
    cast = DecimalCast(2)
    result = cast.get("12.3456")
    assert result == Decimal("12.35"), result
