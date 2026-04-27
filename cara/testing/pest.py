"""Pest-style ``@it("...")`` and ``@describe("...")`` decorators.

These produce regular ``test_*`` functions that pytest auto-discovers,
so the underlying runner is unchanged — but tests *read* like
specifications:

    from cara.testing import it, describe, expect

    @describe("PriceValidationService")
    class _PriceValidationSuite:

        @it("rejects null prices")
        def _():
            valid, reason = PriceValidationService(...).validate(1, None)
            expect(valid).to_be_false()
            expect(reason).to_equal("Price is null")

The function name becomes ``test_<sanitized_string>``, and the class
name is overridden to ``Test<Description>`` so pytest's class
collection picks the suite up. Bodies that use ``self`` get it
threaded through unchanged.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Type, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def _slugify(text: str) -> str:
    """Turn a free-text description into a Python identifier.

    Idempotent on already-slug-shaped strings.
    """
    slug = re.sub(r"[^0-9a-zA-Z_]+", "_", text).strip("_").lower()
    if not slug:
        return "anonymous"
    if slug[0].isdigit():
        slug = "_" + slug
    return slug


def it(description: str) -> Callable[[F], F]:
    """Mark a test with a human description.

    The decorated function is renamed to ``test_<slug>`` so pytest
    discovers it. The original description is stashed on the function
    as ``__pest_description__`` for richer reporters later.
    """

    def decorator(fn: F) -> F:
        fn.__name__ = f"test_{_slugify(description)}"
        fn.__doc__ = description if not fn.__doc__ else fn.__doc__
        fn.__pest_description__ = description  # type: ignore[attr-defined]
        # Pretty representation for pytest -v output.
        fn.__qualname__ = fn.__name__
        return fn

    return decorator


def describe(description: str) -> Callable[[Type[Any]], Type[Any]]:
    """Mark a class as a Pest-style suite.

    Renames the class to ``Test<Slug>`` so pytest collects it as a
    test class, even when the class name uses Pest's ``_`` placeholder
    convention.
    """

    def decorator(cls: Type[Any]) -> Type[Any]:
        # Camel-case the slug so reporters look natural.
        slug = "".join(word.capitalize() for word in re.split(r"[^0-9a-zA-Z]+", description))
        if not slug:
            slug = "Anonymous"
        cls.__name__ = f"Test{slug}"
        cls.__qualname__ = cls.__name__
        cls.__pest_description__ = description  # type: ignore[attr-defined]
        cls.__doc__ = description if not cls.__doc__ else cls.__doc__
        return cls

    return decorator
