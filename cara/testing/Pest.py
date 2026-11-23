"""Pest-style ``@it("...")`` and ``@describe("...")`` decorators.

These produce regular ``test_*`` functions that pytest auto-discovers,
so the underlying runner is unchanged — but tests *read* like
specifications:

    from cara.testing import it, describe, expect

    @describe("PriceValidationService")
    class _PriceValidationSuite:

        @it("rejects null prices")
        def _(self):
            valid, reason = PriceValidationService(...).validate(1, None)
            expect(valid).to_be_false()
            expect(reason).to_equal("Price is null")

The function name becomes ``test_<sanitized_string>``, and the class
name is overridden to ``Test<Description>`` so pytest's class
collection picks the suite up. Bodies that use ``self`` get it
threaded through unchanged.

pytest collects by the name a module or class BINDS, never by ``__name__``,
so renaming alone collected nothing: the suite above reported "no tests ran"
and stayed green whatever its bodies asserted. Each decorator therefore also
binds its result under the collectable name in the namespace being defined
(see :func:`_bind_for_collection`).
"""

from __future__ import annotations

import inspect
import re
import sys
from collections.abc import Callable
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def _bind_for_collection(name: str, obj: Any) -> None:
    """Bind ``obj`` as ``name`` where the decorated definition is being made.

    Called from a decorator, so two frames up is the module or class body
    that holds the definition. A FUNCTION body is left alone: nothing collects
    from it, and a decorator applied there is being exercised directly.

    Two definitions that slug to the same name would replace one another —
    the first would silently never run — so that is an error, not a rebind.
    """
    frame = sys._getframe(2)
    if frame.f_code.co_flags & inspect.CO_OPTIMIZED:
        return
    namespace = frame.f_locals
    if namespace.get(name, obj) is not obj:
        raise ValueError(
            f"{name!r} is already defined in this scope: two descriptions "
            f"produce the same test name, and only one of them would run"
        )
    namespace[name] = obj


def _slugify(text: str) -> str:
    """Turn a free-text description into a Python identifier.

    Idempotent on already-slug-shaped strings.
    """
    slug = re.sub(r"[^0-9a-zA-Z_]+", "_", text).strip("_").lower()
    if not slug:
        return "anonymous"
    if slug[0].isdigit():
        slug = f"_{slug}"
    return slug


def it(description: str) -> Callable[[F], F]:
    """Mark a test with a human description.

    The decorated function is renamed to ``test_<slug>`` so pytest
    discovers it. The original description is stashed on the function
    as ``__pest_description__`` for richer reporters later.
    """

    def decorator(fn: F) -> F:
        # A ``def test_x`` is already collected under the name it was written
        # with; binding the slug as well would run it twice.
        placeholder = not fn.__name__.startswith("test")
        fn.__name__ = f"test_{_slugify(description)}"
        fn.__doc__ = description if not fn.__doc__ else fn.__doc__
        fn.__pest_description__ = description  # type: ignore[attr-defined]
        # Pretty representation for pytest -v output.
        fn.__qualname__ = fn.__name__
        if placeholder:
            _bind_for_collection(fn.__name__, fn)
        return fn

    return decorator


def describe(description: str) -> Callable[[type[Any]], type[Any]]:
    """Mark a class as a Pest-style suite.

    Renames the class to ``Test<Slug>`` so pytest collects it as a
    test class, even when the class name uses Pest's ``_`` placeholder
    convention.
    """

    def decorator(cls: type[Any]) -> type[Any]:
        # Camel-case the slug so reporters look natural.
        slug = "".join(
            word.capitalize() for word in re.split(r"[^0-9a-zA-Z]+", description)
        )
        if not slug:
            slug = "Anonymous"
        # As in ``it``: a ``class TestX`` is collected under its own name.
        placeholder = not cls.__name__.startswith("Test")
        cls.__name__ = f"Test{slug}"
        cls.__qualname__ = cls.__name__
        cls.__pest_description__ = description  # type: ignore[attr-defined]
        cls.__doc__ = description if not cls.__doc__ else cls.__doc__
        if placeholder:
            _bind_for_collection(cls.__name__, cls)
        return cls

    return decorator
