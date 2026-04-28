"""Macroable mixin — Laravel's ``Macroable`` trait equivalent.

Lets a class expose a ``macro()`` class-method that registers new
methods at runtime. Useful for framework extension without
subclassing — packages can attach helper methods to ``Collection``
or ``Str`` from a service provider's ``boot()`` method::

    Collection.macro("to_dict", lambda self: dict(self.items()))

    # Now every Collection instance has ``.to_dict()`` available.

Mirrors Laravel's ``Illuminate\\Support\\Traits\\Macroable``. Macros
are stored per-class (each subclass has its own registry) and looked
up via ``__getattr__`` so they don't shadow real methods. Calling an
unknown attribute that hasn't been registered raises
``AttributeError`` exactly like a normal Python class.
"""

from __future__ import annotations

from typing import Any, Callable, ClassVar, Dict


class Macroable:
    """Mixin that lets the class accept runtime-registered methods.

    Subclasses get their own ``_macros`` registry — a macro
    registered on ``Foo`` is NOT visible on ``Bar`` even if both
    inherit ``Macroable``. The registry is keyed on the concrete
    class via ``__init_subclass__``.
    """

    # Per-class macro registry. ``__init_subclass__`` shadows this
    # with a fresh dict on every subclass so registrations don't leak
    # into siblings or the base.
    _macros: ClassVar[Dict[str, Callable]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Per-subclass macro dict — assignment, not inheritance, so a
        # ``Foo.macro("x", ...)`` call doesn't bleed into siblings.
        cls._macros = {}

    @classmethod
    def macro(cls, name: str, callback: Callable) -> None:
        """Register a callable as a class-wide macro.

        ``callback`` is called with the instance as the first arg
        (``self``-style binding) plus any caller-supplied args, so
        it behaves like a real method::

            Collection.macro("sum_squares", lambda self: sum(x*x for x in self))
            collect([1, 2, 3]).sum_squares()  # → 14

        Re-registering an existing macro overwrites it without warning.
        """
        cls._macros[name] = callback

    @classmethod
    def has_macro(cls, name: str) -> bool:
        """Return True if ``name`` is registered as a macro on this class."""
        return name in cls._macros

    @classmethod
    def flush_macros(cls) -> None:
        """Remove every macro registered on this class.

        Useful in tests that register transient macros and need to
        reset state between cases.
        """
        cls._macros = {}

    def __getattr__(self, name: str) -> Any:
        """Resolve unknown attributes against the macro registry.

        Walks the MRO so a macro registered on a parent class is
        callable on a subclass — matches Laravel's parity, where a
        ``Builder`` macro is also visible on every ``Builder``
        subclass.
        """
        for klass in type(self).__mro__:
            macros = getattr(klass, "_macros", None)
            if macros and name in macros:
                callback = macros[name]
                return lambda *args, **kwargs: callback(self, *args, **kwargs)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )


__all__ = ["Macroable"]
