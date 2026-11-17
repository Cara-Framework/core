"""Constructor coercion for numeric annotations at a JSON boundary.

A queue payload is JSON. A job dispatched with ``product_id=7`` can come back
as ``"7"`` whenever the publisher serialised it as a string (an HTTP body, a
webhook, a scheduler entry read from config). A constructor that annotates the
parameter ``int`` and hands the value on fails SILENTLY: ``"7" == 7`` is never
true, so an "exclude this id" argument excludes nothing, and the ``:.2f``
format of a price raises "Unknown format code 'f'".

``install_numeric_coercion(cls)`` makes the annotation true at construction:
it wraps ``cls.__init__`` once so a numeric-annotated string argument is parsed
before the constructor body runs. A job base class calls it from
``__init_subclass__``; the queue path and the in-process path then agree.

The rule is deliberately narrow:

* only ``int`` / ``float`` annotations and their optional unions take part;
* only ``str`` values are touched (a real number, ``None`` or a bool passes);
* an unparseable string is left exactly as it arrived, so the constructor's
  own validation still rejects it;
* a genuine argument error (missing / unexpected argument) is raised by the
  real constructor, with its own message, never by the wrapper;
* ``functools.wraps`` keeps ``inspect.signature`` on the ORIGINAL parameters,
  which is what ``cara.queues.Bus`` walks to decide what rides the envelope.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any

__all__ = ["install_numeric_coercion", "numeric_annotation_kind"]

#: Marker on a wrapped ``__init__``; a second installation is a no-op.
_INSTALLED_MARKER = "_cara_numeric_coercion_installed"

_INT_ANNOTATIONS = frozenset({"int", "int|none", "none|int", "optional[int]"})
_FLOAT_ANNOTATIONS = frozenset(
    {
        "float",
        "float|none",
        "none|float",
        "optional[float]",
        "int|float",
        "float|int",
    }
)


def numeric_annotation_kind(annotation: Any) -> str | None:
    """Return ``"int"`` / ``"float"`` for a numeric annotation, else None.

    ``from __future__ import annotations`` leaves annotations as STRINGS, so
    the textual form is authoritative; a live class is reduced to its
    ``__name__`` and anything else (a union, whose ``__name__`` is just
    ``"Union"`` on 3.14) to its ``str`` for the same comparison, and a
    ``typing.`` prefix is ignored so ``typing.Optional[int]`` reads as
    ``Optional[int]``.
    """
    if annotation is inspect.Parameter.empty:
        return None
    text = (
        annotation
        if isinstance(annotation, str)
        else annotation.__name__
        if isinstance(annotation, type)
        else str(annotation)
    )
    normalized = text.strip().lower().replace(" ", "").replace("typing.", "")
    if normalized in _INT_ANNOTATIONS:
        return "int"
    if normalized in _FLOAT_ANNOTATIONS:
        return "float"
    return None


def _coerce(value: Any, kind: str) -> Any:
    """Parse a numeric string; leave anything else (or garbage) untouched."""
    if not isinstance(value, str) or not value.strip():
        return value
    try:
        return int(value.strip()) if kind == "int" else float(value.strip())
    except TypeError, ValueError:
        return value


def install_numeric_coercion(cls: type) -> None:
    """Wrap ``cls.__init__`` once so numeric-annotated string args are coerced.

    A class with no ``__init__`` of its own inherits an already-wrapped one,
    and the marker refuses a second installation, so this is safe to call from
    ``__init_subclass__`` for every class in a tree. A constructor that
    annotates nothing numeric is left untouched (no wrapper, no cost).
    """
    init = cls.__dict__.get("__init__")
    if init is None or getattr(init, _INSTALLED_MARKER, False):
        return
    try:
        signature = inspect.signature(init)
    except TypeError, ValueError:
        return
    plan = {
        name: kind
        for name, parameter in signature.parameters.items()
        if name != "self"
        and (kind := numeric_annotation_kind(parameter.annotation)) is not None
    }
    if not plan:
        return

    @functools.wraps(init)
    def _coercing_init(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            bound = signature.bind_partial(self, *args, **kwargs)
        except TypeError:
            return init(self, *args, **kwargs)
        for name, kind in plan.items():
            if name in bound.arguments:
                bound.arguments[name] = _coerce(bound.arguments[name], kind)
        return init(*bound.args, **bound.kwargs)

    setattr(_coercing_init, _INSTALLED_MARKER, True)
    cls.__init__ = _coercing_init  # type: ignore[misc]
