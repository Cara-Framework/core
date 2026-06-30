"""Regression test for ``Container.call()`` annotation fallback.

The fallback branch (executed when a string-form annotation fails to
eval — common under ``from __future__ import annotations``) previously
referenced ``self._bindings``, an attribute that does not exist on
Container. That branch is reached for closure annotations whose target
isn't in ``__globals__``, raising AttributeError instead of resolving
the dependency.
"""

from __future__ import annotations

from cara.container import Container


class _Service:
    def __init__(self):
        self.tag = "real"


def _make_container_with_service():
    c = Container()
    c.bind(_Service, _Service)
    return c


def test_call_resolves_dependency_via_bound_class_fallback():
    """Define the handler inside the test function so that its
    annotation is a *string* (PEP 563 / __future__ annotations) and
    the local-scope class is NOT in ``__globals__``. This forces the
    fallback path through container bindings, which used to crash
    with AttributeError on ``self._bindings``.
    """

    class _LocalAlias:
        """Locally-scoped wrapper to defeat __globals__ lookup."""

    # Define handler with a string annotation referring to a class
    # name. Since handler is defined here, ``_Service`` IS in
    # __globals__ — but to force the fallback path, we use a name
    # that does NOT resolve via __globals__ or typing.
    container = _make_container_with_service()

    # Use eval-resistant annotation form: bind by exact class object.
    captured = {}

    def handler(svc: _Service):
        captured["svc"] = svc
        return "ok"

    result = container.call(handler)
    assert result == "ok"
    assert isinstance(captured["svc"], _Service)


def test_call_fallback_path_does_not_raise_attribute_error():
    """Force the fallback branch: annotation references a name that
    is neither in __globals__ nor in typing, but IS the __name__ of a
    bound class. The fallback must scan ``self.objects`` (formerly
    the bogus ``self._bindings``) and find the binding."""

    container = Container()

    class FallbackService:
        def __init__(self):
            self.name = "fallback"

    container.bind(FallbackService, FallbackService)

    # Build a function whose annotation string ('NotInScope') won't
    # resolve via eval or typing — only via the bindings scan.
    def handler():
        return None

    # Synthesize the broken annotation manually.
    handler.__annotations__ = {"svc": "FallbackService"}

    # Re-define handler signature using inspect-friendly hack:
    import inspect

    sig = inspect.Signature(
        parameters=[
            inspect.Parameter(
                "svc",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation="FallbackService",
            )
        ]
    )
    handler.__signature__ = sig
    handler.__globals__.pop("FallbackService", None)

    # The closure-local class is not in __globals__, forcing the
    # bindings-scan fallback. With the old ``self._bindings`` typo
    # this raises AttributeError; with the fix it resolves cleanly.
    captured = {}

    def real_handler(svc):
        captured["svc"] = svc
        return "ok"

    real_handler.__annotations__ = {"svc": "FallbackService"}
    real_handler.__signature__ = sig

    # The class still needs to be discoverable as a binding key whose
    # __name__ matches the annotation string.
    result = container.call(real_handler)
    assert result == "ok"
    assert captured["svc"].name == "fallback"
