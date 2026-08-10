"""Boot-time proof that every exception can render its error envelope.

An exception handler turns a domain exception into an HTTP response by
reading two things: ``status_code`` for the status line, and ``to_dict()``
for the body — including the stable machine-readable ``type`` discriminator
clients switch on. A subclass that carries neither does not fail; it falls
through to the handler's generic body, and the client that was written
against a typed error silently starts receiving an untyped one.

That is a contract violation which, left alone, first shows up the day that
particular exception is first raised in production. :func:`validate_exception_envelopes`
moves it to boot: the deploy that introduces the offending subclass refuses
to start, naming it.

Usage — once per deployable that serves HTTP, from a provider's ``boot()``::

    from cara.exceptions import validate_exception_envelopes

    validate_exception_envelopes(ServiceException)

The base class itself is exempt by construction (the walk starts at its
subclasses): a base typically carries the fallback ``status_code`` and
deliberately has no ``to_dict``, landing on the handler's generic path on
purpose. Inherited envelopes count — a subclass folded onto a parent that
already renders one reuses it, which is the point of the hierarchy.
"""

from __future__ import annotations


def validate_exception_envelopes(base_cls: type) -> None:
    """Raise if any subclass of ``base_cls`` cannot render an error envelope.

    Args:
        base_cls: The application's exception base. Every transitive subclass
            reachable through ``__subclasses__`` at call time is checked, so
            call this after the modules that define them are imported —
            otherwise the walk only sees whatever the boot path happened to
            load.

    Raises:
        RuntimeError: naming every offender (sorted, qualified) and what each
            one is missing.
    """

    offenders: list[str] = []
    for cls in _all_subclasses(base_cls):
        status = getattr(cls, "status_code", None)
        if not isinstance(status, int):
            offenders.append(
                f"{cls.__module__}.{cls.__qualname__} (missing/invalid status_code)"
            )
        if not callable(getattr(cls, "to_dict", None)):
            offenders.append(f"{cls.__module__}.{cls.__qualname__} (no to_dict envelope)")

    if offenders:
        raise RuntimeError(
            f"{base_cls.__name__} subclass(es) without a canonical error "
            "envelope (status_code + to_dict) — they would map to the "
            "generic request_error body instead of a stable type token: "
            + "; ".join(sorted(offenders))
        )


def _all_subclasses(base: type) -> set[type]:
    """Every transitive subclass of ``base``, excluding ``base`` itself."""
    found: set[type] = set()
    for sub in base.__subclasses__():
        found.add(sub)
        found |= _all_subclasses(sub)
    return found


__all__ = ["validate_exception_envelopes"]
