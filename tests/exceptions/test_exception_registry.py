"""One short name, one exception class — proven, not asserted in a docstring.

The registry used to keep "legacy wildcard winners": nine exception short
names were declared in two or three ``cara/exceptions/types/*.py`` modules,
``cara.exceptions`` bound one copy and ``cara.exceptions.types`` bound
another, and fourteen prefixed aliases kept the shadowed copies reachable.

Concretely wrong behaviour these tests pin:

* ``from cara.exceptions.types import ModelNotFoundException`` bound
  ``types.Eloquent.ModelNotFoundException`` while ``Model.find_or_fail``
  raised ``types.ModelExceptions.ModelNotFoundException``. The two shared
  no base below ``CaraException``, so an ``except`` clause written against
  the ``types`` path never fired and the 404 escaped as an unhandled 500.
* ``except ORMException`` — the base whose own docstring advertised it as
  "Base for ORM-related errors" — caught NONE of the errors the ORM
  raised, because everything the ORM raised descended from the other root,
  ``ModelException``. Symmetrically ``except ModelException`` missed
  ``DatabaseUnavailableException``, so the 503 unreachable-database path
  escaped every ORM-error handler.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

import cara.exceptions as barrel
import cara.exceptions.types as types_barrel

_TYPES_DIR = pathlib.Path(barrel.__file__).parent / "types"


def _class_definitions_by_name() -> dict[str, list[str]]:
    """Every top-level ``class`` in ``types/*.py``, keyed by short name."""
    homes: dict[str, list[str]] = {}
    for module_path in sorted(_TYPES_DIR.glob("*.py")):
        if module_path.name == "__init__.py":
            continue
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                homes.setdefault(node.name, []).append(module_path.name)
    return homes


def test_no_exception_short_name_is_defined_in_two_type_modules() -> None:
    """A duplicated short name makes ``except X`` depend on the import path."""
    duplicates = {
        name: modules
        for name, modules in _class_definitions_by_name().items()
        if len(modules) > 1
    }

    assert not duplicates, (
        "These exception short names are declared in more than one "
        "cara/exceptions/types module, so which class a call site catches "
        "depends on how it imported the name. Give each one a single home "
        "and migrate every caller (no aliases — §5 forbids the shim):\n  "
        + "\n  ".join(
            f"{name}: {', '.join(modules)}" for name, modules in duplicates.items()
        )
    )


def test_both_barrels_bind_every_shared_name_to_the_same_class() -> None:
    """The check that would actually have caught the divergence.

    ``cara.exceptions`` and ``cara.exceptions.types`` are both public
    import paths. A name present on both MUST be the same object, or an
    ``except`` clause silently stops matching the framework's ``raise``.
    """
    divergent = {
        name: (
            getattr(barrel, name).__module__,
            getattr(types_barrel, name).__module__,
        )
        for name in barrel.__all__
        if hasattr(types_barrel, name)
        and getattr(barrel, name) is not getattr(types_barrel, name)
    }

    assert not divergent, (
        "cara.exceptions and cara.exceptions.types resolve these names to "
        "DIFFERENT classes:\n  "
        + "\n  ".join(
            f"{name}: {left} vs {right}" for name, (left, right) in divergent.items()
        )
    )


def test_the_types_barrel_exports_every_public_name_it_owns() -> None:
    """§5.1: a public name missing from its barrel is a bug before anyone imports it.

    ``LazyLoadingViolation`` was public in ``ModelExceptions.__all__`` and
    absent from the ``types`` barrel — the same shadowing class of bug,
    one step earlier.
    """
    missing = [name for name in barrel.__all__ if name == "LazyLoadingViolation"]
    assert missing, "guard is misconfigured — LazyLoadingViolation must be public"
    assert hasattr(types_barrel, "LazyLoadingViolation")
    assert "LazyLoadingViolation" in types_barrel.__all__


@pytest.mark.parametrize(
    "name",
    [
        "ModelNotFoundException",
        "QueryException",
        "MultipleRecordsFoundException",
        "InvalidArgumentException",
        "DriverNotFoundException",
        "ConnectionNotRegisteredException",
        "DatabaseUnavailableException",
    ],
)
def test_orm_exception_catches_every_error_the_orm_raises(name: str) -> None:
    """``except ORMException`` used to catch ZERO of these — a fail-open handler."""
    assert issubclass(getattr(barrel, name), barrel.ORMException)


def test_the_model_root_descends_from_the_orm_root() -> None:
    """One taxonomy (§9), not two disjoint trees sharing only ``CaraException``."""
    assert issubclass(barrel.ModelException, barrel.ORMException)
    assert issubclass(barrel.ORMException, barrel.CaraException)


def test_a_database_error_and_an_unavailable_database_share_one_base() -> None:
    """``PostgresConnection.query`` raises both from the SAME ``except`` block.

    Before the merge their only common ancestor was ``CaraException``,
    which also matches every routing, mail and validation error — so there
    was no way to write "catch a database error from this call".
    """
    common = set(barrel.QueryException.__mro__) & set(
        barrel.DatabaseUnavailableException.__mro__
    )
    assert barrel.ORMException in common


def test_model_not_found_keeps_its_not_found_discriminator() -> None:
    """The two copies were not interchangeable: only one emitted ``type``.

    ``DefaultExceptionHandler.format_response`` branches on
    ``hasattr(exception, "to_dict")``, so the two produced different JSON
    bodies for the same 404 depending on which one a caller raised.
    """
    exception = barrel.ModelNotFoundException("Listing not found")
    assert exception.status_code == 404
    assert exception.to_dict() == {"error": "Listing not found", "type": "not_found"}


def test_database_unavailable_keeps_its_503_contract() -> None:
    """Products pin this same 503 contract in their own API suites."""
    exception = barrel.DatabaseUnavailableException("down", retry_after=1)
    assert exception.status_code == 503
    assert exception.is_http_exception is True
    assert exception.retry_after == 1


@pytest.mark.parametrize(
    "alias",
    [
        "AppRouteRegistrationException",
        "CacheDriverNotRegisteredException",
        "DriverException",
        "DriverLibraryNotFoundFromDriver",
        "DriverNotFoundFromDriver",
        "DriverQueueException",
        "EloquentDriverNotFoundException",
        "EloquentInvalidArgumentException",
        "EloquentModelNotFoundException",
        "EloquentMultipleRecordsFoundException",
        "EloquentQueryException",
        "HttpRouteMiddlewareNotFoundException",
        "QueueDriverNotRegisteredException",
        "RoutingRouteRegistrationException",
    ],
)
def test_the_legacy_shadow_aliases_are_gone(alias: str) -> None:
    """Every one existed only to keep a duplicate class reachable.

    §5 forbids the shim outright, and all fourteen had zero consumers in
    the framework or in any product built on it. Re-adding one means a
    duplicate came back.
    """
    assert alias not in barrel.__all__
    assert alias not in types_barrel.__all__


def test_route_registration_failures_share_one_class() -> None:
    """``ControllerMethodNotFoundException`` descended from the OTHER copy.

    ``Application.boot`` does ``isinstance(e, RouteRegistrationException)``
    to report "route configuration" rather than a generic startup error;
    with two disjoint copies it missed every missing-controller-method
    failure, and ``RouteResolver`` had to name both classes in a single
    ``except`` clause to catch its own errors.
    """
    assert issubclass(
        barrel.ControllerMethodNotFoundException, barrel.RouteRegistrationException
    )


@pytest.mark.parametrize(
    ("name", "expected_module"),
    [
        ("DriverNotRegisteredException", "cara.exceptions.types.storage"),
        ("DriverLibraryNotFoundException", "cara.exceptions.types.scheduling"),
        ("QueueException", "cara.exceptions.types.queue"),
        ("RouteMiddlewareNotFoundException", "cara.exceptions.types.routing"),
        ("RouteRegistrationException", "cara.exceptions.types.application"),
        ("Http404Exception", "cara.exceptions.types.http"),
    ],
)
def test_each_surviving_name_lives_where_its_raisers_expect(
    name: str, expected_module: str
) -> None:
    """Pins the home of every name that used to have two or three."""
    assert getattr(barrel, name).__module__ == expected_module
