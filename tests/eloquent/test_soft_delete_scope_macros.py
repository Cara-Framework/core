"""Regression tests for SoftDeleteScope macro signatures.

The QueryBuilder macro dispatcher (``QueryBuilder.__getattr__`` ~line
430) invokes every registered macro as::

    self._macros[attribute](self._model, self, *args, **kwargs)

i.e. ``(model, builder, *user_args)`` — the model is ALWAYS prepended,
even when the macro does not use it. Pre-fix, ``_with_trashed``,
``_only_trashed``, ``_restore``, ``_force_delete``, and
``_force_delete_query`` were defined as ``(self, builder)`` and every
``Model.with_trashed()`` call crashed with::

    TypeError: _with_trashed() takes 2 positional arguments but 3 were given

These tests:

* Lock in the ``(model, builder)`` signature for all five macros.
* Verify that calling through the macro dispatcher path supplies
  ``self._model`` automatically, so callers never need to pass it.
* Verify each macro mutates / queries the builder as documented
  (scope removal, update payload, delete delegation).
* Cover the existing internal-only ``_soft_delete_query`` (still
  ``(builder)`` — it is registered on ``set_global_scope``, not as a
  macro, so it does NOT receive the model prepend).
"""

from __future__ import annotations

import inspect
import re
from unittest.mock import MagicMock

import pytest

from cara.eloquent.scopes.SoftDeleteScope import SoftDeleteScope

# ── Signature lock-in ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "macro_name",
    [
        "_with_trashed",
        "_only_trashed",
        "_restore",
        "_force_delete",
        "_force_delete_query",
    ],
)
def test_macro_accepts_model_and_builder(macro_name):
    """The dispatcher always calls macros with ``(model, builder)``.
    A regression to the pre-fix ``(builder)`` signature would raise
    ``TypeError`` on the very first call."""
    scope = SoftDeleteScope()
    macro = getattr(scope, macro_name)
    sig = inspect.signature(macro)
    # `self` is bound; remaining must be exactly (model, builder).
    params = list(sig.parameters)
    assert params[:2] == ["model", "builder"], (
        f"{macro_name} must accept (model, builder); got {params!r}"
    )


def test_soft_delete_query_keeps_builder_only_signature():
    """``_soft_delete_query`` is wired via ``set_global_scope`` on
    DELETE, NOT via ``macro()``. set_global_scope callbacks receive
    only ``(builder)`` — the model prepend is a macro-dispatch-only
    convention. Locking this prevents an over-eager refactor from
    sweeping it into the macro signature and breaking every soft
    delete."""
    scope = SoftDeleteScope()
    sig = inspect.signature(scope._soft_delete_query)
    params = list(sig.parameters)
    assert params == ["builder"], (
        f"_soft_delete_query must stay (builder)-only; got {params!r}"
    )


# ── Macro dispatch behavior (with mock builder) ──────────────────────


def _mock_builder():
    """A duck-typed QueryBuilder stand-in that records scope ops."""
    b = MagicMock(name="builder")
    b.get_table_name.return_value = "product"
    return b


def test_with_trashed_removes_soft_delete_scope():
    scope = SoftDeleteScope()
    builder = _mock_builder()
    sentinel_model = object()

    returned = scope._with_trashed(sentinel_model, builder)

    builder.remove_global_scope.assert_called_once_with("_soft_delete", action="select")
    # Returns the builder for chaining (.with_trashed().where(...).get())
    assert returned is builder


def test_only_trashed_removes_scope_and_filters_for_deleted():
    scope = SoftDeleteScope()
    builder = _mock_builder()

    scope._only_trashed(None, builder)

    builder.remove_global_scope.assert_called_once_with("_soft_delete", action="select")
    builder.where_not_null.assert_called_once_with("product.deleted_at")


def test_restore_removes_scope_and_clears_deleted_at():
    scope = SoftDeleteScope()
    builder = _mock_builder()

    scope._restore(None, builder)

    builder.remove_global_scope.assert_called_once_with("_soft_delete", action="select")
    builder.update.assert_called_once_with({"deleted_at": None})


def test_force_delete_strips_both_scopes_and_deletes():
    scope = SoftDeleteScope()
    builder = _mock_builder()

    scope._force_delete(None, builder)

    # Both the select-side scope AND the delete-override must go,
    # otherwise the override would rewrite our hard DELETE into a
    # soft UPDATE again.
    assert builder.remove_global_scope.call_count == 2
    calls = builder.remove_global_scope.call_args_list
    assert calls[0].args[0] == "_soft_delete"
    assert calls[0].kwargs == {"action": "select"}
    assert calls[1].args[0] == "_soft_delete_delete"
    assert calls[1].kwargs == {"action": "delete"}
    builder.delete.assert_called_once_with()


def test_force_delete_query_strips_scopes_and_returns_builder():
    """``_force_delete_query`` returns the unprotected builder
    without executing — for batch operations like
    ``Listing.force_delete_query().where(...).delete()``."""
    scope = SoftDeleteScope()
    builder = _mock_builder()

    returned = scope._force_delete_query(None, builder)

    assert builder.remove_global_scope.call_count == 2
    builder.delete.assert_not_called()
    assert returned is builder


# ── Custom deleted_at column propagation ─────────────────────────────


def test_custom_deleted_at_column_used_in_only_trashed():
    scope = SoftDeleteScope(deleted_at_column="archived_at")
    builder = _mock_builder()
    builder.get_table_name.return_value = "listing"

    scope._only_trashed(None, builder)

    builder.where_not_null.assert_called_once_with("listing.archived_at")


def test_custom_deleted_at_column_used_in_restore():
    scope = SoftDeleteScope(deleted_at_column="archived_at")
    builder = _mock_builder()

    scope._restore(None, builder)

    builder.update.assert_called_once_with({"archived_at": None})


# ── _soft_delete_query (the macro that stayed (builder)-only) ────────


def test_soft_delete_query_calls_update_with_iso_timestamp():
    """The non-macro callback that converts ``.delete()`` into a
    soft delete. Must call ``builder.update`` with the configured
    column set to an ISO-ish timestamp string."""
    scope = SoftDeleteScope()
    builder = _mock_builder()
    builder._model = None  # bypass model branch → uses pendulum fallback

    scope._soft_delete_query(builder)

    builder.update.assert_called_once()
    payload = builder.update.call_args.args[0]
    assert "deleted_at" in payload
    stamp = payload["deleted_at"]
    # ISO-ish: YYYY-MM-DD HH:MM:SS (no Z because to_datetime_string)
    assert re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", stamp), stamp


def test_soft_delete_query_uses_model_timestamp_helper_when_available():
    """When the builder has a model, prefer the model's
    ``get_new_datetime_string()`` for consistency with INSERT/UPDATE
    timestamps the ORM already generates."""
    scope = SoftDeleteScope()
    builder = _mock_builder()
    fake_model = MagicMock(name="model")
    fake_model.get_new_datetime_string.return_value = "2026-05-23 12:00:00"
    builder._model = fake_model

    scope._soft_delete_query(builder)

    fake_model.get_new_datetime_string.assert_called_once()
    builder.update.assert_called_once_with({"deleted_at": "2026-05-23 12:00:00"})


# ── End-to-end: dispatcher signature compatibility ───────────────────


def test_dispatcher_three_arg_call_pattern_does_not_raise():
    """Simulate exactly what ``QueryBuilder.__getattr__`` does:

        self._macros[attribute](self._model, self, *args, **kwargs)

    Every macro must be callable with ``(model, builder)`` positional
    args. A regression to the old ``(builder)`` arity raises
    TypeError here — this is the same exception users hit at runtime
    pre-fix."""
    scope = SoftDeleteScope()
    builder = _mock_builder()
    model = object()

    # No assertion needed — TypeError on signature mismatch is the
    # failure mode this test guards against.
    scope._with_trashed(model, builder)
    scope._only_trashed(model, builder)
    scope._restore(model, builder)
    scope._force_delete(model, builder)
    scope._force_delete_query(model, builder)


def test_on_boot_registers_all_five_macros():
    """on_boot wires every public-facing soft-delete method into the
    builder's macro table. If one is dropped, callers see
    ``AttributeError`` instead of TypeError — equally broken."""
    scope = SoftDeleteScope()
    builder = MagicMock(name="builder")

    scope.on_boot(builder)

    macro_names = [c.args[0] for c in builder.macro.call_args_list]
    assert set(macro_names) == {
        "with_trashed",
        "only_trashed",
        "restore",
        "force_delete",
        "force_delete_query",
    }


def test_on_boot_registers_select_and_delete_scopes():
    scope = SoftDeleteScope()
    builder = MagicMock(name="builder")

    scope.on_boot(builder)

    scope_calls = [
        (c.args[0], c.kwargs.get("action"))
        for c in builder.set_global_scope.call_args_list
    ]
    assert ("_soft_delete", "select") in scope_calls
    assert ("_soft_delete_delete", "delete") in scope_calls


def test_on_remove_strips_both_scopes():
    scope = SoftDeleteScope()
    builder = MagicMock(name="builder")

    scope.on_remove(builder)

    scope_calls = [
        (c.args[0], c.kwargs.get("action"))
        for c in builder.remove_global_scope.call_args_list
    ]
    assert ("_soft_delete", "select") in scope_calls
    assert ("_soft_delete_delete", "delete") in scope_calls
