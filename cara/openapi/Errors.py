"""Discover every machine-readable error discriminator the HTTP surface emits.

A client cannot branch on an HTTP status alone — one 422 may be a field
validation failure and another a domain refusal — so the error envelope carries
a stable ``type``. Those discriminators are scattered across typed exceptions,
middleware and the handful of controller-owned protocol responses, and nothing
collected them, so every frontend hand-mirrored the list and drifted.

This module reads them from source. The framework knows its OWN error surface
(``cara.exceptions`` and the HTTP middleware); an application passes the roots
it owns. Nothing is imported: exception ``__init__`` signatures vary and
importing middleware boots providers, so a generator that imported its inputs
could not run in a fast lane.

Three shapes carry a discriminator:

* a typed exception class — ``error_type`` / ``status_code`` class constants,
* a literal error body — ``{"error": ..., "type": ...}``, anywhere,
* an inline HTTP emission — ``response.json(payload, status)`` with a 4xx/5xx
  status, including the ``payload = {...}`` local one line above.

An HTTP error body that carries ``error`` but no ``type`` is a contract hole:
a caller meets a response it cannot branch on. Those holes are reported
(:meth:`ErrorContractExtractor.untyped_responses`) rather than swallowed, and
:meth:`ErrorContractExtractor.require_typed_responses` turns them into a build
failure — the framework finds them, the application decides whether a
provider-facing endpoint is allowed to have one.
"""

from __future__ import annotations

import ast
from pathlib import Path

# ``types/Base.py`` imports nothing, so the no-import promise above holds.

# ``.../cara`` — the framework package root, resolved through the symlink a
# deployable uses in development and through the vendored copy in an image.
_CARA_ROOT = Path(__file__).resolve().parents[1]

# The framework's own error emitters. An application never has to name these:
# the framework knows where it raises.
FRAMEWORK_ERROR_ROOTS: tuple[Path, ...] = (
    _CARA_ROOT / "exceptions" / "types",
    _CARA_ROOT / "exceptions" / "handlers",
    _CARA_ROOT / "middleware" / "http",
)

# Discriminators whose status is settled by HTTP semantics rather than by the
# source that raises them, so a raise site that omits it is still typed.
STATUS_HINTS: dict[str, int] = {
    "authentication_error": 401,
    "authorization_error": 403,
    "internal_error": 500,
    "request_error": 400,
    "validation_error": 422,
}

# Class constants whose value is a discriminator the generic handler falls back
# to, mapped to the status band that fallback serves.
_GENERIC_FALLBACKS: dict[str, int] = {
    "_GENERIC_5XX_TYPE": 500,
    "_GENERIC_4XX_TYPE": 400,
}

_STATUS_KEYWORDS = frozenset({"status", "status_code"})


def _class_constant(node: ast.ClassDef, name: str) -> str | int | None:
    """Read a literal ``name = <str|int>`` assignment from a class body."""
    for statement in node.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = (
            statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        )
        if not any(
            isinstance(target, ast.Name) and target.id == name for target in targets
        ):
            continue
        value = statement.value
        if (
            isinstance(value, ast.Constant)
            and isinstance(value.value, (str, int))
            and not isinstance(value.value, bool)
        ):
            return value.value
    return None


def _dict_string(payload: ast.AST, key: str) -> str | None:
    """Read a literal string value out of a dict literal."""
    if not isinstance(payload, ast.Dict):
        return None
    for raw_key, raw_value in zip(payload.keys, payload.values, strict=False):
        if (
            isinstance(raw_key, ast.Constant)
            and raw_key.value == key
            and isinstance(raw_value, ast.Constant)
            and isinstance(raw_value.value, str)
        ):
            return raw_value.value
    return None


def _has_key(payload: ast.AST, key: str) -> bool:
    """True when a dict literal declares ``key``, whatever its value is.

    The message beside a discriminator is usually a variable — a formatted
    string, a translated constant — so requiring a literal there would miss
    most real error bodies.
    """
    if not isinstance(payload, ast.Dict):
        return False
    return any(
        isinstance(raw_key, ast.Constant) and raw_key.value == key
        for raw_key in payload.keys
    )


def _emitted_status(call: ast.Call) -> int | None:
    """The HTTP status of a ``response.json(payload, status)`` emission."""
    candidate: ast.AST | None = call.args[1] if len(call.args) > 1 else None
    if candidate is None:
        for keyword in call.keywords:
            if keyword.arg in _STATUS_KEYWORDS:
                candidate = keyword.value
                break
    if (
        isinstance(candidate, ast.Constant)
        and isinstance(candidate.value, int)
        and not isinstance(candidate.value, bool)
    ):
        return candidate.value
    return None
