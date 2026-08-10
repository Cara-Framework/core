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
from dataclasses import dataclass
from pathlib import Path

# ``types/Base.py`` imports nothing, so the no-import promise above holds.
from cara.exceptions.types.Base import CaraException

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


class ConflictingErrorStatus(CaraException, RuntimeError):
    """One discriminator was emitted with two different HTTP statuses.

    In the taxonomy (§9), ``RuntimeError`` kept as a SECOND base for the
    build command that classifies a RuntimeError as a reportable failure.
    """


class UntypedErrorResponse(CaraException, RuntimeError):
    """An HTTP error body was emitted without a machine-readable ``type``.

    Same dual inherit, same reason, as ``ConflictingErrorStatus`` above.
    """


@dataclass(frozen=True, slots=True)
class ErrorDiscriminator:
    """One error ``type`` a client can branch on, and where it comes from."""

    type: str
    status: int | None
    source: str


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


class ErrorContractExtractor:
    """Collect the error discriminators of a framework + application pair.

    ``app_roots`` are the application's own emitters (its exception package,
    its middleware, its controllers); files and directories are both accepted.
    The framework's roots are added automatically — an application does not
    have to know where the framework raises, and cannot forget to look there.
    """

    def __init__(
        self,
        app_roots: tuple[Path, ...] | list[Path] = (),
        *,
        framework_roots: tuple[Path, ...] = FRAMEWORK_ERROR_ROOTS,
        status_hints: dict[str, int] | None = None,
    ):
        self._roots = (*framework_roots, *app_roots)
        self._hints = dict(STATUS_HINTS if status_hints is None else status_hints)

    def extract(self) -> list[ErrorDiscriminator]:
        """Every discriminator, sorted by ``type``."""
        return self._scan()[0]

    def untyped_responses(self) -> list[str]:
        """``module:function`` for every HTTP error body with no ``type``."""
        return self._scan()[1]

    def require_typed_responses(self) -> None:
        """Fail when any HTTP error body cannot be branched on."""
        holes = self.untyped_responses()
        if holes:
            raise UntypedErrorResponse(
                "HTTP error responses without a machine-readable 'type': "
                + ", ".join(holes)
            )

    def _scan(self) -> tuple[list[ErrorDiscriminator], list[str]]:
        found: dict[str, ErrorDiscriminator] = {}
        untyped: list[str] = []

        def register(source: str, error_type: str, status: int | None) -> None:
            resolved = status if status is not None else self._hints.get(error_type)
            existing = found.get(error_type)
            if existing is None:
                found[error_type] = ErrorDiscriminator(error_type, resolved, source)
                return
            if resolved is None or existing.status == resolved:
                return
            if existing.status is None:
                found[error_type] = ErrorDiscriminator(error_type, resolved, source)
                return
            raise ConflictingErrorStatus(
                f"error type {error_type!r} is emitted with status "
                f"{existing.status} ({existing.source}) and "
                f"{resolved} ({source})"
            )

        for path in self._source_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            label = path.name
            self._scan_classes(tree, label, register)
            self._scan_emissions(tree, label, register, untyped)
            self._scan_literal_bodies(tree, label, register)

        return (
            sorted(found.values(), key=lambda row: row.type),
            sorted(set(untyped)),
        )

    def discriminators(self) -> list[str]:
        """Every error ``type``, sorted — the union a client branches on."""
        return [row.type for row in self.extract()]

    def statuses(self) -> dict[str, int]:
        """``type -> HTTP status`` for every discriminator whose status is known."""
        return {row.type: row.status for row in self.extract() if row.status is not None}

    def _source_files(self) -> list[Path]:
        seen: dict[Path, None] = {}
        for root in self._roots:
            if root.is_file():
                seen.setdefault(root.resolve(), None)
                continue
            for path in sorted(root.rglob("*.py")):
                if path.name != "__init__.py":
                    seen.setdefault(path.resolve(), None)
        return sorted(seen)

    @staticmethod
    def _scan_classes(tree: ast.AST, label: str, register) -> None:
        """Typed exception classes: ``error_type``, handler fallbacks, bodies.

        A class that declares ``status_code`` lends that status to the error
        bodies it builds, which is the only place those two facts sit together.
        """
        for cls in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
            raw_status = _class_constant(cls, "status_code")
            status = raw_status if isinstance(raw_status, int) else None
            declared = _class_constant(cls, "error_type")
            if isinstance(declared, str):
                register(f"{label}:{cls.name}", declared, status)
            for constant, fallback_status in _GENERIC_FALLBACKS.items():
                value = _class_constant(cls, constant)
                if isinstance(value, str):
                    register(f"{label}:{cls.name}", value, fallback_status)
            for payload in (node for node in ast.walk(cls) if isinstance(node, ast.Dict)):
                error_type = _dict_string(payload, "type")
                if error_type and _has_key(payload, "error"):
                    register(f"{label}:{cls.name}", error_type, status)

    @staticmethod
    def _scan_emissions(tree: ast.AST, label: str, register, untyped: list[str]) -> None:
        """Inline ``response.json({...}, 4xx)`` emitters, per function scope."""
        for fn in (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            # ``payload = {...}`` one line above the emission is the common
            # shape; resolve it so the body is still readable at the call.
            local_dicts: dict[str, ast.Dict] = {}
            for node in ast.walk(fn):
                if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(
                    node.value, ast.Dict
                ):
                    targets = (
                        node.targets if isinstance(node, ast.Assign) else [node.target]
                    )
                    for target in targets:
                        if isinstance(target, ast.Name):
                            local_dicts[target.id] = node.value

            for call in (node for node in ast.walk(fn) if isinstance(node, ast.Call)):
                if not (
                    isinstance(call.func, ast.Attribute)
                    and call.func.attr == "json"
                    and call.args
                ):
                    continue
                payload: ast.AST = call.args[0]
                if isinstance(payload, ast.Name):
                    payload = local_dicts.get(payload.id, payload)
                status = _emitted_status(call)
                if status is None or status < 400 or not isinstance(payload, ast.Dict):
                    continue
                error_type = _dict_string(payload, "type")
                if error_type:
                    register(f"{label}:{getattr(call, 'lineno', 0)}", error_type, status)
                elif _has_key(payload, "error"):
                    # Named by module and function, not by line: a hole an
                    # application deliberately allows must not move every time
                    # an unrelated line above it changes.
                    untyped.append(f"{label}:{fn.name}")

    @staticmethod
    def _scan_literal_bodies(tree: ast.AST, label: str, register) -> None:
        """``{"error": ..., "type": ...}`` literals with no HTTP status.

        Socket frames and helper builders emit the same envelope without ever
        touching a status argument. The discriminator is still part of the
        contract, so it is collected with whatever status the hints settle.
        """
        for payload in (node for node in ast.walk(tree) if isinstance(node, ast.Dict)):
            error_type = _dict_string(payload, "type")
            if error_type and _has_key(payload, "error"):
                register(label, error_type, None)
