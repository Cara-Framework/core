"""Canonical definition of ``ErrorContractExtractor``."""

from __future__ import annotations

import ast
from pathlib import Path

from .ConflictingErrorStatus import ConflictingErrorStatus
from .ErrorDiscriminator import ErrorDiscriminator
from .Errors import (
    _GENERIC_FALLBACKS,
    FRAMEWORK_ERROR_ROOTS,
    STATUS_HINTS,
    _class_constant,
    _dict_string,
    _emitted_status,
    _has_key,
)
from .UntypedErrorResponse import UntypedErrorResponse


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
