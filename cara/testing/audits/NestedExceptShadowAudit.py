"""NestedExceptShadowAudit: an inner ``except ... as e`` never reuses the outer name.

Python deletes the ``as`` name when an ``except`` suite ends (PEP 3110, to
break the traceback cycle). An inner handler that binds the SAME name as the
handler around it therefore deletes the OUTER exception too, and the outer
suite's next ``raise e`` / ``log(e)`` is a ``NameError`` — on the error path,
the one that only runs when something already went wrong. The trap is a
language rule, so the check is the framework's.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path

from ._scan import _iter_sources
from .RuntimeSafetyFinding import RuntimeSafetyFinding


class _Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.handlers: list[str | None] = []
        self.findings: list[tuple[str, int]] = []

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802
        if node.name and node.name in self.handlers:
            self.findings.append((node.name, node.lineno))
        self.handlers.append(node.name)
        self.generic_visit(node)
        self.handlers.pop()

    def _scope(self, node: ast.AST) -> None:
        # A nested function or class is its own scope: its handlers do not
        # delete the enclosing handler's name.
        saved, self.handlers = self.handlers, []
        self.generic_visit(node)
        self.handlers = saved

    visit_FunctionDef = _scope  # noqa: N815
    visit_AsyncFunctionDef = _scope  # noqa: N815
    visit_Lambda = _scope  # noqa: N815
    visit_ClassDef = _scope  # noqa: N815


class NestedExceptShadowAudit:
    """Find nested ``except ... as NAME`` handlers that rebind an outer NAME."""

    def scan_sources(
        self, sources: Sequence[tuple[str, str]]
    ) -> list[RuntimeSafetyFinding]:
        findings: list[RuntimeSafetyFinding] = []
        for source, path in sources:
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            visitor = _Visitor()
            visitor.visit(tree)
            findings.extend(
                RuntimeSafetyFinding(path, line, f"nested except rebinds {name!r}")
                for name, line in visitor.findings
            )
        return sorted(findings, key=lambda f: (f.path, f.line))

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[RuntimeSafetyFinding]:
        return self.scan_sources(list(_iter_sources(root, directories)))

    @staticmethod
    def report(findings: Sequence[RuntimeSafetyFinding]) -> str:
        return (
            "A nested except-as reusing the outer name deletes the outer exception "
            "when the inner suite ends (NameError on the error path) — rename one:\n"
            + "\n".join(str(f) for f in findings)
        )
