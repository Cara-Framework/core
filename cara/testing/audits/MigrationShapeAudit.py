"""Does every migration on disk still have the shape the runner needs?

A migration that fails to parse, declares no ``Migration`` subclass, declares
two, or is missing ``up``/``down`` passes type-check and the unit suite
untouched — and then stops ``run_pending_migrations`` dead, mid-sweep, on the
one machine that had never applied it. The cheapest place to notice is a test
that reads the directory.

STATIC BY CONSTRUCTION. The obvious implementation imports each module and
looks at the classes, and that is what the previous attempt did — but
importing a migration can open a database connection at module scope, which is
exactly why the sibling ``audit_migrations`` parses with ``ast`` instead. This
audit does the same, so it stays safe in a default, boot-free suite.

The product supplies the directory; the framework owns the rule.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.testing.audits.MigrationShapeFinding import MigrationShapeFinding

#: Methods the runner calls on every migration.
REQUIRED_METHODS = ("up", "down")

#: Base-class name a migration must inherit, by convention and by runner.
MIGRATION_BASE = "Migration"


def _bases(node: ast.ClassDef) -> tuple[str, ...]:
    names: list[str] = []
    for base in node.bases:
        if isinstance(base, ast.Name):
            names.append(base.id)
        elif isinstance(base, ast.Attribute):
            names.append(base.attr)
    return tuple(names)


class MigrationShapeAudit:
    """Audit a product's migration directory for runnable shape."""

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)

    def files(self) -> list[Path]:
        """Every migration file, sorted; ``__init__.py`` is not one."""
        if not self.directory.is_dir():
            return []
        return sorted(
            path
            for path in self.directory.glob("*.py")
            if path.name != "__init__.py"
        )

    def findings(self) -> list[MigrationShapeFinding]:
        results: list[MigrationShapeFinding] = []
        for path in self.files():
            results.extend(self._audit_file(path))
        return results

    def _audit_file(self, path: Path) -> list[MigrationShapeFinding]:
        name = path.name
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except SyntaxError as exc:
            return [
                MigrationShapeFinding(
                    name, exc.lineno or 1, f"does not parse: {exc.msg}"
                )
            ]

        classes = [
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and MIGRATION_BASE in _bases(node)
        ]
        if not classes:
            return [
                MigrationShapeFinding(
                    name, 1, f"declares no {MIGRATION_BASE} subclass"
                )
            ]
        if len(classes) > 1:
            return [
                MigrationShapeFinding(
                    name,
                    classes[1].lineno,
                    f"declares {len(classes)} {MIGRATION_BASE} subclasses "
                    f"({', '.join(node.name for node in classes)}); the runner "
                    "loads exactly one",
                )
            ]

        migration = classes[0]
        defined = {
            node.name
            for node in migration.body
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        }
        return [
            MigrationShapeFinding(
                name,
                migration.lineno,
                f"{migration.name} is missing {method}()",
            )
            for method in REQUIRED_METHODS
            if method not in defined
        ]


__all__ = ["MIGRATION_BASE", "REQUIRED_METHODS", "MigrationShapeAudit"]
