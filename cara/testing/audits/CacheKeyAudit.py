"""CacheKeyAudit: a cache key must carry every dimension of its answer.

``Cache.remember("tenant:settings", ...)`` inside a method that takes a tenant
id serves the FIRST tenant's settings to every tenant that asks afterwards.
The cache works perfectly; the key is the bug. Nothing fails, nothing logs,
and the damage is a correctness one — a caller reading another caller's data.

THE SHAPE THAT CANNOT BE INNOCENT
---------------------------------
A key that is a plain string CONSTANT, inside a function that takes arguments
other than ``self``/``cls``. A constant cannot vary; the arguments can. Any
key built by interpolation, concatenation, formatting or a call is excluded —
it may still be wrong, but it is no longer wrong on its face, and a guard that
guesses is a guard that gets disabled.

The receiver and method are parameters (defaulting to cara's ``Cache.remember``
facade) so a product with its own memoizing seam can point the same rule at it
rather than restating it.
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

#: Parameters that carry no dimension of their own.
IMPLICIT_PARAMETERS: frozenset[str] = frozenset({"self", "cls"})


@dataclass(frozen=True, slots=True)
class CacheKeyFinding:
    """One constant key in a parameterized function."""

    path: str
    line: int
    key: str
    function: str
    parameters: tuple[str, ...]

    def __str__(self) -> str:
        return (
            f"{self.path}:{self.line}: {self.function}({', '.join(self.parameters)}) "
            f"caches under the constant key {self.key!r} — every argument serves "
            f"the first caller's answer"
        )


def _varying_parameters(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[str, ...]:
    """Argument names that can change the answer."""
    arguments = function.args
    names = [
        argument.arg
        for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)
    ]
    if arguments.vararg:
        names.append(f"*{arguments.vararg.arg}")
    if arguments.kwarg:
        names.append(f"**{arguments.kwarg.arg}")
    return tuple(name for name in names if name not in IMPLICIT_PARAMETERS)


class CacheKeyAudit:
    """Find constant cache keys inside functions whose answer varies."""

    def __init__(self, receiver: str = "Cache", method: str = "remember") -> None:
        self._receiver = receiver
        self._method = method

    def scan_source(self, source: str, path: str) -> list[CacheKeyFinding]:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        findings: list[CacheKeyFinding] = []
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            parameters = _varying_parameters(function)
            if not parameters:
                continue
            for node in ast.walk(function):
                if not isinstance(node, ast.Call) or not self._is_remember(node):
                    continue
                key = self._constant_key(node)
                if key is not None:
                    findings.append(
                        CacheKeyFinding(path, node.lineno, key, function.name, parameters)
                    )
        return sorted(findings, key=lambda finding: finding.line)

    def _is_remember(self, call: ast.Call) -> bool:
        return (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == self._method
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == self._receiver
        )

    def _constant_key(self, call: ast.Call) -> str | None:
        """The key, when it is a plain string constant that cannot vary."""
        argument: ast.expr | None = call.args[0] if call.args else None
        for keyword in call.keywords:
            if keyword.arg == "key":
                argument = keyword.value
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            return argument.value
        return None

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[CacheKeyFinding]:
        roots = [root] if directories is None else [root / name for name in directories]
        findings: list[CacheKeyFinding] = []
        for target in roots:
            if not target.is_dir():
                continue
            for path in sorted(target.rglob("*.py")):
                if "__pycache__" in path.parts:
                    continue
                findings.extend(
                    self.scan_source(
                        path.read_text(encoding="utf-8"),
                        path.relative_to(root).as_posix(),
                    )
                )
        return findings

    @staticmethod
    def report(findings: Sequence[CacheKeyFinding]) -> str:
        return (
            f"Found {len(findings)} constant cache key(s) in parameterized "
            f"functions — one caller's answer is served to all of them:\n"
            + "\n".join(str(finding) for finding in findings)
        )
