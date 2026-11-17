"""SaltedHashKeyAudit: a cache key must not come from the builtin ``hash()``.

``hash(str)`` is salted per interpreter (``PYTHONHASHSEED``), so the same
input hashes differently in every worker and every restart. A key built from
it is valid inside one process and meaningless across them: the shared cache
silently never hits, keys multiply until their TTL, and an invalidator in
another process forgets a key nobody wrote. Nothing fails; the cache just
stops working.

THE SHAPE: a cache call (``Cache.<method>``) whose key expression calls
``hash(...)`` directly, or names a variable assigned from an expression that
does. The receiver and the method set are parameters, defaulting to cara's
``Cache`` facade.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Sequence
from pathlib import Path

from ._scan import _iter_sources
from .SaltedHashKeyFinding import SaltedHashKeyFinding

#: Cache facade methods whose first argument is a key.
KEYED_CACHE_METHODS: frozenset[str] = frozenset(
    {
        "add",
        "decrement",
        "forget",
        "get",
        "has",
        "increment",
        "lock",
        "pull",
        "put",
        "remember",
        "remember_forever",
        "remember_with_negative",
    }
)


def _calls_builtin_hash(node: ast.AST) -> bool:
    return any(
        isinstance(child, ast.Call)
        and isinstance(child.func, ast.Name)
        and child.func.id == "hash"
        for child in ast.walk(node)
    )


class SaltedHashKeyAudit:
    """Find cache keys derived from the process-salted builtin ``hash()``."""

    def __init__(
        self, receiver: str = "Cache", methods: Iterable[str] = KEYED_CACHE_METHODS
    ) -> None:
        self._receiver = receiver
        self._methods = frozenset(methods)

    def scan_source(self, source: str, path: str) -> list[SaltedHashKeyFinding]:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        hashed: set[str] = set()
        for node in ast.walk(tree):
            value = getattr(node, "value", None)
            if (
                isinstance(node, (ast.Assign, ast.AnnAssign))
                and value is not None
                and _calls_builtin_hash(value)
            ):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                hashed.update(t.id for t in targets if isinstance(t, ast.Name))
        findings: list[SaltedHashKeyFinding] = []
        for node in ast.walk(tree):
            if not self._is_cache_call(node):
                continue
            key = self._key(node)
            if key is None:
                continue
            uses_hashed = any(
                isinstance(child, ast.Name) and child.id in hashed
                for child in ast.walk(key)
            )
            if _calls_builtin_hash(key) or uses_hashed:
                findings.append(SaltedHashKeyFinding(path, node.lineno, node.func.attr))
        return sorted(findings, key=lambda finding: finding.line)

    def _is_cache_call(self, node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in self._methods
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == self._receiver
        )

    @staticmethod
    def _key(call: ast.Call) -> ast.expr | None:
        for keyword in call.keywords:
            if keyword.arg == "key":
                return keyword.value
        return call.args[0] if call.args else None

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[SaltedHashKeyFinding]:
        findings: list[SaltedHashKeyFinding] = []
        for source, relative in _iter_sources(root, directories):
            findings.extend(self.scan_source(source, relative))
        return findings

    @staticmethod
    def report(findings: Sequence[SaltedHashKeyFinding]) -> str:
        return f"Found {len(findings)} cache key(s) built from hash():\n" + "\n".join(
            str(finding) for finding in findings
        )
