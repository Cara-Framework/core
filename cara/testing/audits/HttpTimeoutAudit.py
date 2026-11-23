"""HttpTimeoutAudit: every external HTTP call carries an explicit ``timeout=``.

``requests`` waits forever by default, and an ``httpx`` client built without a
``timeout`` keeps httpx's 5 s default only until someone passes ``None`` — and
a call routed through ``asyncio.to_thread(requests.get, ...)`` hides the call
from a plain grep. A hung provider then holds a worker slot (or a request
thread) until the process is killed. The rule is a property of the HTTP
libraries, not of a product, so it lives here; a product points it at its
runtime roots.

Recognised, alias-aware: ``requests.<verb>(...)`` and ``from requests import
get``; a ``requests.Session()`` bound to a name and then called — by
assignment (``s = ...``, ``self.session = ...``) or by ``with ... as s``;
``httpx.Client`` / ``httpx.AsyncClient`` construction (the client's timeout
covers its calls); ``asyncio.to_thread(requests.<verb>, ...)``. A ``timeout=``
keyword anywhere on the call (or on ``to_thread`` itself, which forwards it)
satisfies it — unless it is the literal ``None``, which is how both libraries
spell "wait forever".
"""

from __future__ import annotations

import ast
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from ._scan import _iter_sources
from .RuntimeSafetyFinding import RuntimeSafetyFinding

REQUEST_METHODS = frozenset(
    {"delete", "get", "head", "options", "patch", "post", "put", "request"}
)
HTTPX_CLIENTS = frozenset({"AsyncClient", "Client"})


@dataclass
class _Aliases:
    asyncio_modules: set[str] = field(default_factory=set)
    httpx_clients: set[str] = field(default_factory=set)
    httpx_modules: set[str] = field(default_factory=set)
    request_functions: set[str] = field(default_factory=set)
    requests_modules: set[str] = field(default_factory=set)
    session_classes: set[str] = field(default_factory=set)
    sessions: set[str] = field(default_factory=set)


def _aliases(tree: ast.Module) -> _Aliases:
    found = _Aliases()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".", 1)[0]
                target = {
                    "asyncio": found.asyncio_modules,
                    "httpx": found.httpx_modules,
                    "requests": found.requests_modules,
                }.get(alias.name)
                if target is not None:
                    target.add(bound)
        elif isinstance(node, ast.ImportFrom) and node.module in ("httpx", "requests"):
            for alias in node.names:
                bound = alias.asname or alias.name
                if node.module == "httpx" and alias.name in HTTPX_CLIENTS:
                    found.httpx_clients.add(bound)
                elif node.module == "requests" and alias.name in REQUEST_METHODS:
                    found.request_functions.add(bound)
                elif node.module == "requests" and alias.name == "Session":
                    found.session_classes.add(bound)
    for node in ast.walk(tree):
        bound: list[ast.expr] = []
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and _is_session(
            node.value, found
        ):
            bound = node.targets if isinstance(node, ast.Assign) else [node.target]
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            # ``with requests.Session() as s:`` binds a session too; read as
            # an assignment only, the calls made on ``s`` were never checked.
            bound = [
                item.optional_vars
                for item in node.items
                if item.optional_vars is not None
                and _is_session(item.context_expr, found)
            ]
        # Dotted, so a session kept on the instance (``self.session = ...``)
        # is the same binding as a local one.
        found.sessions.update(name for target in bound if (name := _dotted(target)))
    return found


def _dotted(node: ast.expr) -> str | None:
    """``s`` / ``self.session`` for a name or an attribute chain rooted in one."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _is_session(value: ast.expr | None, aliases: _Aliases) -> bool:
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    return (isinstance(func, ast.Name) and func.id in aliases.session_classes) or (
        isinstance(func, ast.Attribute)
        and func.attr == "Session"
        and isinstance(func.value, ast.Name)
        and func.value.id in aliases.requests_modules
    )


def _has_timeout(call: ast.Call) -> bool:
    """A ``timeout=`` keyword that can bound the wait.

    The literal ``None`` cannot: ``requests`` and ``httpx`` both read it as
    "no timeout", so ``timeout=None`` is the unbounded call spelled out — it
    used to satisfy the audit because only the keyword's presence was read.
    """
    return any(
        keyword.arg == "timeout"
        and not (isinstance(keyword.value, ast.Constant) and keyword.value.value is None)
        for keyword in call.keywords
    )


def call_kind(call: ast.Call, aliases: _Aliases) -> str | None:
    """What external HTTP call ``call`` is (``None`` when it is not one)."""
    func = call.func
    if isinstance(func, ast.Name):
        if func.id in aliases.httpx_clients:
            return f"httpx.{func.id}"
        if func.id in aliases.request_functions:
            return f"requests.{func.id}"
        return None
    owner = _dotted(func.value) if isinstance(func, ast.Attribute) else None
    if owner is None:
        return None
    if func.attr in HTTPX_CLIENTS and owner in aliases.httpx_modules:
        return f"httpx.{func.attr}"
    if func.attr == "to_thread" and owner in aliases.asyncio_modules and call.args:
        wrapped = call.args[0]
        if (
            isinstance(wrapped, ast.Attribute)
            and wrapped.attr in REQUEST_METHODS
            and isinstance(wrapped.value, ast.Name)
            and wrapped.value.id in aliases.requests_modules
        ):
            return f"asyncio.to_thread(requests.{wrapped.attr})"
        if isinstance(wrapped, ast.Name) and wrapped.id in aliases.request_functions:
            return f"asyncio.to_thread(requests.{wrapped.id})"
        return None
    if func.attr not in REQUEST_METHODS:
        return None
    if owner in aliases.requests_modules:
        return f"requests.{func.attr}"
    if owner in aliases.sessions:
        return f"{owner}.{func.attr}"
    return None


class HttpTimeoutAudit:
    """Find external HTTP calls that do not bound their wait."""

    def scan_sources(
        self, sources: Sequence[tuple[str, str]]
    ) -> list[RuntimeSafetyFinding]:
        findings: list[RuntimeSafetyFinding] = []
        for source, path in sources:
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            aliases = _aliases(tree)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or _has_timeout(node):
                    continue
                kind = call_kind(node, aliases)
                if kind:
                    # Reaching here WITH the keyword means it was ``None``.
                    stated = any(keyword.arg == "timeout" for keyword in node.keywords)
                    problem = "passes timeout=None" if stated else "lacks timeout="
                    findings.append(
                        RuntimeSafetyFinding(path, node.lineno, f"{kind} {problem}")
                    )
        return sorted(findings, key=lambda f: (f.path, f.line))

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[RuntimeSafetyFinding]:
        return self.scan_sources(list(_iter_sources(root, directories)))

    @staticmethod
    def report(findings: Sequence[RuntimeSafetyFinding]) -> str:
        return (
            "External HTTP calls without a bounded timeout can stall a worker "
            "slot forever — pass timeout=:\n" + "\n".join(str(f) for f in findings)
        )
