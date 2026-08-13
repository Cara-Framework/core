"""AsyncDispatchAudit: a coroutine nobody awaits never runs.

``Event.fire(...)`` on its own line builds a coroutine object and throws it
away. Nothing raises, nothing logs; the event simply does not happen, and the
code reads exactly like code that works. It is the quietest way to lose work
in an async codebase, and it survives review because the missing token is one
word.

WHAT COUNTS AS DISCARDED
------------------------
Only an EXPRESSION-STATEMENT call: the call is the whole statement and its
result goes nowhere. That is the shape with no innocent reading. A dispatch
whose result is assigned, returned, gathered or appended has a consumer, and
guessing at that consumer's intent is how a guard earns false positives — the
fastest route to a guard nobody trusts.

``await``, and handing the coroutine to ``create_task`` / ``ensure_future`` /
``run_until_complete``, are all consumers, so none of them are findings.

WHAT IS FRAMEWORK AND WHAT IS NOT
---------------------------------
The dispatch names default to cara's own facades (``Event.dispatch``,
``Event.fire``, ``Bus.dispatch``) and cara's ``safe_dispatch`` helper, because
those are the framework's own fire-and-forget seams. A product may extend the
inventory with its own async wrapper; it may not need to restate the rule.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Sequence
from pathlib import Path

from .DispatchFinding import DispatchFinding

#: Framework calls that return a coroutine and are routinely written as a
#: bare statement.
DISPATCH_CALLS: frozenset[str] = frozenset(
    {"Event.dispatch", "Event.fire", "Bus.dispatch", "safe_dispatch"}
)


def _dotted_name(node: ast.expr) -> str | None:
    """``Event.fire`` for an attribute chain, ``safe_dispatch`` for a name."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


class AsyncDispatchAudit:
    """Find dispatch coroutines written as bare statements."""

    def __init__(self, calls: Iterable[str] = DISPATCH_CALLS) -> None:
        self._calls = frozenset(calls)

    def scan_source(self, source: str, path: str) -> list[DispatchFinding]:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        findings: list[DispatchFinding] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
                continue
            name = self._matched_call(node.value)
            if name is not None:
                findings.append(DispatchFinding(path, node.lineno, name))
        return findings

    def _matched_call(self, call: ast.Call) -> str | None:
        """The dispatch this bare statement discards, if it discards one.

        A consumer wrapping the dispatch (``asyncio.create_task(Event.fire(...))``)
        makes the OUTER call the statement, so the dispatch is reached through
        the consumer's arguments and is not a finding.
        """
        name = _dotted_name(call.func)
        if name is None:
            return None
        # Suffix match so an aliased receiver (``self.safe_dispatch``,
        # ``cara.facades.Event.fire``) is the same call as the bare form.
        if any(
            name == watched or name.endswith(f".{watched}") for watched in self._calls
        ):
            return name
        return None

    def scan_tree(
        self, root: Path, directories: Sequence[str] | None = None
    ) -> list[DispatchFinding]:
        """Scan a whole tree, or only the named subdirectories of it."""
        roots = [root] if directories is None else [root / name for name in directories]
        findings: list[DispatchFinding] = []
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
    def report(findings: Sequence[DispatchFinding]) -> str:
        return (
            f"Found {len(findings)} discarded dispatch coroutine(s) — the work "
            f"never runs and nothing says so:\n"
            + "\n".join(str(finding) for finding in findings)
        )
