"""NumericTruthinessAudit: a numeric zero is DATA, never absence.

Two source shapes silently convert a legitimate ``0`` into "missing":

* ``value if row.price else fallback`` — a zero price takes the ``else``
  branch as though the field were unset;
* ``row.quantity or 99`` — a zero quantity is replaced by the default.

Both read as null-safety and neither is. The correct form states the question
being asked: ``row.price if row.price is not None else fallback``.

Coalescing to zero (``row.quantity or 0``) is exempt, because ``0 or 0`` is
``0`` — the idiom cannot corrupt what it is defaulting.

An ``or`` chain read as a CONDITION (``if sig.phash or sig.width``) is exempt
too: there it asks "is any of these present", and a zero answers that question
the same way an absent value does. Only an ``or`` whose VALUE is kept can
replace a zero with something else.

WHAT IS FRAMEWORK AND WHAT IS NOT
---------------------------------
The MECHANISM is framework-shaped: "an attribute access in a truthiness
position, where falsy and absent are different things" is a Python fact, not a
product fact. The FIELD NAMES are the opposite — ``price``/``margin``/``fee``
in one product, ``score``/``rating`` in another — so the audit takes them as a
parameter and ships no defaults. Merging two products' vocabularies here would
manufacture false positives in both.

Reading the AST rather than lines is what makes the exemptions honest: an
``is not None`` guard is a different node, not a substring that happens to
appear somewhere on the line, and a comment mentioning ``price or 0`` is not
code at all.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

#: Default operands that cannot corrupt a genuine zero.
SAFE_DEFAULTS: frozenset[object] = frozenset({0})


@dataclass(frozen=True, slots=True)
class TruthinessFinding:
    """One zero-corrupting site, reported against a product-relative path."""

    path: str
    line: int
    field: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


def _is_safe_default(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value in SAFE_DEFAULTS


def _truth_test_expressions(tree: ast.AST) -> set[int]:
    """``id()`` of every expression whose VALUE is consumed as a truth test.

    ``a.width or a.file_size`` means two different things depending on where
    it sits. As a value it DEFAULTS — a zero width is replaced, which is the
    bug this audit exists to find. As a condition (``if sig.phash or
    sig.width or sig.file_size``) it asks "is any of these present", and a
    zero contributes nothing to that question that an absent value would not
    contribute anyway. Flagging the second form is a false positive, and a
    false positive on an existence check is expensive: the honest fix is to
    rewrite a boolean chain into something worse.

    The context propagates through boolean operators and ``not``, because
    ``if sig and (sig.width or sig.file_size)`` asks the same question of the
    inner chain that it asks of the outer one.
    """
    marked: set[int] = set()

    def mark(node: ast.expr | None) -> None:
        if node is None:
            return
        marked.add(id(node))
        if isinstance(node, ast.BoolOp):
            for value in node.values:
                mark(value)
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            mark(node.operand)

    for node in ast.walk(tree):
        if isinstance(node, ast.If | ast.While | ast.IfExp | ast.Assert):
            mark(node.test)
        elif isinstance(node, ast.comprehension):
            for condition in node.ifs:
                mark(condition)
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            mark(node.operand)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "bool"
            and node.args
        ):
            mark(node.args[0])
    return marked


def _attribute_field(node: ast.expr, fields: frozenset[str]) -> str | None:
    """The watched field name this expression reads, if it reads one.

    Only an ATTRIBUTE access counts. A bare local named ``price`` may be
    anything — a loop index, a parsed argument, a Decimal already defaulted —
    while ``row.price`` names a column whose zero the database means.
    """
    if isinstance(node, ast.Attribute) and node.attr in fields:
        return node.attr
    return None


class NumericTruthinessAudit:
    """Find zero-corrupting truthiness on a product's numeric columns."""

    def __init__(self, fields: Iterable[str]) -> None:
        self._fields = frozenset(fields)
        if not self._fields:
            raise ValueError(
                "NumericTruthinessAudit needs the product's numeric column names; "
                "an empty field set would silently pass everything"
            )

    @property
    def fields(self) -> frozenset[str]:
        return self._fields

    def scan_source(self, source: str, path: str) -> list[TruthinessFinding]:
        """Every zero-corrupting site in one module's source."""
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return []
        findings: list[TruthinessFinding] = []
        conditions = _truth_test_expressions(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.IfExp):
                field = _attribute_field(node.test, self._fields)
                if field:
                    findings.append(
                        TruthinessFinding(
                            path,
                            node.lineno,
                            field,
                            f"`... if ....{field} else ...` — a zero {field} takes "
                            f"the else branch; test `is not None`",
                        )
                    )
            elif (
                isinstance(node, ast.BoolOp)
                and isinstance(node.op, ast.Or)
                and id(node) not in conditions
            ):
                findings.extend(self._or_default_findings(node, path))
        return sorted(findings, key=lambda finding: (finding.line, finding.field))

    def _or_default_findings(
        self, node: ast.BoolOp, path: str
    ) -> list[TruthinessFinding]:
        """``row.field or <default>`` for every non-zero default in the chain.

        A chain (``a or b or c``) is checked pairwise: each watched operand is
        corrupted by whatever follows it.
        """
        findings: list[TruthinessFinding] = []
        for index, operand in enumerate(node.values[:-1]):
            field = _attribute_field(operand, self._fields)
            if field is None:
                continue
            default = node.values[index + 1]
            if _is_safe_default(default):
                continue
            rendered = ast.unparse(default)
            findings.append(
                TruthinessFinding(
                    path,
                    operand.lineno,
                    field,
                    f"`.{field} or {rendered}` — a zero {field} is replaced by "
                    f"{rendered}; guard with `is not None` (or default to 0)",
                )
            )
        return findings

    def scan_tree(
        self, root: Path, directories: Sequence[str]
    ) -> list[TruthinessFinding]:
        """Scan the named subdirectories of ``root``.

        A directory that does not exist is skipped, so one declaration can
        serve deployables that do not all carry the same layers.
        """
        findings: list[TruthinessFinding] = []
        for directory in directories:
            target = root / directory
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
    def report(findings: Sequence[TruthinessFinding]) -> str:
        """A failure message that names every site and why it matters."""
        return (
            f"Found {len(findings)} zero-corrupting truthiness bug(s) on numeric "
            f"fields — 0 is data, not absence:\n"
            + "\n".join(str(finding) for finding in findings)
        )
