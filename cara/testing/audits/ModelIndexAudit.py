"""ModelIndexAudit: index invariants read from the MODELS, never migration text.

Two rules, both true of Postgres rather than of a product:

* **Every foreign key has a parent-lookup index.** Postgres does not index a
  referencing column; without one, every parent UPDATE/DELETE walks the child
  table to check the reference, and every "children of X" read seq-scans. The
  index must LEAD with the key, and a partial index counts only when its
  predicate is ``<key> IS NOT NULL`` — the FK check binds only the key, so a
  planner can prove nothing else (an index ``WHERE settled_at IS NULL`` leaves
  the rows it excludes unreachable). A composite key is covered by an index
  whose leading columns are exactly the key's, or that leads with one of its
  ``id``-referencing columns.
* **No index is a redundant prefix of another.** Two indexes on one table with
  the same predicate, one column list a strict prefix of the other: Postgres
  serves the leading-column lookup from the wider one, so the narrow twin only
  costs writes and bloat. A UNIQUE narrow index is never redundant (it
  constrains writes), nor is one whose columns are an expression.

Models come from ``ModelDiscoverer`` (through :class:`cara.testing.ModelSchema`)
unless a test hands its own declarations in.
"""

from __future__ import annotations

import re

from ..ModelSchema import ModelSchema
from .ModelIndexFinding import ModelIndexFinding

_RAW_INDEX = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+CONCURRENTLY)?"
    r"(?:\s+IF\s+NOT\s+EXISTS)?\s+\"?\w+\"?\s+ON\s+\"?\w+\"?"
    r"\s*\((?P<columns>[^()]*)\)(?P<tail>.*)",
    re.IGNORECASE,
)


class ModelIndexAudit:
    """Run the two index rules over model declarations."""

    def __init__(self, models: list[dict] | None = None) -> None:
        self._models = models

    def models(self) -> list[dict]:
        return ModelSchema.discovered_models() if self._models is None else self._models

    # ── foreign keys ─────────────────────────────────────────────────────
    def missing_foreign_key_indexes(self) -> list[ModelIndexFinding]:
        findings: list[ModelIndexFinding] = []
        for model in self.models():
            table = model.get("table") or ""
            indexes = _index_shapes(model)
            for name, definition in (model.get("fields") or {}).items():
                if not definition.get("foreign_key"):
                    continue
                if not any(
                    columns and columns[0] == name and _predicate_binds(predicate, [name])
                    for columns, predicate in indexes
                ):
                    findings.append(
                        ModelIndexFinding(table, name, "foreign key has no lookup index")
                    )
            for foreign in model.get("composite_foreign_keys") or []:
                columns = tuple(foreign.get("columns") or [])
                references = tuple(foreign.get("references") or [])
                identity = [
                    column
                    for column, reference in zip(columns, references, strict=True)
                    if reference == "id"
                ]
                if not any(
                    _covers_composite(index_columns, predicate, columns, identity)
                    for index_columns, predicate in indexes
                ):
                    findings.append(
                        ModelIndexFinding(
                            table,
                            str(foreign.get("name") or columns),
                            "composite foreign key has no lookup index",
                        )
                    )
        return sorted(findings, key=str)

    # ── redundant prefixes ───────────────────────────────────────────────
    def redundant_prefix_indexes(self) -> list[ModelIndexFinding]:
        declared = ModelSchema.declared_indexes(self.models())
        entries = [
            (name, info["table"], info["columns"], info["predicate"], info["unique"])
            for name, info in declared.items()
            if info["columns"]
        ]
        findings: list[ModelIndexFinding] = []
        for name, table, columns, predicate, unique in entries:
            if unique or any("(" in column for column in columns):
                continue
            for other, other_table, other_columns, other_predicate, _ in entries:
                if (
                    other != name
                    and other_table == table
                    and other_predicate == predicate
                    and len(columns) < len(other_columns)
                    and tuple(other_columns[: len(columns)]) == tuple(columns)
                ):
                    findings.append(
                        ModelIndexFinding(
                            table, name, f"is a redundant prefix of {other}"
                        )
                    )
                    break
        return sorted(findings, key=str)

    @staticmethod
    def report(findings: list[ModelIndexFinding]) -> str:
        return "Index invariants broken (read from the models):\n" + "\n".join(
            str(finding) for finding in findings
        )


def _index_shapes(model: dict) -> list[tuple[tuple[str, ...], str]]:
    shapes: list[tuple[tuple[str, ...], str]] = []
    for declaration in (model.get("composite_indexes") or []) + (
        model.get("composite_uniques") or []
    ):
        shapes.append((tuple(declaration.get("columns") or []), ""))
    for name, definition in (model.get("fields") or {}).items():
        params = definition.get("params") or {}
        if (
            params.get("index")
            or params.get("unique")
            or definition.get("type") in {"increments", "big_increments"}
        ):
            shapes.append(((name,), ""))
    for declaration in model.get("indexes") or []:
        sql = " ".join((declaration.get("up") or "").split())
        match = _RAW_INDEX.search(sql)
        if match is not None:
            columns = tuple(
                part.strip().strip('"') for part in match.group("columns").split(",")
            )
            shapes.append((columns, match.group("tail")))
    return shapes


def _covers_composite(
    index_columns: tuple[str, ...],
    predicate: str,
    foreign_columns: tuple[str, ...],
    identity_columns: list[str],
) -> bool:
    width = len(foreign_columns)
    exact = len(index_columns) >= width and set(index_columns[:width]) == set(
        foreign_columns
    )
    selective = bool(index_columns and index_columns[0] in identity_columns)
    return (exact or selective) and _predicate_binds(predicate, identity_columns)


def _predicate_binds(predicate: str, key_columns: list[str]) -> bool:
    """A partial index serves an FK check only when its WHERE is ``key IS NOT NULL``."""
    normalized = predicate.upper().replace('"', "")
    if " WHERE " not in f" {normalized}":
        return True
    condition = f" {normalized}".split(" WHERE ", 1)[1].strip()
    wanted = {column.upper() for column in key_columns}
    terms = re.split(r"\s+AND\s+", condition)
    matches = [re.fullmatch(r"(\w+)\s+IS\s+NOT\s+NULL", term.strip()) for term in terms]
    return all(match and match.group(1) in wanted for match in matches)
