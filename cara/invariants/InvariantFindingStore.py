"""Persistence of an invariant auditor: findings, runs, gauge reads, prune.

Product-blind SQL over two product-owned tables (their schema lives in the
product's models, generated like every other table):

* the finding table — one row per broken subject; ONE open row per
  (invariant, scope, subject) is the database's guarantee through the
  partial unique index :meth:`InvariantFindingStore.one_open_index` spells,
  and the upsert names that index as its conflict target, so two passes
  racing on one break end in one row, never a 23505;
* the run table — one row per check per pass: freshness and the
  incremental cursor live there, and a ``failed`` run closes nothing.

Columns the store reads and writes — finding: ``invariant, severity, owner,
[<scope_column>], subject_key, status, violation_count, evidence (jsonb
array), opened_at, last_seen_at, closed_at, acknowledged_at,
acknowledged_by_user_id, note, created_at, updated_at`` plus ``public_id``
when a ``public_id_factory`` is given; run: ``invariant, cadence,
started_at, finished_at, outcome, violations, cursor (jsonb), error,
created_at, updated_at``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

from cara.facades import DB

from .Violation import EVIDENCE_LIMIT

STATUS_OPEN = "open"
STATUS_CLOSED = "closed"

OUTCOME_HELD = "held"
OUTCOME_VIOLATED = "violated"
OUTCOME_FAILED = "failed"
OUTCOMES = (OUTCOME_HELD, OUTCOME_VIOLATED, OUTCOME_FAILED)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.match(value or ""):
        raise ValueError(f"not a plain SQL identifier: {value!r}")
    return value


class InvariantFindingStore:
    """Findings and runs of one product's ``InvariantAuditor``."""

    def __init__(
        self,
        *,
        finding_table: str = "invariant_finding",
        run_table: str = "invariant_audit_run",
        scope_column: str | None = None,
        public_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.finding_table = _identifier(finding_table)
        self.run_table = _identifier(run_table)
        self.scope_column = _identifier(scope_column) if scope_column else None
        self._public_id = public_id_factory

    # ─── Schema the product's model declares ──────────────────────────────

    @staticmethod
    def one_open_index(
        finding_table: str = "invariant_finding", scope_column: str | None = None
    ) -> dict[str, str]:
        """The ``__indexes__`` entry of the finding model: ONE open finding
        per broken subject. The upsert's conflict target is this index."""
        table = _identifier(finding_table)
        name = f"{table}_one_open_idx"
        key = (
            f"invariant, COALESCE({_identifier(scope_column)}, 0), subject_key"
            if scope_column
            else "invariant, subject_key"
        )
        return {
            "name": name,
            "up": (
                f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table} ({key}) "
                f"WHERE status = '{STATUS_OPEN}'"
            ),
            "down": f"DROP INDEX IF EXISTS {name}",
        }

    # ─── The pass ─────────────────────────────────────────────────────────

    @staticmethod
    def transaction():
        return DB.transaction()

    @staticmethod
    def bound_check_time(milliseconds: int) -> None:
        """Bound every statement of the current transaction (one check)."""
        DB.statement(f"SET LOCAL statement_timeout = {int(milliseconds)}")

    def open_findings(self, invariant_id: str) -> list[dict[str, Any]]:
        """Every open finding of one invariant (``scope_id`` = its scope)."""
        scope = self.scope_column or "NULL::bigint"
        return [
            dict(row)
            for row in DB.select(
                f"SELECT id, {scope} AS scope_id, subject_key, evidence, "
                f"violation_count FROM {self.finding_table} "
                "WHERE invariant = %s AND status = %s",
                [invariant_id, STATUS_OPEN],
            )
            or []
        ]

    def upsert_open(
        self,
        *,
        invariant_id: str,
        severity: str,
        owner: str,
        scope_id: int | None,
        subject_key: str,
        violation_count: int,
        evidence: list[dict[str, Any]],
        now: Any,
    ) -> dict[str, Any]:
        """Open the finding, or refresh the open one; ``{id, opened}``."""
        columns = ["invariant", "severity", "owner"]
        values: list[Any] = [invariant_id, severity, owner]
        if self._public_id is not None:
            columns.insert(0, "public_id")
            values.insert(0, self._public_id())
        if self.scope_column:
            columns.append(self.scope_column)
            values.append(scope_id)
            target = f"invariant, (COALESCE({self.scope_column}, 0)), subject_key"
        else:
            target = "invariant, subject_key"
        columns += [
            "subject_key",
            "status",
            "violation_count",
            "evidence",
            "opened_at",
            "last_seen_at",
            "created_at",
            "updated_at",
        ]
        values += [
            str(subject_key)[:160],
            STATUS_OPEN,
            int(violation_count),
            json.dumps(list(evidence)[:EVIDENCE_LIMIT], default=str),
            now,
            now,
            now,
            now,
        ]
        placeholders = ", ".join(
            "%s::jsonb" if column == "evidence" else "%s" for column in columns
        )
        row = DB.select_one(
            f"INSERT INTO {self.finding_table} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({target}) WHERE status = '{STATUS_OPEN}' "
            "DO UPDATE SET severity = EXCLUDED.severity, owner = EXCLUDED.owner, "
            "violation_count = EXCLUDED.violation_count, "
            "evidence = EXCLUDED.evidence, last_seen_at = EXCLUDED.last_seen_at, "
            "updated_at = EXCLUDED.updated_at "
            "RETURNING id, (xmax = 0) AS opened",
            values,
        )
        return {"id": int(row["id"]), "opened": bool(row["opened"])}

    def close(self, finding_ids: list[int], now: Any) -> int:
        """Close the given open findings; how many closed."""
        if not finding_ids:
            return 0
        return int(
            DB.statement(
                f"UPDATE {self.finding_table} SET status = %s, closed_at = %s, "
                "updated_at = %s WHERE id = ANY(%s) AND status = %s",
                [
                    STATUS_CLOSED,
                    now,
                    now,
                    [int(value) for value in finding_ids],
                    STATUS_OPEN,
                ],
            )
            or 0
        )

    def record_run(
        self,
        *,
        invariant_id: str,
        cadence: str,
        started_at: Any,
        finished_at: Any,
        outcome: str,
        violations: int,
        cursor: dict[str, Any] | None,
        error: str | None,
    ) -> None:
        """Append one run row."""
        DB.statement(
            f"INSERT INTO {self.run_table} (invariant, cadence, started_at, "
            "finished_at, outcome, violations, cursor, error, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)",
            [
                invariant_id,
                cadence,
                started_at,
                finished_at,
                outcome,
                int(violations),
                json.dumps(cursor, default=str) if cursor is not None else None,
                str(error)[:300] if error else None,
                finished_at,
                finished_at,
            ],
        )

    def last_cursor(self, invariant_id: str) -> dict[str, Any] | None:
        """The newest cursor a run of ``invariant_id`` left, or None."""
        row = DB.select_one(
            f"SELECT cursor FROM {self.run_table} WHERE invariant = %s "
            "AND cursor IS NOT NULL ORDER BY started_at DESC, id DESC LIMIT 1",
            [invariant_id],
        )
        if not row:
            return None
        value = row.get("cursor")
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, dict) else None

    # ─── Gauge reads ──────────────────────────────────────────────────────

    def last_good_runs(self) -> dict[str, Any]:
        """Per invariant, when its newest run that did not fail started."""
        return {
            row["invariant"]: row["at"]
            for row in DB.select(
                f"SELECT invariant, MAX(started_at) AS at FROM {self.run_table} "
                "WHERE outcome <> %s GROUP BY invariant",
                [OUTCOME_FAILED],
            )
            or []
        }

    def open_counts(self) -> list[dict[str, Any]]:
        """Open findings per (invariant, severity, acknowledged)."""
        return list(
            DB.select(
                "SELECT invariant, severity, (acknowledged_at IS NOT NULL) AS "
                f"acknowledged, COUNT(*) AS findings FROM {self.finding_table} "
                "WHERE status = %s GROUP BY invariant, severity, "
                "(acknowledged_at IS NOT NULL)",
                [STATUS_OPEN],
            )
            or []
        )

    def open_violation_rows(self) -> list[dict[str, Any]]:
        """Proving rows behind the open findings, per invariant."""
        return list(
            DB.select(
                "SELECT invariant, COALESCE(SUM(violation_count), 0) AS rows "
                f"FROM {self.finding_table} WHERE status = %s GROUP BY invariant",
                [STATUS_OPEN],
            )
            or []
        )

    # ─── The operator ─────────────────────────────────────────────────────

    def acknowledge(
        self, finding_id: int, actor_user_id: int, note: str, now: Any
    ) -> dict[str, Any] | None:
        """Acknowledge an OPEN finding; the row before, or None when it is
        closed or gone. Acknowledging never closes it: it closes when its
        rule holds again."""
        row = DB.select_one(
            f"SELECT id, invariant, acknowledged_at FROM {self.finding_table} "
            "WHERE id = %s AND status = %s FOR UPDATE",
            [int(finding_id), STATUS_OPEN],
        )
        if row is None:
            return None
        DB.statement(
            f"UPDATE {self.finding_table} SET acknowledged_at = %s, "
            "acknowledged_by_user_id = %s, note = %s, updated_at = %s WHERE id = %s",
            [now, int(actor_user_id), str(note)[:500], now, int(row["id"])],
        )
        return dict(row)

    def prune(self, runs_before: Any, closed_before: Any) -> dict[str, int]:
        """Delete runs older than ``runs_before`` and findings closed before
        ``closed_before``. Open findings are never deleted."""
        runs = DB.statement(
            f"DELETE FROM {self.run_table} WHERE started_at < %s", [runs_before]
        )
        findings = DB.statement(
            f"DELETE FROM {self.finding_table} WHERE status = %s AND closed_at < %s",
            [STATUS_CLOSED, closed_before],
        )
        return {"runs": int(runs or 0), "findings": int(findings or 0)}


__all__ = [
    "OUTCOMES",
    "OUTCOME_FAILED",
    "OUTCOME_HELD",
    "OUTCOME_VIOLATED",
    "STATUS_CLOSED",
    "STATUS_OPEN",
    "InvariantFindingStore",
]
