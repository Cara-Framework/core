"""Body redaction of settled ledger rows.

A row stays ``audit_retention_days`` so a late broker copy still dedupes
against it, but that only needs ``payload_sha256``. The signed body (the job's
arguments, which can carry personal data such as a sealed webhook body that
names a buyer) has no reader once the row is settled: execution compares the
digest, terminal hooks have run, and only a dead or expired row can be
replayed. So a settled body is replaced by a stub that keeps what ran, and a
dead letter keeps its body until a replay child carries its own copy.
"""

from __future__ import annotations

from typing import Any

import pendulum

#: The marker key of a redacted body; never a key of a real signed envelope.
REDACTED_MARKER = "redacted_at"


def _delivery_redact_settled_bodies(
    self,
    batch_size: int = 5000,
    *,
    max_batches: int = 200,
) -> int:
    """Redact the bodies of settled rows past ``body_retention_days``.

    Walks ``completed_at`` forward from a bounded look-back (one index range,
    each row read once per run), so the nightly cost is the window's rows, not
    the whole 90-day ledger. A row settled before the look-back keeps its body
    until the retention prune deletes it.
    """
    limit = self._bounded_int(
        batch_size,
        minimum=1,
        maximum=10000,
        field="delivery_redact_batch",
    )
    batches = self._bounded_int(
        max_batches,
        minimum=1,
        maximum=10000,
        field="delivery_redact_max_batches",
    )
    now = pendulum.now("UTC")
    cutoff = now.subtract(days=self.body_retention_days)
    reached: Any = cutoff.subtract(days=self.BODY_REDACTION_LOOKBACK_DAYS)
    total = 0
    database = self._db()
    for _batch in range(batches):
        # Its own transaction: a bare select is not promised to commit a write.
        with database.transaction():
            row = database.select_one(
                "WITH candidate AS ("
                f"SELECT candidate.job_id, candidate.completed_at FROM {self.table} "
                "AS candidate WHERE candidate.status = ANY(%s) "
                "AND candidate.completed_at >= %s AND candidate.completed_at < %s "
                # Hooks done: the terminal-hook runner reads the body until then,
                # and a quarantined row may be retried.
                "AND candidate.post_hooks_completed_at IS NOT NULL "
                f"AND candidate.signed_envelope -> '{REDACTED_MARKER}' IS NULL "
                # A dead or expired row is the replay source until a child exists.
                "AND (candidate.status = ANY(%s) OR EXISTS (SELECT 1 FROM "
                f"{self.table} AS replay WHERE replay.replay_of = candidate.job_id)) "
                "ORDER BY candidate.completed_at LIMIT %s FOR UPDATE SKIP LOCKED), "
                f"redacted AS (UPDATE {self.table} AS delivery SET signed_envelope = "
                "jsonb_build_object("
                f"'{REDACTED_MARKER}', %s::text, "
                "'payload', jsonb_build_object("
                "'job_id', delivery.signed_envelope -> 'payload' -> 'job_id', "
                "'job', jsonb_build_object("
                "'module', delivery.signed_envelope -> 'payload' -> 'job' -> 'module', "
                "'class', delivery.signed_envelope -> 'payload' -> 'job' -> 'class'))) "
                # ``updated_at`` stays: redaction is not a state change.
                "FROM candidate WHERE delivery.job_id = candidate.job_id "
                "RETURNING candidate.completed_at) "
                "SELECT COUNT(*) AS redacted, MAX(completed_at) AS reached FROM redacted",
                [
                    list(self.TERMINAL_STATUSES),
                    reached,
                    cutoff,
                    list(self.HOOKLESS_TERMINAL_STATUSES),
                    limit,
                    now.to_iso8601_string(),
                ],
            )
        redacted = int(self._row_value(row, "redacted") or 0)
        total += redacted
        if redacted < limit:
            return total
        reached = self._row_value(row, "reached") or reached
    return total
