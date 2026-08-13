"""Replay dead-lettered/expired deliveries through the audited ledger rail.

Publication happens exclusively from the ``queue_job_delivery`` outbox, driven
by ``queue:relay``. Rewriting a tracker ``job`` row — flipping it to
``retrying`` and moving ``available_at`` — therefore republishes nothing at
all: the relay never reads that table, and the crash-recovery sweep only
requeues ledger rows holding a stale lease. A retry that edits the tracker is a
silent no-op that reports success.

The real rail is ``AMQPDriver.replay_delivery`` ->
``QueueJobDeliveryStore.replay_from_ledger``: audited (operator and reason are
recorded on the new ledger row), idempotent (a repeat for the same source
returns the existing accepted replay child rather than erroring),
signature-verified, and restricted to ``dead_lettered``/``expired`` sources.
This command never touches the tracker row and never rewrites the payload —
the signed envelope is republished verbatim, so anything the envelope carries
(execution context included) re-enters by construction.
"""

from __future__ import annotations

import getpass

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues import QueueOperationsStore


@command(
    name="queue:retry",
    help="Replay dead-lettered/expired deliveries via the audited ledger rail",
    options=[
        {
            "name": "--job-id",
            "help": "Ledger job_id (UUID) of one delivery to replay",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--queue",
            "help": "Replay dead-lettered deliveries for one queue",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--all",
            "help": "Replay dead-lettered deliveries across every queue",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--limit",
            "help": "Max deliveries to replay in one run",
            "type": int,
            "default": 50,
            "is_flag": False,
        },
        {
            "name": "--reason",
            "help": "Audit reason recorded on each replay",
            "type": str,
            "default": "manual queue:retry",
            "is_flag": False,
        },
    ],
)
class QueueRetryCommand(CommandBase):
    """Replay terminal ledger deliveries back onto their canonical queues."""

    def handle(
        self,
        job_id: str | None = None,
        queue: str | None = None,
        all: bool = False,
        limit: int = 50,
        reason: str = "manual queue:retry",
        store: QueueOperationsStore | None = None,
    ) -> int:
        if not (job_id or queue or all):
            self.line(
                "<error>Specify --job-id, --queue, or --all to retry "
                "dead-lettered deliveries</error>"
            )
            return 1

        store = store or QueueOperationsStore()

        try:
            driver = self.application.make("queue").driver("amqp")
        except Exception as exc:  # noqa: BLE001 — reported to the operator below
            Log.error(
                "queue:retry could not obtain the AMQP driver: %s",
                exc,
                category="command.queue_retry",
                exc_info=True,
            )
            self.line(f"<error>Could not reach the queue driver: {exc}</error>")
            return 1

        if job_id:
            row = store.find_dead_lettered_delivery(job_id)
            candidates = [row] if row else []
        else:
            candidates = store.list_dead_lettered_deliveries(
                queue=queue, limit=int(limit)
            )

        if not candidates:
            self.line(
                "<comment>No dead-lettered/expired ledger deliveries matched</comment>"
            )
            return 0

        operator = self._operator()
        succeeded = 0
        for row in candidates:
            source_job_id = str(row["job_id"])
            try:
                replay_job_id = driver.replay_delivery(
                    source_job_id, operator=operator, reason=reason
                )
            except QueueException as exc:
                self.line(f"  <comment>⚠ {source_job_id}: {exc}</comment>")
                continue
            self.line(f"  <info>✓</info> {source_job_id} -> {replay_job_id}")
            succeeded += 1

        self.line(
            f"\n<info>Replayed {succeeded}/{len(candidates)} "
            "dead-lettered delivery(ies)</info>\n"
        )
        Log.info(
            "queue:retry replayed %s/%s dead-lettered deliveries (queue=%s, operator=%s)",
            succeeded,
            len(candidates),
            queue or "all",
            operator,
            category="command",
        )
        return 0 if succeeded >= 1 else 1

    @staticmethod
    def _operator() -> str:
        """The current CLI operator identity, for the ledger audit trail."""
        try:
            return f"cli:{getpass.getuser()}"
        except OSError:
            return "cli:unknown"
