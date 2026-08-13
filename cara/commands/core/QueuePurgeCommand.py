"""Delete failed tracker jobs and terminal delivery-ledger rows.

Ledger rows go first, on purpose. A tracker ``job`` row still referenced by
``queue_job_delivery.db_job_id`` cannot be deleted (``ON DELETE RESTRICT``), so
freeing its ledger row lets the SAME run reclaim the tracker row — which is why
the tracker candidate list is re-queried after the ledger delete rather than
reused.

``--broker`` additionally empties the live dead-letter queue over AMQP. That
purge has no per-queue or per-age granularity (a broker queue purge empties the
whole queue), so it is reported and confirmed separately from the DB-side,
cutoff-scoped counts.
"""

from __future__ import annotations

import pendulum

from cara.commands.CommandBase import CommandBase
from cara.decorators import command
from cara.facades import Log
from cara.queues import DEAD_LETTER_QUEUE, QueueOperationsStore, close_quietly


@command(
    name="queue:purge",
    help="Purge failed tracker jobs and terminal delivery-ledger rows",
    options=[
        {
            "name": "--queue",
            "help": "Scope the purge to one queue",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--older-than",
            "help": "Purge rows older than N hours",
            "type": int,
            "default": 24,
            "is_flag": False,
        },
        {
            "name": "--broker",
            "help": "Also purge the live dead-letter queue over AMQP (queue-wide, ignores --older-than)",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--force",
            "help": "Skip the confirmation prompt",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
    ],
)
class QueuePurgeCommand(CommandBase):
    """Reclaim terminal queue state from the tracker and the ledger."""

    def handle(
        self,
        queue: str | None = None,
        older_than: int = 24,
        broker: bool = False,
        force: bool = False,
        store: QueueOperationsStore | None = None,
    ) -> int:
        store = store or QueueOperationsStore()
        cutoff = pendulum.now("UTC").subtract(hours=int(older_than))

        tracker_jobs = store.list_failed_jobs_older_than(cutoff=cutoff, queue=queue)
        ledger_count = store.count_dead_lettered_older_than(cutoff=cutoff, queue=queue)

        if not tracker_jobs and not ledger_count and not broker:
            self.line(f"<comment>Nothing older than {older_than}h to purge.</comment>")
            return 0

        self.line(
            f"\n<fg=yellow>  WARNING: about to permanently delete "
            f"{len(tracker_jobs)} tracker job(s) and {ledger_count} ledger "
            "delivery(ies).</fg=yellow>\n"
        )
        by_queue: dict[str, int] = {}
        for job in tracker_jobs:
            queue_name = str(job.get("queue") or "default")
            by_queue[queue_name] = by_queue.get(queue_name, 0) + 1
        for queue_name, count in sorted(by_queue.items()):
            self.line(f"  • Queue <info>{queue_name}</info>: {count} tracker job(s)")
        self.line(
            f"\n<comment>Cutoff: {cutoff.format('YYYY-MM-DD HH:mm:ss')} UTC "
            f"(older than {older_than}h)</comment>"
        )
        if broker:
            self.line(
                f"  <fg=yellow>--broker also empties {DEAD_LETTER_QUEUE} entirely "
                "(every message, any age)</fg=yellow>"
            )

        if not force and not self.confirm("Continue with purge?", default=False):
            self.line("<comment>Purge cancelled</comment>")
            return 0

        deleted_ledger = store.delete_terminal_deliveries_older_than(
            cutoff=cutoff, queue=queue
        )

        # Re-query: the ledger delete above may have freed tracker jobs that
        # were previously blocked by the db_job_id foreign key.
        deleted_tracker = 0
        for job in store.list_failed_jobs_older_than(cutoff=cutoff, queue=queue):
            identifier = job.get("public_id") or job.get("id")
            try:
                deleted_tracker += store.delete_job(job["id"])
            except Exception as exc:  # noqa: BLE001 — reported below, sweep continues
                Log.warning(
                    "queue:purge failed to delete tracker job %s: %s",
                    identifier,
                    exc,
                    category="command.queue_purge",
                )
                self.line(f"  <error>Failed to delete job {identifier}: {exc}</error>")

        self.line(
            f"\n<info>Purged {deleted_tracker} tracker job(s), "
            f"{deleted_ledger} ledger delivery(ies).</info>"
        )
        Log.info(
            "queue:purge deleted %s tracker job(s), %s ledger row(s) "
            "(older than %sh, queue=%s)",
            deleted_tracker,
            deleted_ledger,
            older_than,
            queue or "all",
            category="command",
        )

        if broker:
            broker_result = self._purge_broker_dead_letter_queue()
            if broker_result is None:
                self.line(f"<error>Broker purge of {DEAD_LETTER_QUEUE} failed.</error>")
                return 1
            self.line(
                f"<info>Broker purge: {broker_result} message(s) removed from "
                f"{DEAD_LETTER_QUEUE}.</info>"
            )

        self.line("")
        return 0

    def _purge_broker_dead_letter_queue(self) -> int | None:
        connection = None
        channel = None
        try:
            driver = self.application.make("queue").driver("amqp")
            connection, channel = driver.open_topology_connection()
            result = channel.queue_purge(queue=DEAD_LETTER_QUEUE)
            return int(result.method.message_count)
        except Exception as exc:  # noqa: BLE001 — reported to the operator below
            Log.error(
                "queue:purge broker purge of %s failed: %s",
                DEAD_LETTER_QUEUE,
                exc,
                category="command.queue_purge",
                exc_info=True,
            )
            return None
        finally:
            close_quietly(channel)
            close_quietly(connection)
