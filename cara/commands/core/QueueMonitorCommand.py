"""Queue health metrics from the tracker, dead-letter state from the ledger.

The tracker's ``failed`` rows are NOT a dead-letter queue. A job can fail,
retry and succeed without ever being dead-lettered, and a delivery can be
dead-lettered without the tracker recording a failure at all. Reporting failed
tracker rows under a "dead letter" heading — which is the shape this display
originally had — tells an operator to go looking for messages that were never
there. The dead-letter figure here is the audited ledger count, plus a
best-effort live depth of the broker's dead-letter queue.

The broker probe is best-effort by design: a passive declare over a dedicated
topology connection. Any failure degrades to a "broker unreachable" line and
the command still exits 0 on DB-only data — monitoring must not fail closed.
"""

from __future__ import annotations

import pendulum

from cara.commands import CommandBase
from cara.decorators import command
from cara.facades import Log
from cara.queues.delivery.QueueOperationsStore import QueueOperationsStore
from cara.queues.Topology import DEAD_LETTER_QUEUE, close_quietly


@command(
    name="queue:monitor",
    help="Display queue health metrics and dead-letter ledger stats",
    options={
        "--queue=?": "Monitor one queue only",
        "--limit=20": "Jobs to sample per category",
    },
)
class QueueMonitorCommand(CommandBase):
    """Display per-queue tracker health and audited dead-letter ledger stats."""

    def handle(
        self,
        queue: str | None = None,
        limit: int = 20,
        store: QueueOperationsStore | None = None,
    ) -> int:
        store = store or QueueOperationsStore()
        try:
            cutoff_24h = pendulum.now("UTC").subtract(hours=24)
            cutoff_1h = pendulum.now("UTC").subtract(hours=1)

            queues = (
                [queue] if queue else store.list_distinct_queues_since(cutoff=cutoff_24h)
            )

            self.line(
                "\n<info>Queue Health Monitor</info> - "
                f"{pendulum.now('UTC').format('YYYY-MM-DD HH:mm:ss')} UTC\n"
            )

            for queue_name in sorted(queues):
                self._display_queue_stats(store, queue_name, int(limit), cutoff_24h)

            self._display_overall_health(store, cutoff_24h, cutoff_1h)
            self._display_dead_letter_ledger(store, queue)

            self.line("")
            return 0
        except Exception as exc:  # noqa: BLE001 — reported below, never a raw traceback
            Log.error(
                "queue:monitor failed: %s",
                exc,
                category="command.queue_monitor",
                exc_info=True,
            )
            self.line(f"<error>Error: {exc}</error>")
            return 1

    def _display_queue_stats(
        self,
        store: QueueOperationsStore,
        queue_name: str,
        limit: int,
        cutoff_24h,
    ) -> None:
        pending = store.count_jobs(queue=queue_name, status=store.TRACKER_STATUS_PENDING)
        processing = store.count_jobs(
            queue=queue_name, status=store.TRACKER_STATUS_PROCESSING
        )
        completed = store.count_jobs(
            queue=queue_name,
            status=store.TRACKER_STATUS_SUCCESS,
            created_at_gte=cutoff_24h,
        )
        failed = store.count_jobs(
            queue=queue_name,
            status=store.TRACKER_STATUS_FAILED,
            created_at_gte=cutoff_24h,
        )
        retrying = store.count_jobs(
            queue=queue_name, status=store.TRACKER_STATUS_RETRYING
        )

        total = completed + failed
        failure_rate = (failed / max(total, 1)) * 100

        self.line(f"<fg=white;bg=gray>Queue: {queue_name}</fg=white;bg=gray>")
        self.line(
            f"  Pending: {pending:>4} | Processing: {processing:>3} | "
            f"Completed: {completed:>5} | Failed: {failed:>4} | Retrying: {retrying:>3}"
        )
        self.line(
            f"  <comment>Failure Rate:</comment> {failure_rate:.1f}% ({failed}/{total})"
        )

        failed_jobs = store.list_recent_failed_jobs(
            queue=queue_name, cutoff=cutoff_24h, limit=limit
        )
        if failed_jobs:
            self.line("  <comment>Recent Failures:</comment>")
            error_types: dict[str, int] = {}
            for job in failed_jobs[: min(5, len(failed_jobs))]:
                error_type = str(job.get("error") or "unknown").split(":")[0][:40]
                error_types[error_type] = error_types.get(error_type, 0) + 1
            for error_type, count in sorted(
                error_types.items(), key=lambda item: item[1], reverse=True
            )[:5]:
                self.line(f"    • {error_type}: {count} occurrence(s)")

        dead_lettered = store.count_dead_lettered(queue=queue_name)
        if dead_lettered:
            self.line(
                f"  <error>Dead-lettered (ledger):</error> {dead_lettered} delivery(ies)"
            )

        self.line("")

    def _display_overall_health(
        self,
        store: QueueOperationsStore,
        cutoff_24h,
        cutoff_1h,
    ) -> None:
        total_24h = store.count_jobs(created_at_gte=cutoff_24h)
        completed_24h = store.count_jobs(
            status=store.TRACKER_STATUS_SUCCESS, created_at_gte=cutoff_24h
        )
        failed_24h = store.count_jobs(
            status=store.TRACKER_STATUS_FAILED, created_at_gte=cutoff_24h
        )
        completed_1h = store.count_jobs(
            status=store.TRACKER_STATUS_SUCCESS, created_at_gte=cutoff_1h
        )
        throughput = round(completed_1h / 60, 2) if completed_1h > 0 else 0

        self.line("<info>Overall System Health (Last 24 Hours)</info>")
        self.line(f"  Total Jobs: {total_24h}")
        self.line(f"  Completed: {completed_24h} | Failed: {failed_24h}")
        self.line(
            f"  Success Rate: {round((completed_24h / max(total_24h, 1)) * 100, 1)}%"
        )
        self.line(f"  Throughput: {throughput} jobs/min (last 1 hour)")

    def _display_dead_letter_ledger(
        self, store: QueueOperationsStore, queue: str | None
    ) -> None:
        ledger_total = store.count_dead_lettered(queue=queue)
        self.line("\n<info>Dead-Letter Ledger</info> (queue_job_delivery, audited)")
        self.line(f"  Terminal deliveries: {ledger_total}")

        depth = self._broker_dead_letter_depth()
        if depth is None:
            self.line(
                "  <comment>Broker depth: unreachable "
                "(DB-only data shown above)</comment>"
            )
        else:
            self.line(f"  Broker depth ({DEAD_LETTER_QUEUE}): {depth}")

    def _broker_dead_letter_depth(self) -> int | None:
        """Best-effort passive depth probe; never fails the command."""
        connection = None
        channel = None
        try:
            driver = self.application.make("queue").driver("amqp")
            connection, channel = driver.open_topology_connection()
            result = channel.queue_declare(queue=DEAD_LETTER_QUEUE, passive=True)
            return int(result.method.message_count)
        except Exception as exc:  # noqa: BLE001 — best-effort broker probe
            Log.warning(
                "queue:monitor could not reach the broker for dead-letter depth: %s",
                exc,
                category="command.queue_monitor",
            )
            return None
        finally:
            close_quietly(channel)
            close_quietly(connection)
