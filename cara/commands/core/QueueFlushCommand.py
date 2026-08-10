"""Empty every canonical queue plus the dead-letter queue — reset hygiene only.

A database reset leaves the broker full of envelopes that reference rows which
no longer exist. The worker then churns through orphans, and every job class
that has since been renamed dead-letters on deserialization. None of it is
recoverable and none of it is wanted, so it is simply dropped.

The queue inventory comes from the driver's own canonical set — the same SSOT
``require_canonical_queue`` validates dispatches against — so this can never
drift from what the application actually declares, and the connection comes
from ``open_topology_connection`` so vhost isolation always holds.

Refused in production: it discards in-flight work.
"""

from __future__ import annotations

from cara.commands import CommandBase
from cara.decorators import command
from cara.facades import Log
from cara.queues.Topology import DEAD_LETTER_QUEUE, close_quietly


@command(
    name="queue:flush",
    help="Purge ALL messages from every canonical queue (reset hygiene)",
    options={
        "--force": "Skip the confirmation prompt (required for non-interactive use)",
    },
)
class QueueFlushCommand(CommandBase):
    """Purge every canonical queue and the dead-letter queue."""

    def handle(self, force: bool = False) -> int:
        if self._is_production():
            self.line(
                "<error>queue:flush is refused in production — it drops "
                "in-flight work and is reset hygiene only.</error>"
            )
            return 1

        if not force:
            self.line(
                "<comment>This empties EVERY canonical queue (drops all pending "
                "+ in-flight jobs). Re-run with --force to proceed.</comment>"
            )
            return 0

        try:
            driver = self.application.make("queue").driver("amqp")
            targets = self.purge_targets(driver)
            connection, channel = driver.open_topology_connection()
        except Exception as exc:  # noqa: BLE001 — reported to the operator below
            Log.error(
                "queue:flush could not reach the broker: %s",
                exc,
                category="command.queue_flush",
                exc_info=True,
            )
            self.line(f"<error>AMQP connect failed: {exc}</error>")
            return 1

        try:
            purged = 0
            for name in sorted(targets):
                try:
                    channel.queue_purge(queue=name)
                    purged += 1
                except Exception as exc:  # noqa: BLE001 — a 404 is expected
                    # A missing queue closes the channel — reopen and skip.
                    Log.debug(
                        "queue:flush could not purge %s: %s",
                        name,
                        exc,
                        category="command.queue_flush",
                    )
                    channel = connection.channel()
            self.line(f"<info>Queues purged: {purged}/{len(targets)}.</info>")
            return 0
        finally:
            close_quietly(channel)
            close_quietly(connection)

    @staticmethod
    def purge_targets(driver) -> frozenset[str]:
        """Every canonical queue the driver knows, plus the dead-letter queue."""
        return frozenset(driver.canonical_queues) | {DEAD_LETTER_QUEUE}
