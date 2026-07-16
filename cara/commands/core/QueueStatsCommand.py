"""Display bounded canonical delivery-ledger queue statistics."""

from __future__ import annotations

import sys
import time

import pendulum

from cara.commands import CommandBase
from cara.decorators import command
from cara.exceptions import InvalidArgumentException
from cara.facades import Queue


@command(
    name="queue:stats",
    help="Display canonical delivery-ledger queue statistics",
    options={
        "--queue=?": "Canonical queue name to show stats for",
        "--recent=24": "Show jobs from last N hours (default: 24)",
        "--watch": "Watch mode - refresh every 5 seconds",
    },
)
class QueueStatsCommand(CommandBase):
    """Display due, stale and terminal-hook state for one canonical queue."""

    def handle(self):
        """Show queue statistics with optional watch mode."""
        queue_name = self.option("queue")
        if not queue_name:
            raise ValueError("--queue is required.")
        watch_mode = self.option("watch", False)
        try:
            recent_hours = int(self.option("recent", 24))
        except (TypeError, ValueError) as exc:
            raise InvalidArgumentException(
                "--recent must be an integer number of hours."
            ) from exc
        if not 1 <= recent_hours <= 8760:
            raise InvalidArgumentException(
                "--recent must be between 1 and 8760 hours."
            )

        if watch_mode:
            self._watch_mode(queue_name, recent_hours)
        else:
            self._show_stats_once(queue_name, recent_hours)
        return 0

    def _watch_mode(self, queue_name: str, recent_hours: int):
        """Watch mode - refresh stats every 5 seconds."""
        self.info("📊 Queue Stats Watch Mode (Ctrl+C to exit)")
        self.info("Refreshing every 5 seconds...\n")

        try:
            while True:
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.flush()

                self.info(
                    f"📊 Queue Stats Watch Mode - {pendulum.now('UTC').format('HH:mm:ss')}"
                )
                self.info("=" * 60)

                self._show_stats_once(queue_name, recent_hours)

                self.info("\n🔄 Refreshing in 5 seconds... (Ctrl+C to exit)")
                time.sleep(5)

        except KeyboardInterrupt:
            self.info("\n👋 Watch mode stopped.")

    def _show_stats_once(self, queue_name: str, recent_hours: int):
        """Show queue statistics once."""
        driver = Queue.driver("amqp")
        canonical = driver.require_canonical_queue(queue_name)
        stats = driver.delivery_store.delivery_stats(
            canonical,
            recent_hours=recent_hours,
        )
        self.info(
            f"Durable queue ledger (Queue: {canonical}, "
            f"terminal history: last {recent_hours}h)"
        )
        self.info("-" * 60)
        self.info(f"   Active rows (all ages): {stats['active_total']}")
        self.info(
            "   Terminal rows in window: "
            f"{stats['terminal_recent_total']}"
        )
        for status, count in stats["statuses"].items():
            self.info(f"   {status}: {count}")
        self.info(f"   Due unpublished: {stats['due_unpublished']}")
        self.info(
            f"   Oldest due unpublished: {stats['oldest_due_age']:.1f}s"
        )
        self.info(f"   Publish processing: {stats['publish_processing']}")
        self.info(
            "   Stale leases: "
            f"publish={stats['stale_leases']['publish']} "
            f"execution={stats['stale_leases']['execution']}"
        )
        self.info(
            "   Terminal hooks: "
            f"pending={stats['hooks']['pending']} "
            f"processing={stats['hooks']['processing']} "
            f"stale={stats['hooks']['stale']} "
            f"failed={stats['hooks']['failed']} "
            f"quarantined={stats['hooks']['quarantined']}"
        )
