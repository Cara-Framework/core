"""Template for a development-environment reset: caches, queues, workers, data.

Starting a clean run on a dev box is a fixed sequence — stop the workers, drop
the idempotency cache, empty the broker, then (only if asked) wipe data —
and getting the ORDER wrong is what makes a "reset" produce results that are
still contaminated. Flushing the cache while a worker is still consuming
re-populates it; truncating tables while messages are queued leaves jobs
referencing rows that no longer exist. That sequence, the flag surface, the
confirmation gate and the production refusal are the same for any application,
so they live here.

What is NOT here, deliberately: the SQL. ``_truncate()`` is abstract and the
application owns it outright. Truncate policy is the one part of a reset that
is irreversibly destructive and product-negotiated — which tables are
operational, which hold identity and credentials, whether the statement may
CASCADE (it usually must not: ``TRUNCATE ... CASCADE`` empties every table with
a foreign key into the listed set, regardless of row contents or ON DELETE
actions). A framework that emitted that statement would be choosing, on the
application's behalf, what a mistake costs. It never gets to.

Subclasses supply: a ``name``, ``queue_names()``, and ``_truncate()``. They may
override ``dlx_queue_names()``, ``canonical_dlx()``, ``db_confirm_message()``,
``_extra_cache_steps()`` and ``_extra_db_steps()``.

This class has no ``name`` and is never decorated, so command discovery cannot
register it: only the application's concrete subclass reaches the CLI.
"""

from __future__ import annotations

from typing import Any

from cara.commands.CommandBase import CommandBase
from cara.configuration import config
from cara.facades import Cache, Log
from cara.queues import DEAD_LETTER_EXCHANGE
from cara.support import Process, Sleep

try:
    import pika
except ImportError:  # pragma: no cover - exercised only without the extra
    pika = None  # type: ignore[assignment]

try:
    import redis
except ImportError:  # pragma: no cover - exercised only without the extra
    redis = None  # type: ignore[assignment]


class DevResetCommand(CommandBase):
    """Reset development state; the application owns the destructive half."""

    help = "Flush caches + purge queues + optionally truncate operational tables"
    _cli_options = [
        {
            "name": "--db",
            "help": "Also TRUNCATE operational tables (destructive; default: off)",
            "is_flag": True,
        },
        {
            "name": "--keep-queues",
            "help": "Skip AMQP purge (keeps in-flight jobs)",
            "is_flag": True,
        },
        {
            "name": "--keep-cache",
            "help": "Skip cache flush (keeps idempotency keys)",
            "is_flag": True,
        },
        {
            "name": "--dlx",
            "help": "Drop bound queues so the next publisher re-declares canonical args",
            "is_flag": True,
        },
        {
            "name": "--kill-workers",
            "help": "Also stop running worker processes first",
            "is_flag": True,
        },
        {
            "name": "--yes",
            "help": "Skip the confirmation prompt for --db",
            "is_flag": True,
        },
    ]

    #: Process command-line fragments matched when ``--kill-workers`` is given.
    WORKER_PROCESS_PATTERNS: tuple[str, ...] = ("queue:work", "schedule:work")

    #: Seconds a worker gets to finish its current job before SIGKILL.
    WORKER_DRAIN_SECONDS: int = 4

    # ── the sequence ──────────────────────────────────────────────────
    def handle(
        self,
        db: bool = False,
        keep_queues: bool = False,
        keep_cache: bool = False,
        dlx: bool = False,
        kill_workers: bool = False,
        yes: bool = False,
    ) -> int:
        steps_run: list[str] = []

        # Workers first: anything below is undone by a live consumer.
        if kill_workers:
            self._kill_workers()
            steps_run.append("workers killed")

        if not keep_cache:
            self._flush_cache()
            steps_run.append("cache flushed")

        if not keep_queues:
            self._purge_queues()
            steps_run.append("queues purged")

        if dlx:
            self._rebuild_dlx_queues()
            steps_run.append("DLX queues rebuilt")

        if db:
            # Belt-and-suspenders: ``--yes`` is enough for a dev cluster, but
            # one paste into the wrong shell would otherwise wipe production.
            if (config("app.env", "") or "").lower() in ("production", "prod"):
                self.error(
                    "Refusing to TRUNCATE operational tables in production. "
                    "If you really need to reset, switch APP_ENV first."
                )
                return 1
            if not yes:
                self.warning(self.db_confirm_message())
                return 1
            steps_run.extend(self._run_db_steps())

        self.info(f"✓ Reset complete — {', '.join(steps_run) or 'noop'}")
        return 0

    # ── application seams ─────────────────────────────────────────────
    def queue_names(self) -> set[str]:
        """Every queue ``--keep-queues`` would otherwise leave full."""
        raise NotImplementedError

    def dlx_queue_names(self) -> set[str]:
        """Queues ``--dlx`` drops so the next publisher redeclares them."""
        return set(self.queue_names())

    def canonical_dlx(self) -> str:
        """The dead-letter exchange name reported after a ``--dlx`` rebuild."""

        return DEAD_LETTER_EXCHANGE

    def db_confirm_message(self) -> str:
        """The warning shown when ``--db`` is given without ``--yes``."""
        return "--db will TRUNCATE operational tables. Rerun with --yes to proceed."

    def truncate_label(self) -> str:
        """Summary label reported once ``_truncate`` has run."""
        return "tables truncated"

    def _truncate(self) -> None:
        """Wipe the application's operational tables.

        Owned by the application in full: the framework never chooses the
        statement, the table list, or whether it may CASCADE.
        """
        raise NotImplementedError

    def _extra_cache_steps(self) -> None:
        """Application caches that are not the framework cache store."""

    def _pre_db_steps(self) -> list[str]:
        """Cleanup that must happen BEFORE the tables are wiped.

        Returns the step labels to report.
        """
        return []

    def _extra_db_steps(self) -> list[str]:
        """Post-truncate cleanup (search indexes, payload directories).

        Returns the step labels to report.
        """
        return []

    # ── step implementations ──────────────────────────────────────────
    def _run_db_steps(self) -> list[str]:
        before = self._pre_db_steps()
        self._truncate()
        return [*before, self.truncate_label(), *self._extra_db_steps()]

    def _flush_cache(self) -> None:
        """Flush the cache store so idempotency keys cannot leak across runs."""
        try:
            Cache.flush()
            self.line("  • Cache.flush() OK")
        except Exception as exc:  # noqa: BLE001 — falls back to raw redis below
            Log.warning(
                "[dev:reset] Cache.flush failed: %s; trying raw redis",
                exc,
                category="command",
            )
            self._flush_redis_raw()
        self._extra_cache_steps()

    def _flush_redis_raw(self) -> None:
        if redis is None:  # pragma: no cover - exercised only without the extra
            self.warning("  • redis flush skipped: the redis client is not installed")
            return
        try:
            client = redis.Redis(
                host=config("cache.drivers.redis.host", "127.0.0.1"),
                port=int(config("cache.drivers.redis.port", 6379)),
                password=config("cache.drivers.redis.password") or None,
                db=int(config("cache.drivers.redis.db", 0)),
            )
            client.flushdb()
            self.line("  • redis FLUSHDB OK")
        except Exception as exc:  # noqa: BLE001 — reported to the operator
            self.warning(f"  • redis flush failed: {exc}")

    def _purge_queues(self) -> None:
        """Empty every queue the application declares.

        A management connection rather than a shelled-out broker CLI, so this
        never depends on what is on PATH.
        """
        connection = self._open_broker()
        if connection is None:
            return
        try:
            channel = connection.channel()
            queue_names = set(self.queue_names())
            purged = 0
            for name in sorted(queue_names):
                try:
                    channel.queue_purge(queue=name)
                    purged += 1
                except Exception as exc:  # noqa: BLE001 — a missing queue is fine
                    Log.debug("Could not purge queue %s: %s", name, exc, category="reset")
                    channel = connection.channel()  # a 404 closed the channel
            self.line(f"  • queues purged: {purged}/{len(queue_names)}")
        finally:
            self._close_broker(connection)

    def _rebuild_dlx_queues(self) -> None:
        """Drop bound queues so the next publisher redeclares canonical args.

        Fixes ``PRECONDITION_FAILED - inequivalent arg`` after a topology
        change. Loses in-flight messages, which is why it is opt-in.
        """
        connection = self._open_broker()
        if connection is None:
            return
        try:
            rebuilt = 0
            for name in sorted(self.dlx_queue_names()):
                channel = connection.channel()
                try:
                    channel.queue_delete(queue=name, if_unused=False, if_empty=False)
                    rebuilt += 1
                except Exception as exc:  # noqa: BLE001 — a missing queue is fine
                    Log.debug(
                        "Could not delete queue %s: %s", name, exc, category="reset"
                    )
            self.line(
                f"  • DLX-mismatched queues dropped: {rebuilt} "
                f"(canonical DLX: {self.canonical_dlx()})"
            )
        finally:
            self._close_broker(connection)

    def _kill_workers(self) -> None:
        """Two-phase SIGTERM → SIGKILL so a worker can drain its current job."""
        try:
            for pattern in self.WORKER_PROCESS_PATTERNS:
                Process.command(["pkill", "-TERM", "-f", pattern]).timeout(600).run()
            Sleep.for_(self.WORKER_DRAIN_SECONDS).seconds()
            for pattern in self.WORKER_PROCESS_PATTERNS:
                Process.command(["pkill", "-KILL", "-f", pattern]).timeout(600).run()
            self.line(f"  • {' / '.join(self.WORKER_PROCESS_PATTERNS)} processes stopped")
        except Exception as exc:  # noqa: BLE001 — reported to the operator
            self.warning(f"  • pkill failed: {exc}")

    # ── broker plumbing ───────────────────────────────────────────────
    def _open_broker(self) -> Any | None:
        if pika is None:  # pragma: no cover - exercised only without the extra
            self.warning("  • AMQP step skipped: pika is not installed")
            return None
        try:
            return pika.BlockingConnection(self._pika_params())
        except Exception as exc:  # noqa: BLE001 — reported to the operator
            self.warning(f"  • AMQP connect failed: {exc}")
            return None

    @staticmethod
    def _close_broker(connection: Any) -> None:
        try:
            connection.close()
        except Exception as exc:  # noqa: BLE001 — teardown must not mask the run
            Log.warning("AMQP connection close failed: %s", exc, category="reset")

    def _pika_params(self) -> Any:
        credentials = pika.PlainCredentials(
            config("queue.drivers.amqp.username", "guest"),
            config("queue.drivers.amqp.password", "guest"),
        )
        return pika.ConnectionParameters(
            host=config("queue.drivers.amqp.host", "localhost"),
            port=int(config("queue.drivers.amqp.port", 5672)),
            virtual_host=config("queue.drivers.amqp.vhost", "/"),
            credentials=credentials,
            heartbeat=30,
            socket_timeout=10,
        )
