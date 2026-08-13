"""Queue worker configuration, wildcard discovery and display."""

from __future__ import annotations

import base64
import contextlib
import fnmatch as _fnmatch
import json
import urllib.parse
import urllib.request
from typing import Any

from cara.configuration import config
from cara.exceptions import ConfigurationException, InvalidArgumentException

from .AMQPConnectionManager import AMQPConnectionManager


def _queue_work_prepare_config(
    self,
    driver: str | None,
    queue: str | None,
    timeout: str | None,
    max_jobs: str | None,
    max_time: str | None,
) -> dict[str, Any]:
    """Prepare and validate worker configuration."""
    # Determine driver
    driver_name = driver or config("queue.default")
    if not driver_name:
        raise ConfigurationException(
            "No driver specified and no default 'queue.default' configured"
        )

    drivers = config("queue.drivers", {})
    if driver_name not in drivers:
        raise ConfigurationException(f"Driver '{driver_name}' is not configured")

    # Parse timeout
    timeout_val = 5
    if timeout:
        try:
            timeout_val = int(timeout)
            if timeout_val < 1:
                raise InvalidArgumentException("Timeout must be at least 1 second")
        except ValueError as e:
            raise InvalidArgumentException(f"Invalid timeout value: {e}") from e
    else:
        # Get from driver config
        timeout_val = config(f"queue.drivers.{driver_name}.poll", 5)

    # Parse limits
    max_jobs_val = None
    if max_jobs:
        try:
            max_jobs_val = int(max_jobs)
            if max_jobs_val <= 0:
                raise InvalidArgumentException("max-jobs must be positive")
        except ValueError as e:
            raise InvalidArgumentException(f"Invalid max-jobs value: {e}") from e

    max_time_val = None
    if max_time:
        try:
            max_time_val = int(max_time)
            if max_time_val <= 0:
                raise InvalidArgumentException("max-time must be positive")
        except ValueError as e:
            raise InvalidArgumentException(f"Invalid max-time value: {e}") from e

    return {
        "driver_name": driver_name,
        "queue_names": self._parse_queue_names(queue),
        "timeout": timeout_val,
        "max_jobs": max_jobs_val,
        "max_time": max_time_val,
    }


def _queue_work_resolve_pool(self, pool_name: str) -> dict[str, Any] | None:
    """Resolve a named worker pool from config/queue.py WORKER_POOLS.

    Returns the pool dict on success, or None after printing an error.
    """
    pools = config("queue.worker_pools", None)
    if not pools:
        self.error("× No WORKER_POOLS defined in config/queue.py")
        return None
    if pool_name not in pools:
        available = ", ".join(sorted(pools.keys()))
        self.error(f"× Pool '{pool_name}' not found. Available: {available}")
        return None
    pool_cfg = pools[pool_name]
    if not pool_cfg.get("queues"):
        self.error(f"× Pool '{pool_name}' has no queues defined")
        return None
    self.console.print(
        f"  [bold #30e047]Pool:[/bold #30e047] [white]{pool_name}[/white] "
        f"[dim]({len(pool_cfg['queues'])} queues, "
        f"concurrency={pool_cfg.get('concurrency', 1)}, "
        f"timeout={pool_cfg.get('timeout', 5)}s)[/dim]"
    )
    return pool_cfg


def _queue_work_parse_queue_names(self, queue: str | None) -> list:
    """Parse queue names from comma-separated string with wildcard support."""
    if not queue:
        return ["default"]

    # Split by comma and clean up
    queue_patterns = [q.strip() for q in queue.split(",")]
    queue_patterns = [q for q in queue_patterns if q]  # Remove empty strings

    if not queue_patterns:
        return ["default"]

    # Expand wildcard patterns
    expanded_queues = []
    for pattern in queue_patterns:
        # Trailing-dot prefix shorthand: "discovery." means "every
        # priority sub-queue of discovery" — i.e. discovery.{critical,
        # high,default,low} (plus any nested queues the management API
        # reports, e.g. notification.email.default). This is the form
        # the operator-facing docs and the e2e queue:work command use.
        #
        # Without this normalisation a bare "discovery." fell through
        # the ``"*" in pattern`` check and was polled as a LITERAL queue
        # name. RabbitMQ has no queue called "discovery.", so the worker
        # lazily created an empty one and consumed from it forever while
        # the real discovery.default (where a discovery job actually
        # lands) was never read — the whole pipeline stalled at dispatch.
        # Mapping "prefix." → "prefix.*" routes it through the same
        # expansion the wildcard form already uses.
        if pattern.endswith(".") and "*" not in pattern:
            pattern = f"{pattern}*"

        if "*" in pattern:
            expanded_queues.extend(self._expand_wildcard_pattern(pattern))
        else:
            expanded_queues.append(pattern)

    # De-duplicate while preserving the operator/config sequence so a
    # queue named by two overlapping patterns (e.g. "discovery." and
    # "discovery.high") isn't polled twice per cycle.
    seen: set[str] = set()
    deduped = [q for q in expanded_queues if not (q in seen or seen.add(q))]

    return deduped if deduped else ["default"]


def _queue_work_expand_wildcard_pattern(self, pattern: str) -> list:
    """Expand wildcard pattern to actual queue names.

    Two-phase expansion:
    1. Try to discover real queues from RabbitMQ Management API and
       match with fnmatch. This catches nested prefixes like
       ``notification.email.default`` when the user passes
       ``notification.*``.
    2. Merge canonical queue names from configured bindings so the worker
       starts correctly even when RabbitMQ management is unavailable.
    """

    if pattern.endswith(".*"):
        static: set[str] = set()
    elif pattern.endswith("*"):
        static = set()
    else:
        return [pattern]

    # Merge with any extra queues discovered from RabbitMQ
    # (e.g. notification.email.default) that the static set misses.
    discovered = self._discover_rabbitmq_queues()
    if discovered:
        matched = {q for q in discovered if _fnmatch.fnmatch(q, pattern)}
        static |= matched

    # Also merge canonical queue names declared in the process-local
    # routing rules that match this pattern. Live RabbitMQ discovery only
    # sees queues that ALREADY exist at worker startup; a queue first
    # created mid-run — e.g. ``notification.email`` the moment the first
    # email job is dispatched after the worker booted — would otherwise
    # never be polled, so those messages pile up with no consumer. The
    # rules are the declarative source of truth for canonical queue
    # names (notification.email/sms/push, etc.), so consulting them makes
    # a ``notification.*`` worker pick up those channel queues regardless
    # of broker timing. Missing queues are handled gracefully by the
    # per-queue declare path, so adding a not-yet-created name is safe.
    try:
        bindings = config("queue.queue_routing_rules", []) or []
        bound = {name for name, _routing in bindings if _fnmatch.fnmatch(name, pattern)}
        static |= bound
    except ImportError, RuntimeError, AttributeError, OSError:
        pass

    return sorted(static)


def _queue_work_discover_rabbitmq_queues(self) -> list:
    """Fetch existing queue names from RabbitMQ Management API.

    Returns an empty list on any failure so the caller can
    fall back to static expansion.
    """
    if hasattr(self, "_rabbitmq_queues_cache"):
        return self._rabbitmq_queues_cache

    try:
        # The AMQP config lives under queue.drivers.amqp.* (see
        # QueueProvider) — the old queue.connections.amqp.* paths never
        # existed, so discovery always probed guest@127.0.0.1.
        host = config("queue.drivers.amqp.host", "127.0.0.1")
        mgmt_port = config("queue.drivers.amqp.management_port", 15672)
        user = config("queue.drivers.amqp.username", "guest")
        password = config("queue.drivers.amqp.password", "guest")
        vhost = config("queue.drivers.amqp.vhost", "/")

        encoded_vhost = urllib.parse.quote(vhost, safe="")
        url = f"http://{host}:{mgmt_port}/api/queues/{encoded_vhost}"

        req = urllib.request.Request(url)
        credentials = f"{user}:{password}"

        auth = base64.b64encode(credentials.encode()).decode()
        req.add_header("Authorization", f"Basic {auth}")

        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            queues = [q["name"] for q in data if isinstance(q, dict) and "name" in q]
            self._rabbitmq_queues_cache = queues
            return queues
    except Exception:
        self._rabbitmq_queues_cache = []
        return []


def _queue_work_show_config(self, config: dict[str, Any]):
    """Display worker configuration in ServeCommand style."""
    self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")

    # Driver info
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Driver:[/white] [bold white]{config['driver_name'].upper()}[/bold white]"
    )

    # Queue info
    queue_names = config["queue_names"]
    if len(queue_names) > 1:
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Queues:[/white] [dim]{len(queue_names)} canonical queues[/dim]"
        )
        for i, queue in enumerate(queue_names, 1):  # Show all queues
            priority_color = (
                "#E21102"
                if "critical" in queue
                else "#e5c07b"
                if "high" in queue
                else "#30e047"
                if "default" in queue
                else "dim"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{priority_color}]{queue}[/{priority_color}]"
            )
    else:
        queue_color = (
            "#E21102"
            if "critical" in queue_names[0]
            else "#e5c07b"
            if "high" in queue_names[0]
            else "#30e047"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Queue:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
        )

    # Timing and limits
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Reconnect Backoff:[/white] [dim]{config['timeout']}s[/dim]"
    )

    if config.get("max_jobs"):
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Max Jobs:[/white] [dim]{config['max_jobs']}[/dim]"
        )
    if config.get("max_time"):
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Max Runtime:[/white] [dim]{config['max_time']}s[/dim]"
        )

    # Auto-reload status (default: enabled in development)

    auto_reload = bool(self.option("reload"))
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if auto_reload else '#E21102'}]{'✓' if auto_reload else '×'}[/{'#30e047' if auto_reload else '#E21102'}]"
    )

    self.console.print("[#e5c07b]└─[/#e5c07b]")
    self.console.print()


def _queue_work_verify_consumer_queue(
    connection_manager: AMQPConnectionManager,
    queue_name: str,
) -> None:
    """Passively require one deploy-reconciled canonical queue."""
    channel = connection_manager.create_channel()
    if channel is None:
        raise ConnectionError("RabbitMQ channel could not be created")
    try:
        channel.queue_declare(
            queue=queue_name,
            passive=True,
        )
    finally:
        with contextlib.suppress(
            ImportError,
            RuntimeError,
            AttributeError,
            OSError,
        ):
            channel.close()
