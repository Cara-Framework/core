"""Process-local routing-key validation for canonical queues."""

from __future__ import annotations

from dataclasses import dataclass

from cara.exceptions import QueueException
from cara.facades import Log


@dataclass
class RoutingKey:
    """Parsed routing key components."""

    domain: str
    subtype: str
    priority: str

    @property
    def key(self) -> str:
        """Get full routing key string."""
        return f"{self.domain}.{self.subtype}.{self.priority}"

    @classmethod
    def parse(cls, routing_key: str) -> RoutingKey:
        """Parse routing key string into components."""
        parts = routing_key.split(".")
        if len(parts) != 3:
            raise QueueException(
                f"Invalid routing key format: {routing_key}. Expected: domain.subtype.priority"
            )

        return cls(domain=parts[0], subtype=parts[1], priority=parts[2])


class QueueRouter:
    """
    Map product routing keys to one canonical direct-publish queue.

    Features:
    This class never declares or publishes to a broker exchange. It validates
    a routing key locally, selects exactly one canonical queue, then delegates
    to the durable delivery ledger.

    Usage:
        router = QueueRouter()

        # Define queue bindings
        router.bind_queue("jobs", "jobs.*.*")

        # Dispatch with routing key
        router.dispatch_job(
            routing_key="jobs.process.high",
            job_instance=my_job,
            payload={"id": 123},
        )
    """

    _instance: QueueRouter | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Prevent re-initialization
        if hasattr(self, "_initialized"):
            return

        self.queue_bindings: dict[str, list[str]] = {}
        self._queue_patterns: dict[str, list[str]] = {}
        self._logged_bindings = set()

        # Auto-bind standard queues
        self._setup_default_bindings()

        # Initialization complete - no logging to reduce spam
        self._initialized = True

    def _setup_default_bindings(self):
        """Setup queue bindings from app configuration."""
        # Load app-specific bindings from config (required)
        from cara.configuration import config

        app_bindings = config("queue.queue_routing_rules", None)

        if not app_bindings:
            raise QueueException(
                "QUEUE_ROUTING_RULES not found in queue config."
            )

        default_bindings = app_bindings

        for queue_name, pattern in default_bindings:
            self.bind_queue(queue_name, pattern)

    def bind_queue(self, queue_name: str, routing_pattern: str) -> None:
        """
        Bind a queue to a routing pattern.

        Args:
            queue_name: Name of the queue to bind
            routing_pattern: Routing pattern (e.g., "enrichment.*.high")
        """
        if queue_name not in self.queue_bindings:
            self.queue_bindings[queue_name] = []

        if routing_pattern not in self.queue_bindings[queue_name]:
            self.queue_bindings[queue_name].append(routing_pattern)

            # Only log once when binding is first created
            binding_key = f"{queue_name}->{routing_pattern}"
            if binding_key not in self._logged_bindings:
                Log.debug(
                    "Queue routing rule: %s -> %s",
                    queue_name,
                    routing_pattern,
                    category="cara.queue.routing",
                )
                self._logged_bindings.add(binding_key)

    def get_matching_queues(self, routing_key: str) -> list[str]:
        """
        Get queues that match the routing key.

        Args:
            routing_key: Full routing key (e.g., "enrichment.product.high")

        Returns:
            List of matching queue names
        """
        matching_queues = []

        for queue_name, patterns in self.queue_bindings.items():
            for pattern in patterns:
                if self._matches_pattern(routing_key, pattern):
                    matching_queues.append(queue_name)
                    break  # Don't add same queue multiple times

        return matching_queues

    def _matches_pattern(self, routing_key: str, pattern: str) -> bool:
        """
        Check if routing key matches the pattern.

        Supports:
        - * matches exactly one word
        - # matches zero or more words (not implemented for simplicity)
        """
        routing_parts = routing_key.split(".")
        pattern_parts = pattern.split(".")

        if len(routing_parts) != len(pattern_parts):
            return False

        for routing_part, pattern_part in zip(routing_parts, pattern_parts, strict=False):
            if pattern_part != "*" and pattern_part != routing_part:
                return False

        return True

    def dispatch_job(
        self,
        routing_key: str,
        job_instance,
        payload: dict | None = None,
        delay: float | None = None,
    ) -> str:
        """
        Dispatch job to appropriate queue based on routing key.

        Args:
            routing_key: Routing key (e.g., "enrichment.product.high")
            job_instance: Job instance to dispatch
            payload: Additional payload data

        Returns:
            Job ID string on success.

        Raises:
            ValueError: routing_key is malformed (RoutingKey.parse).
            RuntimeError: routing_key has no matching queue binding —
                publishing would silently black-hole the message.
            pika.exceptions.AMQPError / OSError: broker errors that
                survive the internal retry loop. Propagated so callers
                can decide whether to release locks, retry, alert, etc.

        NOTE on previous behaviour:
            This method used to wrap the entire body in
            ``except Exception: Log.error(...); return None``.  That
            outer catch defeated the explicit ``raise`` paths above
            (FAIL LOUD on no matching queues, ValueError from a
            malformed routing key, exhausted publish retries) by
            converting every failure into a silent ``return None``.
            Callers — notably ``PendingDispatch._dispatch_via_router``,
            whose docstring already promised "NO FALLBACK - If routing
            dispatch fails, exception is raised" — would store
            ``str(None)`` as the tracking id and continue as if the
            publish had succeeded.  The scenario 2 (cycle 2) heavy
            flood reproduced this: 1500 messages dispatched with a
            2-segment routing key (``RoutingKey.parse`` raised
            ``ValueError``) and the test loop saw ``errors=0`` while
            the broker received zero of them.  Removing the outer
            catch lets the caller observe the failure.
        """
        RoutingKey.parse(routing_key)
        matching_queues = self.get_matching_queues(routing_key)

        if not matching_queues:
            # FAIL LOUD: raising here surfaces the misconfiguration
            # immediately rather than letting work silently disappear.
            msg = (
                f"No queues match routing key '{routing_key}'. "
                f"Add a rule to QUEUE_ROUTING_RULES or fix the "
                f"routing_key — refusing to dispatch into a black hole."
            )
            Log.error(msg, category="cara.queue.routing")
            raise QueueException(msg)

        if len(matching_queues) != 1:
            raise QueueException(
                f"Routing key {routing_key!r} matched {len(matching_queues)} queues "
                f"({matching_queues}); exactly one canonical stage queue is required."
            )
        target_queue = matching_queues[0]

        if hasattr(job_instance, "queue"):
            job_instance.queue = target_queue
        if hasattr(job_instance, "routing_key"):
            job_instance.routing_key = routing_key

        from cara.facades import Queue

        # Queue.push/later is now a PostgreSQL transaction, not broker I/O.
        # Retrying an ambiguous DB commit would mint a second delivery ID.
        if delay:
            job_id = Queue.later(delay, job_instance)
        else:
            job_id = Queue.push(job_instance)
        Log.debug(
            "Job dispatched: %s -> %s [%s]",
            routing_key,
            target_queue,
            job_id,
            category="cara.queue.routing",
        )
        return str(job_id)

    @staticmethod
    def _pattern_segments(pattern: str) -> tuple[str, str, str]:
        """Split a ``domain.subtype.priority`` routing pattern.

        Missing trailing segments (``orders.#``) come back as ``"#"`` so
        callers can treat them as match-anything.
        """
        parts = pattern.split(".")
        parts += ["#"] * (3 - len(parts))
        return parts[0], parts[1], parts[2]

    def get_queue_info(self) -> dict[str, dict]:
        """Get information about all bound queues.

        Derived from ``queue_bindings`` — the single source of truth
        ``bind_queue`` maintains. (An earlier ``bindings`` dict existed
        for this but was never populated, so introspection always came
        back empty.)
        """
        queue_info = {}

        for queue_name, patterns in self.queue_bindings.items():
            domain, _subtype, priority = self._pattern_segments(patterns[0])
            queue_info[queue_name] = {
                "routing_pattern": patterns[0],
                "routing_patterns": list(patterns),
                "domain": domain,
                "priority": priority,
            }

        return queue_info

    def list_queues_for_domain(self, domain: str) -> list[str]:
        """Get all queues serving a specific domain.

        A queue counts as serving the domain when any of its patterns
        names that domain outright or wildcards it (``*``/``#``).
        """
        matches = []
        for queue_name, patterns in self.queue_bindings.items():
            for pattern in patterns:
                pattern_domain = self._pattern_segments(pattern)[0]
                if pattern_domain in (domain, "*", "#"):
                    matches.append(queue_name)
                    break
        return matches
