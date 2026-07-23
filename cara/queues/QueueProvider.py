"""
Queue Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the
signed AMQP queue subsystem.
"""

from __future__ import annotations

from cara.configuration import config
from cara.exceptions import QueueConfigurationException
from cara.foundation import DeferredProvider
from cara.queues.drivers import AMQPDriver
from cara.queues.Queue import Queue


class QueueProvider(DeferredProvider):
    @classmethod
    def provides(cls) -> list[str]:
        return ["queue", "JobTracker"]

    def register(self) -> None:
        """Register queue services with configuration."""
        default_driver = config("queue.default")
        drivers = config("queue.drivers", {}) or {}

        if default_driver != "amqp":
            raise QueueConfigurationException(
                "AMQP is the only supported queue execution driver."
            )
        unsupported = set(drivers) - {"amqp"}
        if unsupported:
            raise QueueConfigurationException(
                "Unsupported queue execution drivers configured: "
                + ", ".join(sorted(unsupported))
            )
        if "amqp" not in drivers:
            raise QueueConfigurationException("Missing queue.drivers.amqp config.")

        queue_manager = Queue(self.application, default_driver)

        self._add_amqp_driver(queue_manager)

        self.application.bind("queue", queue_manager)
        self._register_job_tracker()

    def _add_amqp_driver(self, queue_manager: Queue) -> None:
        """Register AMQP queue driver with configuration."""
        if not config("queue.drivers.amqp"):
            return

        username = config("queue.drivers.amqp.username")
        password = config("queue.drivers.amqp.password")
        canonical_queues = sorted(
            str(queue_name)
            for queue_name in (config("queue.canonical_queues", ()) or ())
        )

        if not username or not password or not canonical_queues:
            raise QueueConfigurationException(
                "AMQP requires credentials and canonical topic bindings."
            )

        driver = AMQPDriver(
            application=self.application,
            options={
                "username": username,
                "password": password,
                # Process capability (none/consume/publish/topology/full).
                # verify_runtime_health() keys its probes off this value; if it
                # is not forwarded, every process defaults to "full" and even a
                # deliberately broker-less role (access "none" ships sentinel
                # credentials) dials the broker and dies on a login 403.
                "broker_access": config(
                    "queue.drivers.amqp.broker_access",
                    "full",
                ),
                "host": config("queue.drivers.amqp.host", "localhost"),
                "port": config("queue.drivers.amqp.port", 5672),
                "vhost": config("queue.drivers.amqp.vhost", "/"),
                "scheme": config("queue.drivers.amqp.scheme", "amqp"),
                "ssl_ca_certs": config("queue.drivers.amqp.ssl_ca_certs"),
                "ssl_certfile": config("queue.drivers.amqp.ssl_certfile"),
                "ssl_keyfile": config("queue.drivers.amqp.ssl_keyfile"),
                "exchange": config("queue.drivers.amqp.exchange", ""),
                "connection_options": config(
                    "queue.drivers.amqp.connection_options", {}
                ),
                "canonical_queues": canonical_queues,
                "topology_sentinel_exchange": config(
                    "queue.topology_sentinel_exchange",
                    "",
                ),
                "tz": config("queue.drivers.amqp.tz", "UTC"),
                "allowed_job_prefixes": config(
                    "queue.drivers.amqp.allowed_job_prefixes", ()
                ),
                "max_priority": config("queue.drivers.amqp.max_priority"),
                "max_length": config(
                    "queue.drivers.amqp.max_length",
                    100000,
                ),
                "max_length_bytes": config(
                    "queue.drivers.amqp.max_length_bytes",
                    1073741824,
                ),
                "priority_levels": config(
                    "queue.drivers.amqp.priority_levels", {}
                ),
                "signing_key_id": config(
                    "queue.drivers.amqp.signing_key_id",
                    "",
                ),
                "signing_keys": config("queue.drivers.amqp.signing_keys", {}),
                "delivery_table": config(
                    "queue.drivers.amqp.delivery_table",
                    "queue_job_delivery",
                ),
                "delivery_claim_batch": config(
                    "queue.drivers.amqp.delivery_claim_batch",
                    100,
                ),
                "delivery_execution_lease_seconds": config(
                    "queue.drivers.amqp.delivery_execution_lease_seconds",
                    7200,
                ),
                "delivery_execution_lease_grace_seconds": config(
                    "queue.drivers.amqp.delivery_execution_lease_grace_seconds",
                    300,
                ),
                "delivery_default_job_timeout_seconds": config(
                    "queue.drivers.amqp.delivery_default_job_timeout_seconds",
                    300,
                ),
                "delivery_audit_retention_days": config(
                    "queue.drivers.amqp.delivery_audit_retention_days",
                    90,
                ),
                "delivery_audit_safety_days": config(
                    "queue.drivers.amqp.delivery_audit_safety_days",
                    7,
                ),
                "delivery_hook_timeout_seconds": config(
                    "queue.drivers.amqp.delivery_hook_timeout_seconds",
                    60,
                ),
                "delivery_publish_lease_seconds": config(
                    "queue.drivers.amqp.delivery_publish_lease_seconds",
                    300,
                ),
                "envelope_ttl_seconds": config(
                    "queue.drivers.amqp.envelope_ttl_seconds",
                    604800,
                ),
                "envelope_max_age_seconds": config(
                    "queue.drivers.amqp.envelope_max_age_seconds",
                    2678400,
                ),
                "clock_skew_seconds": config(
                    "queue.drivers.amqp.clock_skew_seconds",
                    30,
                ),
            },
        )
        queue_manager.add_driver(AMQPDriver.driver_name, driver)

    def _register_job_tracker(self) -> None:
        """Register JobTracker singleton with unified Job model from container."""
        from cara.queues.tracking import JobTracker

        def create_job_tracker():
            job_model = self.application.make("Job") if self.application.has("Job") else None
            return JobTracker(job_model=job_model)

        self.application.singleton("JobTracker", create_job_tracker)
