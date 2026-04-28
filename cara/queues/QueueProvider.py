"""
Queue Provider for the Cara framework.

This module provides the deferred service provider that configures and registers the queue
subsystem, including various queue drivers.
"""

from typing import Any, Dict, List, Optional

from cara.configuration import config
from cara.exceptions import QueueConfigurationException
from cara.foundation import DeferredProvider
from cara.queues import Queue
from cara.queues.drivers import AMQPDriver, AsyncDriver, DatabaseDriver, RedisDriver


class QueueProvider(DeferredProvider):
    @classmethod
    def provides(cls) -> List[str]:
        return ["queue", "JobTracker"]

    def register(self) -> None:
        """Register queue services with configuration."""
        default_driver = config("queue.default")
        drivers = config("queue.drivers", {}) or {}

        if not default_driver or default_driver not in drivers:
            raise QueueConfigurationException(
                "Missing or invalid 'queue.default' or 'queue.drivers' config."
            )

        queue_manager = Queue(self.application, default_driver)

        self._add_database_driver(queue_manager)
        self._add_amqp_driver(queue_manager)
        self._add_async_driver(queue_manager)
        # Redis driver expects an explicit settings dict; pull it from
        # the same `queue.drivers` config block the other drivers use.
        # Passing None makes _add_redis_driver early-return without
        # touching Redis — that's the desired behaviour on dev boxes
        # without REDIS_URL configured.
        self._add_redis_driver(queue_manager, drivers.get("redis"))

        self.application.bind("queue", queue_manager)
        self._register_job_tracker()

    def _add_database_driver(self, queue_manager: Queue) -> None:
        """Register database queue driver with configuration."""
        if not config("queue.drivers.database"):
            return

        connection = config("queue.drivers.database.connection")
        table = config("queue.drivers.database.table")
        if not connection or not table:
            raise QueueConfigurationException(
                "'queue.drivers.database.connection' and 'table' must be defined."
            )

        driver = DatabaseDriver(
            application=self.application,
            options={
                "connection": connection,
                "table": table,
                "failed_table": config("queue.drivers.database.failed_table"),
                "attempts": config("queue.drivers.database.attempts", 1),
                "poll": config("queue.drivers.database.poll", 5),
                "tz": config("queue.drivers.database.tz", "UTC"),
                "queue": config("queue.drivers.database.queue", "default"),
            },
        )
        queue_manager.add_driver(DatabaseDriver.driver_name, driver)

    def _add_amqp_driver(self, queue_manager: Queue) -> None:
        """Register AMQP queue driver with configuration."""
        if not config("queue.drivers.amqp"):
            return

        username = config("queue.drivers.amqp.username")
        password = config("queue.drivers.amqp.password")
        queue_name = config("queue.drivers.amqp.queue")

        if not username or not password or not queue_name:
            raise QueueConfigurationException(
                "Missing required 'queue.drivers.amqp.username', 'password', or 'queue'."
            )

        driver = AMQPDriver(
            application=self.application,
            options={
                "username": username,
                "password": password,
                "host": config("queue.drivers.amqp.host", "localhost"),
                "port": config("queue.drivers.amqp.port", 5672),
                "vhost": config("queue.drivers.amqp.vhost", "/"),
                "exchange": config("queue.drivers.amqp.exchange", ""),
                "connection_options": config("queue.drivers.amqp.connection_options", {}),
                "queue": queue_name,
                "tz": config("queue.drivers.amqp.tz", "UTC"),
            },
        )
        queue_manager.add_driver(AMQPDriver.driver_name, driver)

    def _add_async_driver(self, queue_manager: Queue) -> None:
        """Register async queue driver with configuration."""
        if not config("queue.drivers.async"):
            return

        driver = AsyncDriver(
            application=self.application,
            options={
                "blocking": config("queue.drivers.async.blocking", False),
                "callback": config("queue.drivers.async.callback", "handle"),
                "mode": config("queue.drivers.async.mode", "threading"),
                "workers": config("queue.drivers.async.workers"),
            },
        )
        queue_manager.add_driver(AsyncDriver.driver_name, driver)

    def _add_redis_driver(
        self,
        queue_manager: Queue,
        settings: Optional[Dict[str, Any]],
    ) -> None:
        """Register Redis queue driver with configuration."""
        if not settings:
            return
        try:
            driver = RedisDriver(
                application=self.application,
                options=settings,
            )
            queue_manager.add_driver(RedisDriver.driver_name, driver)
        except Exception as e:
            # Don't fail entire queue registration if Redis is unavailable
            # Redis driver is optional, only fail if explicitly required
            from cara.facades import Log

            Log.warning(
                f"Redis driver not available (Redis connection failed): {e}. Skipping Redis driver registration."
            )
            # Don't raise exception - allow queue system to work with other drivers
            return

    def _register_job_tracker(self) -> None:
        """Register JobTracker singleton with unified Job model from container."""
        from cara.queues.tracking import JobTracker

        def create_job_tracker():
            Job = self.application.make("Job") if self.application.has("Job") else None
            return JobTracker(job_model=Job)

        self.application.singleton("JobTracker", create_job_tracker)
