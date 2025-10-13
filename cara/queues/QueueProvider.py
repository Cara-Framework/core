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
        settings = config("queue", {})
        default_driver = settings.get("default", None)
        drivers_config = settings.get("drivers", {}) or {}

        if not default_driver or default_driver not in drivers_config:
            raise QueueConfigurationException(
                "Missing or invalid 'queue.default' or 'queue.drivers' config."
            )

        queue_manager = Queue(self.application, default_driver)

        # Register queue drivers
        self._add_database_driver(queue_manager, drivers_config.get("database"))
        self._add_amqp_driver(queue_manager, drivers_config.get("amqp"))
        self._add_async_driver(queue_manager, drivers_config.get("async"))
        self._add_redis_driver(queue_manager, drivers_config.get("redis"))

        self.application.bind("queue", queue_manager)

        # Register JobTracker (lazy singleton)
        self._register_job_tracker()

    def _add_database_driver(
        self,
        queue_manager: Queue,
        settings: Optional[Dict[str, Any]],
    ) -> None:
        """Register database queue driver with configuration."""
        if not settings:
            return
        connection = settings.get("connection")
        table = settings.get("table")
        failed_table = settings.get("failed_table")
        attempts = settings.get("attempts", 1)
        poll = settings.get("poll", 5)
        tz = settings.get("tz", "UTC")
        queue_name = settings.get("queue", "default")

        if not connection or not table:
            raise QueueConfigurationException(
                "'queue.drivers.database.connection' and 'table' must be defined."
            )

        driver = DatabaseDriver(
            application=self.application,
            options={
                "connection": connection,
                "table": table,
                "failed_table": failed_table,
                "attempts": attempts,
                "poll": poll,
                "tz": tz,
                "queue": queue_name,
            },
        )
        queue_manager.add_driver(DatabaseDriver.driver_name, driver)

    def _add_amqp_driver(
        self,
        queue_manager: Queue,
        settings: Optional[Dict[str, Any]],
    ) -> None:
        """Register AMQP queue driver with configuration."""
        if not settings:
            return
        username = settings.get("username")
        password = settings.get("password")
        host = settings.get("host", "localhost")
        port = settings.get("port", 5672)
        vhost = settings.get("vhost", "/")
        exchange = settings.get("exchange", "")
        connection_options = settings.get("connection_options", {})
        queue_name = settings.get("queue")
        tz = settings.get("tz", "UTC")

        if not username or not password or not queue_name:
            raise QueueConfigurationException(
                "Missing required 'queue.drivers.amqp.username', 'password', or 'queue'."
            )

        driver = AMQPDriver(
            application=self.application,
            options={
                "username": username,
                "password": password,
                "host": host,
                "port": port,
                "vhost": vhost,
                "exchange": exchange,
                "connection_options": connection_options,
                "queue": queue_name,
                "tz": tz,
            },
        )
        queue_manager.add_driver(AMQPDriver.driver_name, driver)

    def _add_async_driver(
        self,
        queue_manager: Queue,
        settings: Optional[Dict[str, Any]],
    ) -> None:
        """Register async queue driver with configuration."""
        if not settings:
            return
        blocking = settings.get("blocking", False)
        callback = settings.get("callback", "handle")
        mode = settings.get("mode", "threading")
        workers = settings.get("workers", None)

        driver = AsyncDriver(
            application=self.application,
            options={
                "blocking": blocking,
                "callback": callback,
                "mode": mode,
                "workers": workers,
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
        except Exception as e:
            raise QueueConfigurationException(f"Failed to instantiate RedisDriver: {e}")
        queue_manager.add_driver(RedisDriver.driver_name, driver)

    def _register_job_tracker(self) -> None:
        """Register JobTracker singleton with models from container."""
        from cara.queues.tracking import JobTracker

        def create_job_tracker():
            Job = self.application.make("Job") if self.application.has("Job") else None
            JobLog = (
                self.application.make("JobLog")
                if self.application.has("JobLog")
                else None
            )
            return JobTracker(job_log_model=JobLog, job_model=Job)

        self.application.singleton("JobTracker", create_job_tracker)
