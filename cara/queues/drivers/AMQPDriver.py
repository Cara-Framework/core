"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

import json
import logging
import pickle
import uuid
from typing import Any, Dict, List, Union

import pendulum
import pika

from cara.exceptions import DriverLibraryNotFoundException, QueueException
from cara.facades import Log
from cara.queues.contracts.Queue import Queue
from cara.support.Console import HasColoredOutput


class AMQPDriver(HasColoredOutput, Queue):
    """
    AMQP-based queue driver for RabbitMQ.

    Features:
    - Reliable message delivery with publisher confirms
    - Job tracking with unique IDs
    - Integration with JobTracker for status updates
    - Persistent messages and durable queues
    """

    driver_name = "amqp"

    def __init__(self, application, options: Dict[str, Any]):
        super().__init__(module="queue.amqp")
        self.application = application
        self.options = options
        self.connection = None
        self.channel = None

        # Suppress verbose pika logs
        logging.getLogger("pika").setLevel(logging.WARNING)

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push jobs to queue and return job ID(s) for tracking."""
        merged_opts = {**self.options, **options}
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Create job record in database via JobTracker
            db_job_id = self._create_job_record(job, job_id, merged_opts)

            # Prepare payload
            payload = {
                "obj": job,
                "args": merged_opts.get("args", ()),
                "callback": merged_opts.get("callback", "handle"),
                "created": pendulum.now(tz=merged_opts.get("tz", "UTC")),
                "job_id": job_id,
                "db_job_id": db_job_id,
            }

            try:
                self._connect_and_publish(payload, merged_opts)
            except pika.exceptions.AMQPConnectionError:
                # Retry once on connection error
                self._connect_and_publish(payload, merged_opts)

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """Chain jobs: push each job in sequence."""
        if not jobs:
            return

        for job in jobs:
            self.push(job, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """Schedule job for future execution using AMQP delayed plugin."""
        merged_opts = {**self.options, **options}

        # Calculate delay in milliseconds
        delay_ms = int(
            pendulum.parse(str(when)).float_timestamp() * 1000
            - pendulum.now().float_timestamp() * 1000
        )

        # Add delay header for RabbitMQ delayed plugin
        headers = {"x-delay": delay_ms}
        merged_opts["connection_options"] = {
            **merged_opts.get("connection_options", {}),
            **headers,
        }

        self.push(job, options=merged_opts)

    def retry(self, options: Dict[str, Any]) -> None:
        """Retry is not supported in AMQP driver."""
        raise QueueException("AMQP retry is not supported in this implementation.")

    def consume(self, options: Dict[str, Any]) -> None:
        """
        Consume is handled by QueueWorkCommand.

        Use: python craft queue:work
        """
        raise NotImplementedError(
            "AMQPDriver.consume() is not used. Use 'python craft queue:work' command."
        )

    def _create_job_record(self, job, job_id: str, opts: Dict[str, Any]) -> int:
        """Create job record via JobTracker for consistent tracking."""
        try:
            tracker = self._resolve_job_tracker()
            if not tracker:
                return None

            # Get job queue
            queue_name = (
                job.queue
                if hasattr(job, "queue") and job.queue
                else opts.get("queue", "default")
            )

            # Get job class info
            job_name = job.__class__.__name__
            job_class = f"{job.__class__.__module__}.{job.__class__.__name__}"

            # Extract job parameters for payload
            from cara.queues import Bus

            payload = Bus.get_dispatch_params(job)

            # Create job record
            db_job_id = tracker.create_sync_job_record(
                job_name=job_name,
                job_class=job_class,
                queue=queue_name,
                payload=payload,
                metadata={"job_id": job_id, "driver": "amqp"},
            )

            return db_job_id

        except Exception as e:
            Log.warning(f"Failed to create job record: {e}")
            return None

    def _resolve_job_tracker(self):
        """Resolve JobTracker from container."""
        if self.application and self.application.has("JobTracker"):
            return self.application.make("JobTracker")
        return None

    def _connect_and_publish(self, payload: Any, opts: Dict[str, Any]) -> None:
        """Connect to RabbitMQ and publish message."""
        self._connect(opts)

        # Get queue name from job or options
        job = payload.get("obj")
        queue_name = (
            job.queue
            if hasattr(job, "queue") and job.queue
            else opts.get("queue", "default")
        )

        # Declare queue (durable for persistence)
        self.channel.queue_declare(queue=queue_name, durable=True)

        # Serialize payload
        serializer = opts.get("serializer", "pickle")
        if serializer == "json":
            # For JSON: convert job instance to string class path
            if hasattr(job, "__class__"):
                payload_copy = payload.copy()
                payload_copy["obj"] = (
                    f"{job.__class__.__module__}.{job.__class__.__name__}"
                )
                body = json.dumps(payload_copy, default=str).encode("utf-8")
            else:
                body = json.dumps(payload, default=str).encode("utf-8")
        else:
            body = pickle.dumps(payload)

        # Publish with persistence
        self.channel.basic_publish(
            exchange=opts.get("exchange", ""),
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                headers=opts.get("connection_options"),
            ),
        )

        # Wait for confirmation
        if self.channel.is_open:
            self.channel.confirm_delivery()

        # Close connection
        try:
            self.channel.close()
            self.connection.close()
        except Exception:
            pass

    def _connect(self, opts: Dict[str, Any]) -> None:
        """Establish connection to RabbitMQ."""
        try:
            import pika
        except ImportError:
            raise DriverLibraryNotFoundException(
                "pika is required for AMQPDriver. Install with: pip install pika"
            )

        connection_url = self._build_url(opts)
        self.connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        self.channel = self.connection.channel()

        # Enable publisher confirms for reliability
        self.channel.confirm_delivery()

        # Declare durable queue
        self.channel.queue_declare(opts.get("queue"), durable=True)

    def _build_url(self, opts: Dict[str, Any]) -> str:
        """Build AMQP connection URL with proper encoding."""
        from urllib.parse import quote_plus

        connection_params = {
            "username": opts.get("username", ""),
            "password": opts.get("password", ""),
            "host": opts.get("host", "localhost"),
            "port": opts.get("port", 5672),
            "vhost": opts.get("vhost", "/"),
        }

        # URL encode username and password (handles special characters like *, #, %, etc.)
        encoded_username = quote_plus(connection_params["username"])
        encoded_password = quote_plus(connection_params["password"])

        # Encode vhost (/ becomes %2F)
        encoded_vhost = (
            "%2F"
            if not connection_params["vhost"] or connection_params["vhost"] == "/"
            else connection_params["vhost"].replace("/", "%2F")
        )

        base_url = (
            f"amqp://{encoded_username}:{encoded_password}"
            f"@{connection_params['host']}:{connection_params['port']}/{encoded_vhost}"
        )

        # Append connection options if present
        connection_options = opts.get("connection_options")
        if connection_options:
            from urllib.parse import urlencode

            return f"{base_url}?{urlencode(connection_options)}"

        return base_url
