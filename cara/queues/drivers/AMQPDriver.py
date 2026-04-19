"""
AMQP Queue Driver for the Cara framework.

Modern, clean implementation for RabbitMQ-based job queue management.
"""

import json
import logging
import pickle
import uuid
from typing import Any, Dict, List, Optional, Union

import pendulum
import pika

from cara.exceptions import DriverLibraryNotFoundException
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

    def later(self, delay: Union[int, "pendulum.Duration"], job: Any, options: Dict[str, Any] = None) -> Union[str, List[str]]:
        """
        Schedule a job to be executed after a delay.

        Uses AMQP message TTL and delayed plugin for scheduling.
        Implements exponential backoff for retries.

        Args:
            delay: Delay in seconds or pendulum Duration
            job: Job instance to schedule
            options: Queue options

        Returns:
            Job ID(s)
        """
        import pendulum as pendulum_module

        if options is None:
            options = {}

        # Handle delay as Duration or int
        if isinstance(delay, int):
            delay_seconds = delay
        else:
            # Assume it's a Duration-like object
            delay_seconds = int(delay.total_seconds()) if hasattr(delay, "total_seconds") else delay

        # Merge options
        merged_opts = {**self.options, **options}

        # Calculate when job should run
        when = pendulum_module.now(tz=merged_opts.get("tz", "UTC")).add(seconds=delay_seconds)

        # Use schedule method which handles AMQP delayed plugin
        self.schedule(job, when, merged_opts)

        # Generate and return job ID
        job_id = str(uuid.uuid4())
        return job_id

    def retry(
        self,
        job: Any,
        options: Dict[str, Any] = None,
        attempts: int = 3,
        backoff: str = "exponential"
    ) -> Optional[Union[str, List[str]]]:
        """
        Retry a failed job with optional exponential backoff.

        Implements exponential backoff retry strategy similar to Laravel.
        Uses AMQP dead letter exchanges and TTL for delayed retries.

        Args:
            job: Job instance to retry
            options: Queue options
            attempts: Maximum retry attempts (default: 3)
            backoff: Backoff strategy - 'exponential', 'linear', or int for fixed delay in seconds

        Returns:
            Job ID

        Example:
            driver.retry(my_job, attempts=5, backoff='exponential')
        """
        if options is None:
            options = {}

        # Get current attempt count
        attempts_made = options.get("attempts", 0)

        if attempts_made >= attempts:
            # Max attempts reached, send to dead letter
            Log.error(
                f"Job {job.__class__.__name__} exceeded max retry attempts ({attempts}). "
                f"Sending to dead letter queue."
            )
            self._send_to_dead_letter(job, options, attempts_made)
            return None

        # Calculate delay based on backoff strategy
        if isinstance(backoff, str) and backoff == "exponential":
            # Exponential backoff: 1s, 2s, 4s, 8s, etc.
            delay_seconds = 2 ** attempts_made
        elif isinstance(backoff, str) and backoff == "linear":
            # Linear backoff: 1s, 2s, 3s, 4s, etc.
            delay_seconds = (attempts_made + 1) * 60  # 1 minute per attempt
        else:
            # Fixed delay (assume it's an int)
            delay_seconds = int(backoff) if backoff else 300  # Default 5 minutes

        attempts_made += 1
        Log.info(
            f"Retrying job {job.__class__.__name__} (attempt {attempts_made}/{attempts}) "
            f"with {delay_seconds}s delay"
        )

        # Update options with retry info
        retry_opts = {
            **options,
            "attempts": attempts_made,
            "retry_backoff": backoff,
            "max_attempts": attempts,
        }

        # Schedule retry with delay
        return self.later(delay_seconds, job, retry_opts)

    def _send_to_dead_letter(
        self, job: Any, options: Dict[str, Any], attempts: int
    ) -> None:
        """
        Send a permanently failed job to dead letter queue.

        Args:
            job: Failed job instance
            options: Queue options
            attempts: Number of attempts made
        """
        try:
            self._connect({})

            # Prepare dead letter message
            payload = {
                "obj": job,
                "args": options.get("args", ()),
                "callback": options.get("callback", "handle"),
                "failed_at": pendulum.now(tz=options.get("tz", "UTC")).to_datetime_string(),
                "attempts": attempts,
                "error": options.get("error"),
            }

            # Serialize
            serializer = options.get("serializer", "pickle")
            if serializer == "json":
                body = json.dumps(payload, default=str).encode("utf-8")
            else:
                body = pickle.dumps(payload)

            # Publish to dead letter exchange
            dlx_name = options.get("exchange", "default") + ".dlx"
            dlq_routing_key = f"dead.{options.get('queue', 'default')}"

            self.channel.basic_publish(
                exchange=dlx_name,
                routing_key=dlq_routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    headers={
                        "x-death-count": attempts,
                        "x-original-job": job.__class__.__name__,
                    },
                ),
            )

            Log.info(f"Job sent to dead letter queue: {dlq_routing_key}")

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to send job to dead letter queue: {e}")

    def consume(self, options: Dict[str, Any]) -> None:
        """
        Consume is handled by QueueWorkCommand.

        Use: python craft queue:work
        """
        raise NotImplementedError(
            "AMQPDriver.consume() is not used. Use 'python craft queue:work' command."
        )

    def _create_job_record(self, job, job_id: str, opts: Dict[str, Any]) -> Optional[int]:
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

    def declare_dead_letter_exchange(self, exchange_name: str = "dead.letter") -> None:
        """
        Declare RabbitMQ dead letter exchange and queues.

        Creates DLX exchange and binds dead letter queues for failed jobs.

        Args:
            exchange_name: Base exchange name (default: "dead.letter")
        """
        try:
            self._connect({})

            # Declare dead letter exchange
            dlx_name = f"{exchange_name}.dlx"
            self.channel.exchange_declare(
                exchange=dlx_name,
                exchange_type="topic",
                durable=True
            )

            # Declare dead letter queue
            dlq_name = f"{exchange_name}.queue"
            self.channel.queue_declare(
                queue=dlq_name,
                durable=True,
                arguments={
                    "x-message-ttl": 86400000,  # 24 hours
                }
            )

            # Bind queue to DLX
            self.channel.queue_bind(
                exchange=dlx_name,
                queue=dlq_name,
                routing_key="dead.*"
            )

            Log.info(f"Dead letter exchange configured: {dlx_name}")

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to declare dead letter exchange: {e}")

    def get_dead_letter_messages(
        self, queue_name: str = "dead.letter.queue", limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Peek at dead letter queue messages without consuming them.

        Args:
            queue_name: Dead letter queue name
            limit: Maximum messages to retrieve

        Returns:
            List of message details (headers, body, routing_key)
        """
        messages = []
        try:
            self._connect({})

            # Use basic_get to peek at messages without consuming
            for _ in range(limit):
                method, properties, body = self.channel.basic_get(queue_name, auto_ack=False)

                if method is None:
                    break

                # Decode payload
                try:
                    payload = pickle.loads(body)
                except Exception:
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception:
                        payload = {"raw": body.decode("utf-8", errors="ignore")}

                messages.append({
                    "delivery_tag": method.delivery_tag,
                    "routing_key": method.routing_key,
                    "redelivered": method.redelivered,
                    "exchange": method.exchange,
                    "headers": dict(properties.headers or {}),
                    "timestamp": properties.timestamp,
                    "payload": payload,
                })

                # Don't consume - requeue the message
                self.channel.basic_nack(method.delivery_tag, requeue=True)

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to get dead letter messages: {e}")

        return messages

    def replay_dead_letter(
        self, queue_name: str, message_id: Optional[str] = None
    ) -> int:
        """
        Replay dead letter messages back to original queue.

        Args:
            queue_name: Original queue to replay to
            message_id: Specific message ID to replay, or None for all

        Returns:
            Number of messages replayed
        """
        dlq_name = "dead.letter.queue"
        replayed = 0

        try:
            self._connect({})

            while True:
                method, properties, body = self.channel.basic_get(dlq_name, auto_ack=False)

                if method is None:
                    break

                # Check if this is the message to replay
                headers = dict(properties.headers or {})
                msg_id = headers.get("message_id")

                if message_id is None or msg_id == message_id:
                    # Republish to original queue
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=queue_name,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            headers=headers,
                        ),
                    )
                    replayed += 1
                    Log.info(f"Replayed message {msg_id} to {queue_name}")

                    if message_id is not None:
                        break

                # Remove from dead letter queue
                self.channel.basic_ack(method.delivery_tag)

            # Close connection
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

        except Exception as e:
            Log.error(f"Failed to replay dead letter messages: {e}")

        return replayed

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

        # Declare queue with dead letter exchange support
        exchange_name = opts.get("exchange", "")
        message_ttl = opts.get("message_ttl", 86400000)  # 24h default

        queue_args = {
            "x-dead-letter-exchange": f"{exchange_name}.dlx" if exchange_name else "dead.letter.dlx",
            "x-dead-letter-routing-key": f"dead.{queue_name}",
            "x-message-ttl": message_ttl,
        }

        # Idempotent declare: if the queue was originally created with
        # different args (e.g. older codebase or manual setup left it
        # without x-message-ttl), an active declare raises
        # PRECONDITION_FAILED (406) and kills the channel. We swallow
        # that once, reopen the channel, and fall back to passive
        # declare — the queue already exists so we can publish anyway.
        # This prevents TTL-drift from wedging every dispatcher forever.
        try:
            self.channel.queue_declare(
                queue=queue_name,
                durable=True,
                arguments=queue_args,
            )
        except pika.exceptions.ChannelClosedByBroker as exc:
            if getattr(exc, "reply_code", None) != 406:
                raise
            Log.warning(
                f"⚠️  Queue '{queue_name}' exists with different args "
                f"(reply={exc.reply_text}); falling back to passive declare"
            )
            # Channel is dead after 406 — reopen on the same connection.
            try:
                self.channel = self.connection.channel()
                self.channel.confirm_delivery()
            except Exception:
                self._connect(opts)
            self.channel.queue_declare(
                queue=queue_name, durable=True, passive=True
            )

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

        # NOTE: Queue declaration is intentionally NOT done here.
        # Each caller (_connect_and_publish, setup_dead_letter_exchange, etc.)
        # declares its target queue with the correct arguments (x-message-ttl,
        # x-dead-letter-exchange, ...). Declaring here without arguments
        # conflicted with existing queues and caused PRECONDITION_FAILED
        # (inequivalent arg 'x-message-ttl') on reconnects.

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
