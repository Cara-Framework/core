"""
AMQP Queue Driver for the Cara framework.

This module implements a queue driver for Advanced Message Queuing Protocol (AMQP) systems.
"""

import asyncio
import inspect
import pickle
import uuid
from typing import Any, Dict, List, Union

import pendulum

from cara.exceptions import DriverLibraryNotFoundException, QueueException
from cara.queues.contracts.Queue import Queue
from cara.support.Console import HasColoredOutput


class AMQPDriver(HasColoredOutput, Queue):
    """
    AMQP-based queue driver.

    Publishes and consumes messages from RabbitMQ, and on consume, instantiates job instances and
    calls their handle() method.
    """

    driver_name = "amqp"

    def __init__(self, application, options: Dict[str, Any]):
        super().__init__(module="queue.amqp")
        self.application = application
        self.options = options
        self.connection = None
        self.channel = None
        self.pika = None

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push jobs to queue and return job ID(s) for tracking."""
        merged_opts = {**self.options, **options}
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Create job record in database (like DatabaseDriver)
            db_job_id = self._create_job_record(job, job_id, merged_opts)

            # We store the object itself (instance or class) and any init args if provided in options
            payload = {
                # job may be an instance or a class; we'll handle in consume
                "obj": job,
                # if options supply 'args', use them when instantiating a class
                "args": merged_opts.get("args", ()),
                # callback method name, default "handle"
                "callback": merged_opts.get("callback", "handle"),
                "created": pendulum.now(tz=merged_opts.get("tz", "UTC")),
                # Add job ID for tracking
                "job_id": job_id,
                # Add database job ID for JobTracker
                "db_job_id": db_job_id,
            }
            try:
                self._connect_and_publish(payload, merged_opts)
            except Exception as e:
                # Check if it's a connection exception after pika is imported
                if self.pika and self._is_connection_exception(e):
                    # retry once
                    self._connect_and_publish(payload, merged_opts)
                else:
                    raise

        # Return single job ID if only one job, otherwise return list
        return job_ids[0] if len(job_ids) == 1 else job_ids

    def _create_job_record(self, job, job_id: str, opts: Dict[str, Any]) -> int:
        """Create job record in database for tracking (like DatabaseDriver)."""
        try:
            import pendulum

            from commons.models.core import Job

            # Get job queue
            if hasattr(job, 'queue') and job.queue:
                queue_name = job.queue
            else:
                queue_name = opts.get("queue", "default")
            
            # Create job record
            job_record = Job.create({
                'name': str(job),
                'payload': {'job_id': job_id, 'driver': 'amqp'},
                'queue': queue_name,
                'available_at': pendulum.now(),
                'status': Job.STATUS_PENDING,
                'job_class': job.__class__.__name__,
                'metadata': {'job_id': job_id, 'driver': 'amqp'}
            })
            
            return job_record.id
            
        except Exception as e:
            # Log error but don't fail job dispatch
            print(f"⚠️ Failed to create job record: {e}")
            return None

    def consume(self, options: Dict[str, Any]) -> None:
        merged_opts = {**self.options, **options}
        queue_name = merged_opts.get("queue")
        self.info(
            f'[*] Waiting to process jobs on queue "{queue_name}". To exit press CTRL+C'
        )
        self._connect(merged_opts)
        
        # Declare queue (durable=True for persistence)
        self.channel.queue_declare(queue=queue_name, durable=True)
        
        # prefetch_count=1 for fair dispatch
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue_name, self._work_callback)
        try:
            self.channel.start_consuming()
        finally:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass
            try:
                self.channel.close()
                self.connection.close()
            except Exception:
                pass

    def retry(self, options: Dict[str, Any]) -> None:
        raise QueueException("AMQP retry is not supported in this implementation.")

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """
        Simple chain: push first job with a callback that pushes the rest.
        """

        def make_chain_callback(rest):
            def _callback(*args):
                if rest:
                    # push next job in chain
                    self.push(rest[0], options=options)
                    make_chain_callback(rest[1:])()

            return _callback

        if not jobs:
            return
        first, *rest = jobs
        chain_opts = {**options, "callback": "handle"}
        # override first.handle so that after it runs, next is enqueued
        setattr(first, "handle", make_chain_callback(rest))
        self.push(first, options=chain_opts)

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """
        Batch: push all jobs at once.
        """
        self.push(*jobs, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """Schedule by calculating delay and using AMQP delayed plugin headers."""
        merged_opts = {**self.options, **options}
        # calculate delay in ms
        delay_ms = int(
            pendulum.parse(str(when)).float_timestamp() * 1000
            - pendulum.now().float_timestamp() * 1000
        )
        headers = {"x-delay": delay_ms}
        merged_opts["connection_options"] = {
            **merged_opts.get("connection_options", {}),
            **headers,
        }
        self.push(job, options=merged_opts)

    def _is_connection_exception(self, exception: Exception) -> bool:
        """Check if exception is a connection-related exception."""
        if not self.pika:
            return False

        connection_exceptions = (
            self.pika.exceptions.ConnectionClosed,
            self.pika.exceptions.ChannelClosed,
            self.pika.exceptions.ConnectionWrongStateError,
            self.pika.exceptions.ChannelWrongStateError,
        )
        return isinstance(exception, connection_exceptions)

    def _connect_and_publish(self, payload: Any, opts: Dict[str, Any]) -> None:
        self._connect(opts)
        
        # Use job's queue if available, otherwise use config queue
        job = payload.get("obj")
        if hasattr(job, 'queue') and job.queue:
            queue_name = job.queue
        else:
            queue_name = opts.get("queue", "default")
            
        # Declare queue (durable=True for persistence)
        self.channel.queue_declare(queue=queue_name, durable=True)
            
        # publish the pickled payload with persistence
        self.channel.basic_publish(
            exchange=opts.get("exchange", ""),
            routing_key=queue_name,
            body=pickle.dumps(payload),
            properties=self.pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                headers=opts.get("connection_options"),
            )
        )
        
        # Wait for confirmation
        if self.channel.is_open:
            self.channel.confirm_delivery()
        
        # Close connection after confirmation
        try:
            self.channel.close()
            self.connection.close()
        except Exception:
            pass

    def _connect(self, opts: Dict[str, Any]) -> None:
        try:
            import logging

            import pika

            self.pika = pika  # Store pika module reference

            # Suppress verbose pika logs
            logging.getLogger("pika").setLevel(logging.WARNING)
        except ImportError:
            raise DriverLibraryNotFoundException(
                "pika is required for AMQPDriver. "
                "Please install it with: pip install pika"
            )
        connection_url = self._build_url(opts)
        self.connection = pika.BlockingConnection(pika.URLParameters(connection_url))
        self.channel = self.connection.channel()
        
        # Enable publisher confirms for reliability
        self.channel.confirm_delivery()
        
        # declare durable queue
        self.channel.queue_declare(opts.get("queue"), durable=True)

    def _build_url(self, opts: Dict[str, Any]) -> str:
        """Build AMQP connection URL with proper encoding."""
        
        connection_params = self._extract_connection_params(opts)
        encoded_vhost = self._encode_vhost(connection_params['vhost'])
        
        base_url = (
            f"amqp://{connection_params['username']}:{connection_params['password']}"
            f"@{connection_params['host']}:{connection_params['port']}/{encoded_vhost}"
        )
        
        return self._append_connection_options(base_url, opts.get("connection_options"))
    
    def _extract_connection_params(self, opts: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and validate connection parameters."""
        return {
            'username': opts.get("username", ""),
            'password': opts.get("password", ""),
            'host': opts.get("host", "localhost"),
            'port': opts.get("port", 5672),
            'vhost': opts.get("vhost", "/")
        }
    
    def _encode_vhost(self, vhost: str) -> str:
        """Encode vhost for URL (/ becomes %2F)."""
        if not vhost or vhost == "/":
            return "%2F"
        return vhost.replace("/", "%2F")
    
    def _append_connection_options(self, base_url: str, options: Dict[str, Any]) -> str:
        """Append connection options to URL if present."""
        if not options:
            return base_url
            
        from urllib.parse import urlencode
        return f"{base_url}?{urlencode(options)}"

    def _work_callback(self, ch, method, properties, body):
        """
        Called when a message is received.

        Unpickle payload dict, instantiate or use instance, call the callback (usually 'handle'). On
        exception, call failed() if present.
        """
        try:
            msg = pickle.loads(body)
        except (pickle.UnpicklingError, RecursionError, Exception) as e:
            # invalid message or circular reference; ack and skip
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                pass

            error_type = type(e).__name__
            if "recursion" in str(e).lower() or isinstance(e, RecursionError):
                self.danger(
                    f"AMQPDriver: skipping corrupted message with circular reference: {e}"
                )
            else:
                self.danger(f"AMQPDriver: failed to unpickle message ({error_type}): {e}")
            return

        raw = msg.get("obj")
        init_args = msg.get("args", ())
        callback = msg.get("callback", "handle")
        job_id = msg.get("job_id", "unknown")

        # Determine instance:
        if inspect.isclass(raw):
            # if no init_args and container available, resolve from container; else instantiate
            if hasattr(self.application, "make") and not init_args:
                try:
                    instance = self.application.make(raw)
                except Exception:
                    instance = raw(*init_args)
            else:
                instance = raw(*init_args)
        else:
            instance = raw

        try:
            # Set up job tracking if instance supports it
            if hasattr(instance, "set_tracking_id"):
                instance.set_tracking_id(job_id)

            # Set database job ID for JobTracker
            if hasattr(instance, '_db_job_id') or hasattr(instance, '__dict__'):
                db_job_id = msg.get("db_job_id")
                if db_job_id:
                    instance._db_job_id = db_job_id

            # Call the callback method on instance
            method_to_call = getattr(instance, callback, None)
            if not callable(method_to_call):
                raise AttributeError(f"Callback '{callback}' not found on {instance!r}")

            # Check if the method is a coroutine and run it accordingly
            if inspect.iscoroutinefunction(method_to_call):
                # Run the async method in a new event loop
                asyncio.run(method_to_call(*init_args))
            else:
                # Execute the sync method directly
                method_to_call(*init_args)

            # Call completion handler if available
            if hasattr(instance, "on_job_complete"):
                instance.on_job_complete()

            self._log_success(method.delivery_tag, job_id)
        except Exception as e:
            # Job failed
            self._log_failure(method.delivery_tag, job_id)
            try:
                if hasattr(instance, "failed"):
                    failed_method = getattr(instance, "failed")
                    if inspect.iscoroutinefunction(failed_method):
                        # Run async failed method
                        asyncio.run(failed_method(msg, str(e)))
                    else:
                        # Run sync failed method
                        failed_method(msg, str(e))
            except Exception as inner:
                self.danger(f"Exception in failed(): {inner}")

            # Re-raise the exception to ensure proper error handling
            raise
        finally:
            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                pass

    def _log_success(self, tag: int, job_id: str = "unknown") -> None:
        self.success(
            f"[{tag}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Successfully Processed (ID: {job_id})"
        )

    def _log_failure(self, tag: int, job_id: str = "unknown") -> None:
        self.danger(
            f"[{tag}][{pendulum.now(tz=self.options.get('tz', 'UTC')).to_datetime_string()}] Job Failed (ID: {job_id})"
        )
