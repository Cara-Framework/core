"""
Redis Queue Driver for the Cara framework.

Modern, clean implementation for Redis-backed job queue management.
"""

import inspect
import pickle
import time
import uuid
from typing import Any, Dict, List, Union

import pendulum

from cara.exceptions import DriverLibraryNotFoundException, QueueException
from cara.queues.contracts import Queue
from cara.support.Console import HasColoredOutput


class RedisDriver(HasColoredOutput, Queue):
    """
    Redis-based queue driver.

    Features:
    - Fast, in-memory job queuing
    - Delayed job execution with sorted sets
    - Blocking pop for efficient polling
    - Failed job handling with retry support
    """

    driver_name = "redis"

    def __init__(self, application, options: Dict[str, Any]):
        """
        Initialize Redis driver.

        Options:
            host: Redis host (default: localhost)
            port: Redis port (default: 6379)
            db: Redis database (default: 0)
            password: Redis password (optional)
            queue_prefix: Prefix for queue keys (default: queue:)
            failed_prefix: Prefix for failed queue keys (default: failed:)
            delayed_prefix: Prefix for delayed queue keys (default: delayed:)
            poll_interval: Sleep seconds when no jobs (default: 1)
            blocking_timeout: BLPOP timeout in seconds (default: 5)
            tz: Timezone for timestamps (default: UTC)
        """
        try:
            import redis as _redis
        except ImportError:
            raise DriverLibraryNotFoundException(
                "RedisDriver requires 'redis'. Install with: pip install redis"
            )

        self.application = application
        self.options = options or {}

        # Connection parameters
        host = self.options.get("host", "localhost")
        port = int(self.options.get("port", 6379))
        db = int(self.options.get("db", 0))
        password = self.options.get("password") or None

        # Create Redis client
        if password:
            self._redis = _redis.Redis(host=host, port=port, db=db, password=password)
        else:
            self._redis = _redis.Redis(host=host, port=port, db=db)

        # Test connection
        try:
            self._redis.ping()
        except Exception as e:
            raise DriverLibraryNotFoundException(
                f"Cannot connect to Redis at {host}:{port}: {e}"
            )

        # Prefix settings
        self.queue_prefix = self.options.get("queue_prefix", "queue:")
        self.failed_prefix = self.options.get("failed_prefix", "failed:")
        self.delayed_prefix = self.options.get("delayed_prefix", "delayed:")

        # Consume settings
        self.poll_interval = float(self.options.get("poll_interval", 1))
        self.blocking_timeout = int(self.options.get("blocking_timeout", 5))
        self.tz = self.options.get("tz", "UTC")

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """Push jobs immediately to Redis list and return job ID(s)."""
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        callback = merged.get("callback", "handle")
        args = merged.get("args", ())
        key = self._queue_key(queue_name)
        job_ids = []

        for job in jobs:
            # Generate unique job ID
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            # Prepare payload
            payload_obj = {
                "obj": job,
                "callback": callback,
                "args": args,
                "job_id": job_id,
                "created_at": pendulum.now(
                    tz=merged.get("tz", self.tz)
                ).to_datetime_string(),
            }

            try:
                data = pickle.dumps(payload_obj)
            except Exception as e:
                raise QueueException(f"RedisDriver: could not pickle payload: {e}")

            try:
                # RPUSH to append job to queue
                self._redis.rpush(key, data)
            except Exception as e:
                raise QueueException(f"RedisDriver: error pushing to Redis: {e}")

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def consume(self, options: Dict[str, Any]) -> None:
        """
        Continuously consume jobs from Redis queue.

        Process:
        1. Move due delayed jobs from sorted set to list
        2. BLPOP from list with timeout
        3. Process the popped payload
        """
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        key = self._queue_key(queue_name)
        delayed_key = self._delayed_key(queue_name)

        self.info(f"RedisDriver: starting consume on queue='{queue_name}'")

        while True:
            try:
                # 1. Check delayed sorted set for due jobs
                now_ts = pendulum.now(tz=merged.get("tz", self.tz)).int_timestamp
                due_items = self._redis.zrangebyscore(delayed_key, 0, now_ts)

                if due_items:
                    for item in due_items:
                        try:
                            # Remove from delayed set, push to main queue
                            self._redis.zrem(delayed_key, item)
                            self._redis.rpush(key, item)
                        except Exception as e:
                            self.danger(f"RedisDriver: error moving delayed job: {e}")

                # 2. Blocking pop from main queue
                popped = self._redis.blpop(key, timeout=self.blocking_timeout)

                if not popped:
                    # Timeout: no job available
                    time.sleep(self.poll_interval)
                    continue

                _, data = popped

                # 3. Process the payload
                self._process_payload(data, queue_name)

            except Exception as e:
                self.danger(f"RedisDriver.consume encountered error: {e}")
                time.sleep(self.poll_interval)

    def retry(self, options: Dict[str, Any]) -> None:
        """Move all jobs from failed list back to main queue."""
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        key = self._queue_key(queue_name)
        failed_key = self._failed_key(queue_name)

        count = 0
        while True:
            data = self._redis.lpop(failed_key)
            if data is None:
                break

            try:
                self._redis.rpush(key, data)
                count += 1
            except Exception as e:
                self.danger(f"RedisDriver.retry error pushing job: {e}")

        self.info(f"RedisDriver.retry: {count} jobs re-enqueued")

    def chain(self, jobs: list, options: Dict[str, Any]) -> None:
        """Chain jobs: push each job in sequence."""
        for job in jobs:
            self.push(job, options=options)

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """
        Schedule job for future execution.

        - If run_time <= now: push immediately
        - Otherwise: add to delayed sorted set
        """
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        callback = merged.get("callback", "handle")
        args = merged.get("args", ())
        delayed_key = self._delayed_key(queue_name)
        now = pendulum.now(tz=merged.get("tz", self.tz))

        # Parse run time to timestamp
        if isinstance(when, (int, float)):
            run_ts = int(when)
        else:
            try:
                run_dt = pendulum.parse(str(when))
                run_ts = run_dt.int_timestamp
            except Exception as e:
                raise QueueException(f"RedisDriver.schedule: invalid time: {e}")

        # Prepare payload
        payload_obj = {
            "obj": job,
            "callback": callback,
            "args": args,
            "scheduled_at": pendulum.now(
                tz=merged.get("tz", self.tz)
            ).to_datetime_string(),
        }

        try:
            data = pickle.dumps(payload_obj)
        except Exception as e:
            raise QueueException(f"RedisDriver.schedule: could not pickle payload: {e}")

        if run_ts <= now.int_timestamp:
            # Time is now or past: push immediately
            self._redis.rpush(self._queue_key(queue_name), data)
        else:
            # Add to delayed sorted set with score = run_ts
            try:
                self._redis.zadd(delayed_key, {data: run_ts})
            except Exception as e:
                raise QueueException(
                    f"RedisDriver.schedule: error adding to delayed set: {e}"
                )

    def _queue_key(self, queue_name: str) -> str:
        """Get queue key with prefix."""
        return f"{self.queue_prefix}{queue_name}"

    def _failed_key(self, queue_name: str) -> str:
        """Get failed queue key with prefix."""
        return f"{self.failed_prefix}{queue_name}"

    def _delayed_key(self, queue_name: str) -> str:
        """Get delayed queue key with prefix."""
        return f"{self.delayed_prefix}{queue_name}"

    def _process_payload(self, data: bytes, queue_name: str) -> None:
        """
        Unpickle payload, instantiate job, and execute callback.

        On exception, push to failed list and call failed() if available.
        """
        failed_key = self._failed_key(queue_name)

        try:
            msg = pickle.loads(data)
        except Exception as e:
            self.danger(f"RedisDriver: could not unpickle payload: {e}")
            return

        raw = msg.get("obj")
        callback = msg.get("callback", "handle")
        args = msg.get("args", ())

        # Instantiate job if it's a class
        try:
            if inspect.isclass(raw):
                if hasattr(self.application, "make") and not args:
                    try:
                        instance = self.application.make(raw)
                    except Exception:
                        instance = raw(*args)
                else:
                    instance = raw(*args)
            else:
                instance = raw
        except Exception as e:
            self.danger(f"RedisDriver: could not instantiate job: {e}")
            return

        # Execute callback
        try:
            method = getattr(instance, callback, None)
            if not callable(method):
                raise AttributeError(
                    f"Callback '{callback}' not found on instance {instance!r}"
                )

            if args:
                method(*args)
            else:
                method()

            self.info(f"RedisDriver: job processed successfully, queue={queue_name}")

        except Exception as e:
            self.danger(f"RedisDriver: job processing failed: {e}")

            try:
                # Push to failed list
                self._redis.rpush(failed_key, data)

                # Call failed() if exists
                if hasattr(instance, "failed"):
                    instance.failed(msg, str(e))

            except Exception as inner:
                self.danger(f"RedisDriver: error handling failure: {inner}")
