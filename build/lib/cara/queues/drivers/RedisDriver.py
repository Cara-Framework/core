"""
Redis Queue Driver for the Cara framework.

This module implements a queue driver that uses Redis as the backend for job queue management
and processing, with support for delayed jobs and job failures.
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

    - push: pickle the job payload and push to a Redis list.
    - consume: continuously fetch jobs (with blocking BLPOP) and process them.
               Handles delayed jobs via a sorted set.
    - retry: move failed jobs back to the main queue.
    - chain, batch: simple push logic.
    - schedule: if scheduled time > now, add to delayed sorted set; otherwise push immediately.
    """

    driver_name = "redis"

    def __init__(self, application, options: Dict[str, Any]):
        """
        Expected options:
        {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "password": None,            # No AUTH if None or empty
            "queue_prefix": "queue:",    # optional prefix
            "failed_prefix": "failed:",  # optional prefix
            "delayed_prefix": "delayed:",# optional prefix
            "poll_interval": 1,          # seconds to sleep when none
            "blocking_timeout": 5,       # BLPOP timeout in seconds
            "tz": "UTC",                 # timezone for pendulum
        }
        """
        try:
            import redis as _redis
        except ImportError:
            raise DriverLibraryNotFoundException(
                "RedisDriver requires 'redis'. Install with 'pip install redis'."
            )

        self.application = application
        self.options = options or {}

        host = self.options.get("host", "localhost")
        port = int(self.options.get("port", 6379))
        db = int(self.options.get("db", 0))
        password = self.options.get("password") or None

        # Create Redis client; pass password only if provided
        if password:
            self._redis = _redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
            )
        else:
            self._redis = _redis.Redis(host=host, port=port, db=db)

        # Test connection immediately
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
        # Consume loop settings
        self.poll_interval = float(self.options.get("poll_interval", 1))
        self.blocking_timeout = int(self.options.get("blocking_timeout", 5))
        # Timezone for parsing and timestamps
        self.tz = self.options.get("tz", "UTC")

    def _queue_key(self, queue_name: str) -> str:
        return f"{self.queue_prefix}{queue_name}"

    def _failed_key(self, queue_name: str) -> str:
        return f"{self.failed_prefix}{queue_name}"

    def _delayed_key(self, queue_name: str) -> str:
        return f"{self.delayed_prefix}{queue_name}"

    def push(self, *jobs: Any, options: Dict[str, Any]) -> Union[str, List[str]]:
        """
        Push jobs immediately to the Redis list and return job ID(s) for tracking.

        If scheduling/delay is needed, schedule() should be called instead.
        """
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        callback = merged.get("callback", "handle")
        args = merged.get("args", ())
        key = self._queue_key(queue_name)
        job_ids = []

        for job in jobs:
            # Generate unique job ID for tracking
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)

            payload_obj = {
                "obj": job,
                "callback": callback,
                "args": args,
                "job_id": job_id,  # Add job ID for tracking
                "created_at": pendulum.now(
                    tz=merged.get("tz", self.tz)
                ).to_datetime_string(),
            }
            try:
                data = pickle.dumps(payload_obj)
            except Exception as e:
                raise QueueException(f"RedisDriver: could not pickle payload: {e}")
            try:
                # RPUSH to append job to the queue list
                self._redis.rpush(key, data)
            except Exception as e:
                raise QueueException(f"RedisDriver: error pushing to Redis: {e}")

        # Return single job ID if only one job, otherwise return list
        return job_ids[0] if len(job_ids) == 1 else job_ids

    def consume(self, options: Dict[str, Any]) -> None:
        """
        Continuously consume jobs from the queue:
        1) Move due delayed jobs from sorted set to the list.
        2) BLPOP from the list with a timeout; if none, sleep poll_interval.
        3) Process the popped payload.
        """
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        key = self._queue_key(queue_name)
        failed_key = self._failed_key(queue_name)
        delayed_key = self._delayed_key(queue_name)

        self.info(f"RedisDriver: starting consume on queue='{queue_name}'")
        while True:
            try:
                # 1) Check delayed sorted set for due jobs
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
                # 2) Blocking pop from main queue
                popped = self._redis.blpop(key, timeout=self.blocking_timeout)
                if not popped:
                    # Timeout: no job; sleep then continue
                    time.sleep(self.poll_interval)
                    continue
                _, data = popped
                # 3) Process the payload
                self._process_payload(data, queue_name)
            except Exception as e:
                # Log and sleep before retrying
                self.danger(f"RedisDriver.consume encountered error: {e}")
                time.sleep(self.poll_interval)

    def retry(self, options: Dict[str, Any]) -> None:
        """Move all jobs from the failed list back to the main queue."""
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
        """
        Simple chain: push each job in sequence.
        """
        for job in jobs:
            self.push(job, options=options)

    def batch(self, *jobs: Any, options: Dict[str, Any]) -> None:
        """
        Batch push: push all jobs at once.
        """
        self.push(*jobs, options=options)

    def schedule(self, job: Any, when: Any, options: Dict[str, Any]) -> None:
        """
        Schedule a job for a future time:
        - when can be datetime, ISO string, timestamp, or parseable string.
        - If run_time <= now: push immediately; otherwise add to delayed sorted set.
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

    def _process_payload(self, data: bytes, queue_name: str) -> None:
        """
        Unpickle payload, instantiate or use the object, call callback.

        On exception, push the payload to the failed list and call failed().
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

        # Instantiate the job if raw is a class
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

        # Call the callback method
        try:
            method = getattr(instance, callback, None)
            if not callable(method):
                raise AttributeError(
                    f"Callback '{callback}' not found on instance {instance!r}"
                )
            # If handle expects payload, adjust here (e.g., method(msg) or method(*args))
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
                # Call failed() if exists; pass original message and error string
                if hasattr(instance, "failed"):
                    instance.failed(msg, str(e))
            except Exception as inner:
                self.danger(f"RedisDriver: error handling failure: {inner}")
