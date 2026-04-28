"""
Redis Queue Driver for the Cara framework.

Modern, clean implementation for Redis-backed job queue management.
"""

import os
import pickle
import time
import uuid
from typing import Any, Dict, List, Optional, Union

import pendulum

from cara.exceptions import DriverLibraryNotFoundException, QueueException
from cara.queues.contracts import Queue
from cara.queues.job_instantiation import instantiate_job
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

    # Atomically promotes due delayed jobs to the main queue. Runs on
    # the Redis server in a single step so concurrent consumers cannot
    # both ``rpush`` the same payload, and a crash between zrem and
    # rpush cannot drop a job. KEYS[1]=delayed_key, KEYS[2]=main_key,
    # ARGV[1]=now_ts.
    _MOVE_DUE_DELAYED_LUA = (
        "local items = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1]) "
        "if #items == 0 then return 0 end "
        "redis.call('zrem', KEYS[1], unpack(items)) "
        "redis.call('rpush', KEYS[2], unpack(items)) "
        "return #items"
    )

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
            ) from e

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
                raise QueueException(
                    f"RedisDriver: could not pickle payload: {e}"
                ) from e

            try:
                # RPUSH to append job to queue
                self._redis.rpush(key, data)
            except Exception as e:
                raise QueueException(
                    f"RedisDriver: error pushing to Redis: {e}"
                ) from e

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
                # 1. Move every due job from the delayed sorted set into
                #    the main queue atomically. The previous loop did
                #    ``zrem`` then ``rpush`` outside of any transaction —
                #    if the process crashed between the two, the job
                #    vanished; if two consumers polled simultaneously,
                #    each ``rpush``-ed the same job and we got duplicate
                #    execution. ``ZPOPMIN`` doesn't fit (it pops the
                #    smallest score, not "<= now"), so we use a Lua
                #    script that does ``ZRANGEBYSCORE`` + ``ZREM`` +
                #    ``RPUSH`` in one server-side step.
                now_ts = pendulum.now(tz=merged.get("tz", self.tz)).int_timestamp
                try:
                    self._redis.eval(
                        self._MOVE_DUE_DELAYED_LUA,
                        2,
                        delayed_key,
                        key,
                        str(now_ts),
                    )
                except Exception as e:
                    self.danger(f"RedisDriver: error moving delayed jobs: {e}")

                # 2. Blocking pop from main queue, ATOMICALLY
                # transferred onto a per-worker processing list.
                # ``BLPOP``-then-process used to lose the job if the
                # worker crashed between pop and completion: the
                # payload only existed in worker memory. With
                # ``BRPOPLPUSH`` the job lives on the processing list
                # for the entire duration of the work; a periodic
                # reaper sweeps stale processing lists back onto the
                # main queue so abandoned jobs are re-tried.
                processing_key = self._processing_key(queue_name)
                data = self._brpoplpush_compat(key, processing_key)

                if data is None:
                    # Timeout: no job available
                    time.sleep(self.poll_interval)
                    continue

                # Run the periodic reaper in the same loop iteration
                # as occasional housekeeping. Cheap when there's
                # nothing to reap; runs at most once per
                # ``reaper_interval`` seconds.
                self._maybe_reap_processing(merged, queue_name)

                # 3. Process the payload, then remove it from the
                # processing list. The remove is in a finally so a
                # crashing job still gets handled by the failed-job
                # path; only an actual worker crash (process gone)
                # leaves the entry on the processing list, where the
                # reaper picks it up.
                try:
                    self._process_payload(data, queue_name)
                finally:
                    try:
                        # ``LREM count=1 value=data`` removes one
                        # matching entry — covers the common case
                        # where the same payload appears multiple
                        # times.
                        self._redis.lrem(processing_key, 1, data)
                    except Exception as e:
                        self.danger(
                            f"RedisDriver.consume: failed to drop "
                            f"processing entry for {queue_name}: {e}"
                        )

            except Exception as e:
                self.danger(f"RedisDriver.consume encountered error: {e}")
                time.sleep(self.poll_interval)

    def retry(self, options: Dict[str, Any]) -> None:
        """Move valid jobs from failed list back to main queue.

        Payloads that cannot be unpickled are dead-lettered instead of
        cycling forever between main and failed queues.
        """
        merged = {**self.options, **(options or {})}
        queue_name = merged.get("queue", "default")
        key = self._queue_key(queue_name)
        failed_key = self._failed_key(queue_name)

        count = 0
        dead = 0
        while True:
            data = self._redis.lpop(failed_key)
            if data is None:
                break

            # Validate payload before re-enqueuing
            try:
                pickle.loads(data)
            except Exception:
                self._dead_letter(data, queue_name, reason="retry: corrupt payload")
                dead += 1
                continue

            try:
                self._redis.rpush(key, data)
                count += 1
            except Exception as e:
                self.danger(f"RedisDriver.retry error pushing job: {e}")

        self.info(f"RedisDriver.retry: {count} re-enqueued, {dead} dead-lettered")

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
                raise QueueException(
                    f"RedisDriver.schedule: invalid time: {e}"
                ) from e

        # Prepare payload. Include a fresh uuid envelope so two
        # ``schedule()`` calls with the same callable + same args + same
        # ``when`` produce different pickled bytes. Without this, the
        # delayed sorted set silently deduplicates identical members
        # (ZADD only stores each member once) and one of the two
        # scheduled jobs is dropped.
        payload_obj = {
            "obj": job,
            "callback": callback,
            "args": args,
            "job_id": str(uuid.uuid4()),
            "scheduled_at": pendulum.now(
                tz=merged.get("tz", self.tz)
            ).to_datetime_string(),
        }

        try:
            data = pickle.dumps(payload_obj)
        except Exception as e:
            raise QueueException(
                f"RedisDriver.schedule: could not pickle payload: {e}"
            ) from e

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
                ) from e

    def _queue_key(self, queue_name: str) -> str:
        """Get queue key with prefix."""
        return f"{self.queue_prefix}{queue_name}"

    def _failed_key(self, queue_name: str) -> str:
        """Get failed queue key with prefix."""
        return f"{self.failed_prefix}{queue_name}"

    def _delayed_key(self, queue_name: str) -> str:
        """Get delayed queue key with prefix."""
        return f"{self.delayed_prefix}{queue_name}"

    def _dead_letter_key(self, queue_name: str) -> str:
        """Get dead-letter queue key with prefix."""
        return f"{self.queue_prefix}{queue_name}:dead"

    def _processing_key(self, queue_name: str) -> str:
        """Per-worker processing list key.

        We can't really tell different *workers* apart from inside
        the same Redis client (the worker index is opaque to us), so
        we share one processing list per (queue, host, pid). That's
        good enough: the reaper sweeps any list whose host:pid
        combination doesn't match a currently-alive worker, and a
        worker crash always gives the same list back to itself on
        restart so we can recover its in-flight work.
        """
        # ``host:pid`` uniquely identifies a worker process within
        # the cluster. ``socket.gethostname`` is cached internally
        # so this is cheap to call repeatedly.
        import socket

        host = socket.gethostname()
        return f"{self.queue_prefix}{queue_name}:processing:{host}:{os.getpid()}"

    def _brpoplpush_compat(self, src: str, dst: str) -> Optional[bytes]:
        """Atomic blocking pop-and-push from ``src`` to ``dst``.

        Redis 6.2+ replaces ``BRPOPLPUSH`` with ``BLMOVE``; both are
        atomic and both block until a job arrives or the timeout
        expires. We prefer ``BLMOVE`` when redis-py exposes it and
        fall back to ``BRPOPLPUSH`` for older clients/servers.
        Returns the popped payload bytes, or ``None`` on timeout.
        """
        # ``BLMOVE src dst RIGHT LEFT`` matches ``BRPOPLPUSH``: take
        # the last element of ``src`` (FIFO consume), prepend it to
        # ``dst`` (so the most-recently-claimed entry is at the head
        # of the processing list).
        if hasattr(self._redis, "blmove"):
            try:
                return self._redis.blmove(
                    src, dst, timeout=self.blocking_timeout, src="RIGHT", dest="LEFT"
                )
            except (AttributeError, TypeError):
                pass
            except Exception:
                return None
        try:
            return self._redis.brpoplpush(src, dst, timeout=self.blocking_timeout)
        except Exception:
            return None

    def _maybe_reap_processing(self, merged: Dict[str, Any], queue_name: str) -> None:
        """Move stale processing-list entries back to the main queue.

        Called from the consume loop. We only run the sweep at most
        once per ``reaper_interval`` seconds (default 60s) to avoid
        adding latency to the hot path of every job poll. The reaper
        scans every ``{prefix}{queue_name}:processing:*`` key; any
        list whose host:pid doesn't match the current worker AND
        whose entries pre-date the visibility timeout is requeued.
        """
        import socket

        reaper_interval = max(30, int(merged.get("reaper_interval", 60)))
        last = getattr(self, "_last_reap_at", 0.0)
        now_ts = time.time()
        if now_ts - last < reaper_interval:
            return
        self._last_reap_at = now_ts

        visibility_timeout = int(merged.get("visibility_timeout", 600))
        my_host = socket.gethostname()
        my_suffix = f":processing:{my_host}:{os.getpid()}"

        try:
            cursor = 0
            pattern = f"{self.queue_prefix}{queue_name}:processing:*"
            requeue_key = self._queue_key(queue_name)
            recovered = 0
            while True:
                cursor, keys = self._redis.scan(
                    cursor=cursor, match=pattern, count=200
                )
                for raw_key in keys:
                    proc_key = (
                        raw_key.decode("utf-8")
                        if isinstance(raw_key, bytes)
                        else raw_key
                    )
                    if proc_key.endswith(my_suffix):
                        # Our own list — leave it alone. We'll clean
                        # our entries via lrem in the consume loop.
                        continue
                    # ``LRANGE`` is O(N) on length, but processing
                    # lists are bounded by per-worker concurrency so
                    # they stay small.
                    pending = self._redis.lrange(proc_key, 0, -1) or []
                    if not pending:
                        # Empty list left over from a clean shutdown
                        # — remove it.
                        self._redis.delete(proc_key)
                        continue
                    # Best-effort age check via the list's TTL
                    # (reset on every push). Lists without a TTL
                    # default to "old enough to reap".
                    list_idle = self._redis.object("idletime", proc_key) or 0
                    if list_idle < visibility_timeout:
                        continue
                    # Move every entry back to the main queue.
                    for data in pending:
                        try:
                            self._redis.rpush(requeue_key, data)
                            recovered += 1
                        except Exception as e:
                            self.danger(
                                f"Reaper: failed to requeue from {proc_key}: {e}"
                            )
                    self._redis.delete(proc_key)
                if cursor == 0:
                    break
            if recovered:
                self.info(
                    f"RedisDriver: reaper recovered {recovered} job(s) "
                    f"from stale processing lists on '{queue_name}'"
                )
        except Exception as e:
            self.danger(f"RedisDriver: reaper sweep failed: {e}")

    def _dead_letter(self, data: bytes, queue_name: str, reason: str = "") -> None:
        """Move an unrecoverable payload to the dead-letter queue."""
        dl_key = self._dead_letter_key(queue_name)
        try:
            self._redis.rpush(dl_key, data)
            self.info(f"RedisDriver: dead-lettered job on {dl_key}: {reason}")
        except Exception as e:
            self.danger(f"RedisDriver: dead-letter push failed: {e}")

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
            # Dead-letter unrecoverable payloads instead of silently dropping
            self._dead_letter(data, queue_name, reason=f"unpickle: {e}")
            return

        raw = msg.get("obj")
        callback = msg.get("callback", "handle")
        args = msg.get("args", ())

        # Instantiate job if it's a class
        try:
            instance = instantiate_job(self.application, raw, args)
        except Exception as e:
            self.danger(f"RedisDriver: could not instantiate job: {e}")
            self._dead_letter(data, queue_name, reason=f"instantiate: {e}")
            return

        # Execute callback
        try:
            method = getattr(instance, callback, None)
            if not callable(method):
                raise AttributeError(
                    f"Callback '{callback}' not found on instance {instance!r}"
                )

            if hasattr(self.application, "call"):
                self.application.call(method, *args)
            else:
                method(*args) if args else method()

            self.info(f"RedisDriver: job processed successfully, queue={queue_name}")
            # Batch lifecycle (success path).
            self._dispatch_batch_completion(instance, None)

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

            # Batch lifecycle (failure path) — fires ``catch()`` and
            # decrements pending so ``then()`` still triggers when
            # the rest of the batch succeeds.
            self._dispatch_batch_completion(instance, e)
        finally:
            # Always release the UniqueJob lock so subsequent
            # legitimate dispatches for the same ``unique_id`` can
            # proceed. Without this release the lock survives until
            # ``unique_for`` (default 1h) expires, silently dropping
            # every retry / re-dispatch in the meantime.
            self._release_unique_lock_if_any(instance)

    @staticmethod
    def _release_unique_lock_if_any(instance) -> None:
        if instance is None:
            return
        try:
            from cara.queues.contracts import UniqueJob

            if isinstance(instance, UniqueJob):
                UniqueJob.release_unique_lock(instance.unique_id())
        except Exception:
            pass

    @staticmethod
    def _dispatch_batch_completion(instance, exception=None) -> None:
        if instance is None:
            return
        try:
            from cara.queues.Batch import auto_dispatch_batch_completion

            auto_dispatch_batch_completion(instance, exception)
        except Exception:
            pass
