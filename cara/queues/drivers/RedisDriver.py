"""
Redis Queue Driver for the Cara framework.

Modern, clean implementation for Redis-backed job queue management.
"""

import os
import pickle
import time
import uuid
from typing import Any

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

    # Atomically claims a stale processing list and moves every entry
    # back to the main queue. KEYS[1]=stale processing list,
    # KEYS[2]=main queue. Returns the number of entries requeued.
    # Required because the reaper runs concurrently from every healthy
    # worker; without this script, two reapers can both LRANGE the
    # same stale list and both RPUSH each entry before either DELs it
    # — every dead worker's in-flight jobs get duplicated once per
    # healthy reaper. Lua scripts run atomically against the keyspace
    # (single-threaded), so the second concurrent caller sees an
    # empty list and is a no-op.
    _REAP_PROCESSING_LUA = (
        "local items = redis.call('lrange', KEYS[1], 0, -1) "
        "if #items == 0 then "
        "  redis.call('del', KEYS[1]) "
        "  return 0 "
        "end "
        "redis.call('rpush', KEYS[2], unpack(items)) "
        "redis.call('del', KEYS[1]) "
        "return #items"
    )

    def __init__(self, application, options: dict[str, Any]):
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

        # Pool hardening — kept in lockstep with ``RedisCacheDriver``.
        # Pre-fix the queue driver constructed ``redis.Redis(host, port,
        # db[, password])`` with NOTHING else, so a single Redis blip
        # would stall every queue worker until the OS TCP timeout
        # fired (potentially minutes), stale sockets after a Redis
        # restart got handed out until the first command failed, and
        # the pool was free to grow to redis-py's default 2^31-1 cap —
        # exhausting file descriptors on the worker host. Pull
        # overrides from ``options`` so the queue config can tune
        # them per-driver without touching every framework caller.
        socket_connect_timeout = float(
            self.options.get("socket_connect_timeout", 5.0)
        )
        socket_timeout = float(self.options.get("socket_timeout", 5.0))
        health_check_interval = int(
            self.options.get("health_check_interval", 30)
        )
        max_connections = int(self.options.get("max_connections", 32))

        # Drop ``password`` from the kwargs entirely when not set —
        # redis-py treats explicit ``None`` differently from "omitted"
        # in some auth-aware paths.
        client_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "db": db,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_timeout": socket_timeout,
            "socket_keepalive": True,
            "health_check_interval": health_check_interval,
            "max_connections": max_connections,
            "retry_on_timeout": True,
        }
        if password:
            client_kwargs["password"] = password
        self._redis = _redis.Redis(**client_kwargs)

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

    def push(self, *jobs: Any, options: dict[str, Any]) -> str | list[str]:
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
                raise QueueException(f"RedisDriver: could not pickle payload: {e}") from e

            try:
                # RPUSH to append job to queue
                self._redis.rpush(key, data)
            except Exception as e:
                raise QueueException(f"RedisDriver: error pushing to Redis: {e}") from e

        return job_ids[0] if len(job_ids) == 1 else job_ids

    def consume(self, options: dict[str, Any]) -> None:
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

    def retry(self, options: dict[str, Any]) -> None:
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

    def chain(self, jobs: list, options: dict[str, Any]) -> None:
        """Chain jobs: push each job in sequence."""
        for job in jobs:
            self.push(job, options=options)

    def batch(self, *jobs: Any, options: dict[str, Any]) -> None:
        """Batch push: push all jobs at once."""
        self.push(*jobs, options=options)

    def later(self, delay: Any, job: Any, options: dict[str, Any]) -> None:
        """Schedule ``job`` to run ``delay`` seconds from now.

        Laravel-compatible delay entry point. Pre-fix RedisDriver had
        no ``later`` method, so ``Queue.later(5, job)`` fell through
        to ``schedule(job, 5)`` which treated ``5`` as a unix
        timestamp (1970-01-01) — ``run_ts <= now`` so the job was
        published immediately, defeating every retry-with-backoff
        caller on the Redis driver.
        """
        if hasattr(delay, "total_seconds"):
            delay_seconds = float(delay.total_seconds())
        else:
            try:
                delay_seconds = float(delay)
            except (TypeError, ValueError) as e:
                raise QueueException(
                    f"RedisDriver.later: invalid delay {delay!r}"
                ) from e
        merged = {**self.options, **(options or {})}
        now = pendulum.now(tz=merged.get("tz", self.tz))
        when = now.add(seconds=max(delay_seconds, 0.0))
        return self.schedule(job, when, options or {})

    def schedule(self, job: Any, when: Any, options: dict[str, Any]) -> None:
        """
        Schedule job for future execution.

        ``when`` accepts: a ``pendulum.DateTime``, a parseable string,
        or a numeric unix timestamp. Numeric values are interpreted
        as absolute timestamps (kept for backwards compatibility). For
        "N seconds from now" semantics use :meth:`later` — that path
        is what every retry-with-backoff caller goes through.

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
        if hasattr(when, "int_timestamp"):
            # pendulum DateTime (handed to us by ``later`` or a caller
            # that already built the absolute moment) — read the int
            # timestamp directly so we don't pay a round-trip through
            # ``str()`` + ``pendulum.parse()``.
            run_ts = when.int_timestamp
        elif isinstance(when, (int, float)):
            run_ts = int(when)
        else:
            try:
                run_dt = pendulum.parse(str(when))
                run_ts = run_dt.int_timestamp
            except Exception as e:
                raise QueueException(f"RedisDriver.schedule: invalid time: {e}") from e

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

    def _brpoplpush_compat(self, src: str, dst: str) -> bytes | None:
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

    def _maybe_reap_processing(self, merged: dict[str, Any], queue_name: str) -> None:
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
                cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=200)
                for raw_key in keys:
                    proc_key = (
                        raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
                    )
                    if proc_key.endswith(my_suffix):
                        # Our own list — leave it alone. We'll clean
                        # our entries via lrem in the consume loop.
                        continue
                    # Best-effort age check via the list's idle time
                    # (reset on every read or write). Empty lists from
                    # clean shutdowns also report large idletime, so
                    # they're swept by the same path. Lists younger
                    # than ``visibility_timeout`` are left alone — the
                    # owning worker is presumed alive and processing.
                    list_idle = self._redis.object("idletime", proc_key) or 0
                    if list_idle < visibility_timeout:
                        continue
                    # Atomically drain the stale processing list back
                    # onto the main queue and DEL the source. The Lua
                    # script holds Redis's keyspace lock for the
                    # duration, so when two healthy reapers race on
                    # the same stale list, only the first sees a
                    # non-empty LRANGE; the second sees an already-
                    # deleted key and returns 0 — no duplicate
                    # requeue. Pre-fix, two reapers could both
                    # ``lrange`` the same key, both ``rpush`` each
                    # entry, and only the slower ``delete`` would
                    # win, producing one duplicate per healthy reaper.
                    try:
                        moved = self._redis.eval(
                            self._REAP_PROCESSING_LUA, 2, proc_key, requeue_key
                        )
                        recovered += int(moved or 0)
                    except Exception as e:
                        self.danger(f"Reaper: atomic move failed for {proc_key}: {e}")
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
