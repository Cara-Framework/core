"""Fire-and-forget wide-event writer → ClickHouse (Obs-7).

One wide row per unit of work (a job execution). ``emit()`` only drops
the event onto a bounded in-memory queue and returns immediately — a
single background daemon thread batches rows and POSTs them to
ClickHouse over the HTTP interface. So emitting:

  * never blocks the job (just a queue put),
  * never fails the job (every error is swallowed),
  * never holds the process open (daemon thread, bounded queue).

Disabled unless ``wide_events.enabled`` is truthy. When ClickHouse is
unreachable the batch is dropped silently — wide events are analytics,
never load-bearing.

Config (cara ``config()`` with UPPER_SNAKE env fallback):
    wide_events.enabled         "1"/"true" to turn on        (default off)
    wide_events.clickhouse_url  HTTP endpoint                (default http://localhost:8123)
    wide_events.db / .user / .password                       (default "default"/"default"/"")
    wide_events.batch_rows      flush at N rows              (default 200)
    wide_events.batch_secs      or after N seconds           (default 5)
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from typing import Any

_QUEUE: queue.Queue = queue.Queue(maxsize=10000)
_worker_started = False
_start_lock = threading.Lock()


def _env(key: str, default: str = "") -> str:
    try:
        from cara.configuration import config

        val = config(key)
        if val is not None:
            return str(val)
    except (OSError, RuntimeError, AttributeError, ConnectionError):
        pass
    return os.environ.get(key.upper().replace(".", "_"), str(default))


def _truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def enabled() -> bool:
    return _truthy(_env("wide_events.enabled", ""))


def emit(event: dict[str, Any]) -> None:
    """Queue one wide event. Non-blocking; drops on overflow or when off."""
    if not enabled():
        return
    try:
        _ensure_worker()
        _QUEUE.put_nowait(event)
    except Exception:
        # Queue full or anything else → drop. Analytics are never
        # allowed to back-pressure or break the job path.
        pass


def _ensure_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    with _start_lock:
        if _worker_started:
            return
        threading.Thread(
            target=_run, name="wide-events-writer", daemon=True
        ).start()
        _worker_started = True


def _run() -> None:
    url = _env("wide_events.clickhouse_url", "http://localhost:8123")
    db = _env("wide_events.db", "default")
    user = _env("wide_events.user", "default")
    password = _env("wide_events.password", "")
    insert_sql = f"INSERT INTO {db}.wide_events FORMAT JSONEachRow"
    try:
        flush_rows = int(_env("wide_events.batch_rows", "200") or 200)
        flush_secs = float(_env("wide_events.batch_secs", "5") or 5)
    except Exception:
        flush_rows, flush_secs = 200, 5.0

    try:
        import requests
    except Exception:
        return  # no HTTP client available → nothing to write to

    buf: list[dict] = []
    last = time.monotonic()
    while True:
        try:
            buf.append(_QUEUE.get(timeout=max(0.1, flush_secs)))
        except queue.Empty:
            pass
        due = len(buf) >= flush_rows or (time.monotonic() - last) >= flush_secs
        if buf and due:
            batch, buf = buf, []
            last = time.monotonic()
            try:
                body = "\n".join(json.dumps(r, default=str) for r in batch)
                requests.post(
                    url,
                    params={"query": insert_sql},
                    data=body.encode("utf-8"),
                    auth=(user, password),
                    timeout=5,
                )
            except (OSError, RuntimeError, AttributeError, ConnectionError):
                pass  # ClickHouse down / transient → drop the batch


__all__ = ["emit", "enabled"]
