"""The published schedule snapshot — the contract between the scheduler
process and anything that wants to answer "when is the next run?".

``schedule:work`` republishes the live schedule to the shared cache every
``SCHEDULE_SNAPSHOT_EVERY_SECONDS``; readers (an API serving a dashboard)
call :func:`read_schedule_snapshot` from their own process. The TTL is the
honesty mechanism: a scheduler that dies stops republishing, the key
expires, and readers see ``None`` — "not reporting" — instead of a
forever-stale table of next-run times that will never happen.

This module exists so readers never import the command: pulling a CLI
module (auto-reload machinery, console UX) into a request path to reach a
string constant would be the wrong dependency direction.
"""

from __future__ import annotations

import json
from typing import Any

SCHEDULE_SNAPSHOT_CACHE_KEY = "scheduler:snapshot"
SCHEDULE_SNAPSHOT_EVERY_SECONDS = 30
SCHEDULE_SNAPSHOT_TTL_SECONDS = 120


def read_schedule_snapshot() -> dict[str, Any] | None:
    """The last published snapshot, or ``None`` when the scheduler is not
    reporting (down, or its last publish has expired).

    Shape: ``{"published_at": iso, "jobs": [{"id", "name", "next_run_at"}]}``
    where ``next_run_at`` is ``None`` for a paused job.
    """
    from cara.facades import Cache

    raw = Cache.get(SCHEDULE_SNAPSHOT_CACHE_KEY)
    if not raw:
        return None
    try:
        snapshot = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return snapshot if isinstance(snapshot, dict) else None
