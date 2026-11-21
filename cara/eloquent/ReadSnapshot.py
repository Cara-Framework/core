"""ReadSnapshot — one instant of the database for a whole block of reads.

A read that spans many statements (a page: its rows, their evidence, the
counts, read side by side on several threads) sees, statement by statement,
whatever had committed when each one began. For most pages that is fine; for
a read that must answer ONE instant exactly (a temporal read: which heads
changed after T, then the page over them) a commit landing between two of
its statements makes the answer a mixture.

Inside ``read_snapshot()`` (``cara.eloquent.ReadScope``) every statement runs
in ONE database snapshot:

* the first statement opens the block's HOLDER: a session in a ``REPEATABLE
  READ, READ ONLY`` transaction that EXPORTS its snapshot
  (``pg_export_snapshot()``) and runs nothing else. It is its own session
  because an exported snapshot lives only while the exporting transaction
  is open and un-aborted: a statement that fails in a transaction ends its
  export at once, so the exporter may never run a statement that can fail;
* every thread of the block (``ExecutionContext.run_in_thread`` copies the
  context) opens its own session that IMPORTS it (``SET TRANSACTION
  SNAPSHOT``) — the same instant on every connection, no statement shared
  between threads;
* a session is the thread's for the whole block: a statement is ONE round
  trip, nothing is wrapped per statement (the transaction is read-only, so a
  write fails with SQLSTATE 25006 at the database);
* an explicit transaction opened inside the block (``atomic``) imports the
  snapshot too, on its own connection, so nesting and savepoints keep their
  meaning and still read the same instant;
* a statement that fails aborts only its own session: it is ended, and the
  next statement of that thread imports the snapshot afresh from the holder;
* the block's end rolls every session back and returns it to the pool, the
  holder last.

Driver-neutral: the snapshot holds sessions; the connection class opens them
(``opener(exported_id) -> (connection, exported_id | None, release)``).
"""

from __future__ import annotations

import contextlib
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

#: What ``pg_export_snapshot()`` returns (``00000003-0000001B-1``).
SNAPSHOT_ID = re.compile(r"\A[0-9A-Fa-f]+(?:-[0-9A-Fa-f]+)+\Z")

#: ``opener(exported_id)``: a new session in the snapshot. With ``None`` it
#: exports one and returns its id; with an id it imports it and returns None.
Opener = Callable[[str | None], tuple[Any, str | None, Callable[[], None]]]


@dataclass(slots=True)
class _Session:
    connection: Any
    release: Callable[[], None]


class ReadSnapshot:
    """The holder of one read block's instant, and its sessions (one per
    thread) that read at it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._holder: _Session | None = None
        self._sessions: dict[int, _Session] = {}
        self._exported: str | None = None
        self._closed = False

    @property
    def exported(self) -> str | None:
        """The snapshot's id once the holder exported it."""
        return self._exported

    def _export(self, opener: Opener) -> str:
        """The exported id, opening the holder first (under the lock)."""
        if self._exported is None:
            connection, exported, release = opener(None)
            if not exported or not SNAPSHOT_ID.match(exported):
                release()
                raise RuntimeError(f"read_snapshot: not a snapshot id: {exported!r}")
            self._holder = _Session(connection, release)
            self._exported = exported
        return self._exported

    def session(self, opener: Opener) -> Any:
        """This thread's connection inside the snapshot, opened on first use."""
        ident = threading.get_ident()
        with self._lock:
            if self._closed:
                raise RuntimeError("read_snapshot: the block has ended")
            held = self._sessions.get(ident)
            if held is not None:
                return held.connection
            connection, _none, release = opener(self._export(opener))
            self._sessions[ident] = _Session(connection, release)
            return connection

    def snapshot_id(self, opener: Opener) -> str:
        """The exported id — what a transaction inside the block imports."""
        with self._lock:
            if self._closed:
                raise RuntimeError("read_snapshot: the block has ended")
            return self._export(opener)

    def owns(self, connection: Any) -> bool:
        """Is ``connection`` a live session of this block?"""
        if connection is None:
            return False
        with self._lock:
            return any(s.connection is connection for s in self._sessions.values())

    def failed(self, connection: Any) -> None:
        """A statement failed on ``connection``: its transaction is aborted,
        so the session is ended; the thread's next statement imports afresh
        (the holder keeps the snapshot importable)."""
        with self._lock:
            for ident, held in list(self._sessions.items()):
                if held.connection is connection:
                    del self._sessions[ident]
                    with contextlib.suppress(Exception):
                        held.release()
                    return

    def close(self) -> None:
        """Roll every session back and hand it to the pool, the holder last."""
        with self._lock:
            self._closed = True
            sessions = [*self._sessions.values(), *([self._holder] if self._holder else [])]
            self._sessions.clear()
            self._holder = None
        for held in sessions:
            with contextlib.suppress(Exception):
                held.release()


__all__ = ["SNAPSHOT_ID", "Opener", "ReadSnapshot"]
