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
* a statement LEASES a session of the block for as long as it runs: an idle
  one when the block has one (the one with the most relations built first),
  else a new session that IMPORTS the snapshot (``SET TRANSACTION
  SNAPSHOT``) — the same instant on every connection. The lease is the
  statement's, not the thread's: a request whose reads hop between worker
  threads (``ExecutionContext.run_in_thread`` copies the context) runs them
  on ONE session, and only reads that truly overlap open a second. No
  session ever runs two statements at once;
* a statement is ONE round trip, nothing is wrapped per statement (the
  transaction is read-only, so a write fails with SQLSTATE 25006 at the
  database);
* an explicit transaction opened inside the block (``atomic``) imports the
  snapshot too, on its own connection, so nesting and savepoints keep their
  meaning and still read the same instant;
* a statement that fails aborts only its own session: it is ended, and the
  next statement leases another or imports the snapshot afresh from the
  holder;
* the block's end rolls every session back and returns it to the pool, the
  holder last.

**Built relations.** A session also keeps the relations BUILT in it
(``cara.eloquent.BuiltRelation``): rows a scope derives once and every later
statement of the session reads, held in the session's own transaction — so
they are of this block alone (its snapshot, its scope's instant and
workspace), end with the session's rollback on every exit path (the block's
end, a failed statement, an error while building), and are never on a
connection the pool hands to anyone else. What was built is keyed by the
relation's name AND its rendered SQL, so a nested scope that replaces a
relation never reads the outer scope's rows.

**Derived once.** What application code derives from the database inside the
block — not a relation, a value it computes from several reads — can be kept
for the block too (``once``, reached through ``cara.eloquent.read_once``):
the block reads one instant, so the value cannot change while it lasts.

Driver-neutral: the snapshot holds sessions; the connection class opens them
(``opener(exported_id) -> (connection, exported_id | None, release)``) and
builds in them (``builder(connection, exported_id, reopened, orders)``).
"""

from __future__ import annotations

import contextlib
import hashlib
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: What ``pg_export_snapshot()`` returns (``00000003-0000001B-1``).
SNAPSHOT_ID = re.compile(r"\A[0-9A-Fa-f]+(?:-[0-9A-Fa-f]+)+\Z")

#: ``opener(exported_id)``: a new session in the snapshot. With ``None`` it
#: exports one and returns its id; with an id it imports it and returns None.
Opener = Callable[[str | None], tuple[Any, str | None, Callable[[], None]]]

#: One relation to build: ``(name, table, sql, bound columns)`` — ``table``
#: is the name its rows are kept under in the session.
BuildOrder = tuple[str, str, str, tuple[str, ...]]

#: ``builder(connection, exported_id, reopened, orders)``: build ``orders``
#: in the session and return, per relation name, what a statement reads its
#: rows ``FROM``. ``reopened`` says the session has built before (it is
#: already in the form that can).
Builder = Callable[[Any, str, bool, Sequence[BuildOrder]], Mapping[str, str]]


def built_table_name(name: str, sql: str) -> str:
    """The name a relation's built rows are kept under: the relation's name
    and a digest of its rendered SQL (which holds its instant and its
    workspace as literals), inside the 63 bytes an identifier may have."""
    digest = hashlib.sha256(f"{name}\0{sql}".encode()).hexdigest()[:16]
    return f"cara_read_{name[:35]}_{digest}"


@dataclass(slots=True)
class _Session:
    connection: Any
    release: Callable[[], None]
    leased: bool = True
    #: Has it built before (the builder's ``reopened``)?
    reopened: bool = False
    #: ``(name, relation sql)`` → what to read its built rows FROM.
    built: dict[tuple[str, str], str] = field(default_factory=dict)


@dataclass(slots=True)
class _Derived:
    """One value derived once for the block (:meth:`ReadSnapshot.once`)."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    done: bool = False
    value: Any = None


class ReadSnapshot:
    """The holder of one read block's instant, and the sessions its
    statements lease to read at it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._holder: _Session | None = None
        self._sessions: list[_Session] = []
        self._exported: str | None = None
        self._closed = False
        self._derived: dict[Any, _Derived] = {}

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

    def _held(self, connection: Any) -> _Session | None:
        return next((s for s in self._sessions if s.connection is connection), None)

    def session(self, opener: Opener) -> Any:
        """Lease a connection inside the snapshot: an idle session of the
        block — the one with the most relations built — else a new one.
        The caller hands it back with :meth:`returned`."""
        with self._lock:
            if self._closed:
                raise RuntimeError("read_snapshot: the block has ended")
            idle = [held for held in self._sessions if not held.leased]
            if idle:
                held = max(idle, key=lambda session: len(session.built))
                held.leased = True
                return held.connection
            connection, _none, release = opener(self._export(opener))
            self._sessions.append(_Session(connection, release))
            return connection

    def returned(self, connection: Any) -> None:
        """The statement that leased ``connection`` is done with it."""
        with self._lock:
            held = self._held(connection)
            if held is not None:
                held.leased = False

    def relations(
        self,
        connection: Any,
        wanted: Sequence[tuple[str, str, tuple[str, ...]]],
        builder: Builder,
    ) -> dict[str, str]:
        """What a statement on ``connection`` reads each of ``wanted`` FROM
        (``(name, relation sql, bound columns)``), building in that session
        the ones it has not built yet — once per session, whatever the
        number of statements that name them. The caller holds the lease, so
        nothing else runs on the session meanwhile; a builder that raises
        leaves the session to be ended (:meth:`failed`)."""
        with self._lock:
            held = self._held(connection)
            exported = self._exported
        if held is None or exported is None:
            raise RuntimeError("read_snapshot: not a session of this block")
        missing = [
            (name, built_table_name(name, sql), sql, bound)
            for name, sql, bound in wanted
            if (name, sql) not in held.built
        ]
        if missing:
            sources = builder(connection, exported, held.reopened, missing)
            held.reopened = True
            for name, _table, sql, _bound in missing:
                held.built[(name, sql)] = sources[name]
        return {name: held.built[(name, sql)] for name, sql, _bound in wanted}

    def once(self, key: Any, compute: Callable[[], Any]) -> Any:
        """``compute()``'s value, computed ONCE for the block under ``key``:
        the first caller computes, a caller arriving meanwhile (another
        thread of the block) waits for it, every later one takes the value.
        A ``compute`` that raises leaves nothing: the next caller computes.
        The block reads one instant, so what a pure function of the
        database derives in it does not change while it lasts."""
        with self._lock:
            derived = self._derived.get(key)
            if derived is None:
                derived = self._derived[key] = _Derived()
        with derived.lock:
            if not derived.done:
                derived.value = compute()
                derived.done = True
            return derived.value

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
            return self._held(connection) is not None

    def failed(self, connection: Any) -> None:
        """A statement failed on ``connection``: its transaction is aborted,
        so the session is ended — what was built in it goes with it; the
        next statement leases another or imports afresh (the holder keeps
        the snapshot importable)."""
        with self._lock:
            held = self._held(connection)
            if held is None:
                return
            self._sessions.remove(held)
        with contextlib.suppress(Exception):
            held.release()

    def close(self) -> None:
        """Roll every session back and hand it to the pool, the holder last."""
        with self._lock:
            self._closed = True
            sessions = [*self._sessions, *([self._holder] if self._holder else [])]
            self._sessions.clear()
            self._holder = None
        for held in sessions:
            with contextlib.suppress(Exception):
                held.release()


__all__ = [
    "SNAPSHOT_ID",
    "BuildOrder",
    "Builder",
    "Opener",
    "ReadSnapshot",
    "built_table_name",
]
