"""``read_snapshot``: one instant for a whole block of reads, without a database.

The sessions (one per thread, the first exports, the others import), what a
failed statement does to them, and how ``PostgresConnection`` routes its
statements and transactions inside a block. The database half — every
statement of an as-of request sees one instant while another connection
commits — is the product's: ``api/tests/test_as_of_reads_postgres.py``.
"""

from __future__ import annotations

import threading

import pytest

from cara.eloquent import ReadSnapshot, read_scope, read_snapshot
from cara.eloquent.connections import PostgresReadSnapshot
from cara.eloquent.connections.PostgresConnection import PostgresConnection


class _Opener:
    """Fake sessions: the first exports ``1-A``, the others import it."""

    def __init__(self) -> None:
        self.opened: list[str | None] = []
        self.released: list[str] = []

    def __call__(self, exported):
        name = f"session-{len(self.opened)}"
        self.opened.append(exported)
        return name, (None if exported else "0000000A-0000001B-1"), (
            lambda: self.released.append(name)
        )


SID = "0000000A-0000001B-1"


def test_a_holder_exports_and_each_thread_imports_its_own_session():
    snapshot, opener = ReadSnapshot(), _Opener()
    first = snapshot.session(opener)
    assert first == "session-1"  # session-0 is the holder: it runs nothing
    assert snapshot.session(opener) == first  # the thread keeps its session
    seen: list[str] = []
    worker = threading.Thread(target=lambda: seen.append(snapshot.session(opener)))
    worker.start()
    worker.join()
    assert seen == ["session-2"]
    assert opener.opened == [None, SID, SID]  # exported once, imported per thread
    assert snapshot.owns(first) and not snapshot.owns("session-0")
    snapshot.close()
    assert sorted(opener.released) == ["session-0", "session-1", "session-2"]
    assert opener.released[-1] == "session-0"  # the holder ends last
    with pytest.raises(RuntimeError, match="ended"):
        snapshot.session(opener)


def test_a_failed_statement_ends_its_session_and_the_thread_imports_afresh():
    snapshot, opener = ReadSnapshot(), _Opener()
    session = snapshot.session(opener)
    snapshot.failed(session)
    assert opener.released == [session]  # aborted: ended at once
    again = snapshot.session(opener)
    assert again not in (session, "session-0") and opener.opened[-1] == SID
    snapshot.close()
    assert opener.released[-1] == "session-0"


def test_a_transaction_needs_the_snapshot_exported_first():
    snapshot, opener = ReadSnapshot(), _Opener()
    assert snapshot.snapshot_id(opener) == "0000000A-0000001B-1"
    assert snapshot.snapshot_id(opener) == "0000000A-0000001B-1"
    assert opener.opened == [None]
    snapshot.close()


def test_an_opener_that_exports_no_snapshot_id_is_refused():
    snapshot = ReadSnapshot()
    released: list[bool] = []
    with pytest.raises(RuntimeError, match="snapshot id"):
        snapshot.session(lambda _e: ("c", "not an id", lambda: released.append(True)))
    assert released == [True]


class _Cursor:
    def __init__(self, log: list) -> None:
        self.log, self.description, self.rowcount = log, None, 0

    def execute(self, sql, params=None) -> None:
        self.log.append(sql if params is None else (sql, tuple(params)))

    def fetchone(self):
        return ("0000000A-0000001B-1",)

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass


class _Raw:
    """A psycopg2 connection stand-in that logs what it is sent."""

    def __init__(self, name: str) -> None:
        self.name, self.log, self.autocommit, self.closed = name, [], True, False

    def cursor(self, cursor_factory=None) -> _Cursor:
        return _Cursor(self.log)

    def commit(self) -> None:
        self.log.append("COMMIT")

    def rollback(self) -> None:
        self.log.append("ROLLBACK()")


@pytest.fixture
def pool(monkeypatch):
    """``pooled_connection`` hands out logged fakes; ``close_connection``
    of a pooled one records its return."""
    handed: list[_Raw] = []
    returned: list[str] = []

    def pooled(self):
        raw = _Raw(f"raw-{len(handed)}")
        handed.append(raw)
        return raw

    def close(self):
        if self._read_session is not None:
            self._connection, self._read_session = None, None
            return
        if self._connection is not None:
            returned.append(self._connection.name)
        self._connection = None

    monkeypatch.setattr(PostgresConnection, "pooled_connection", pooled)
    monkeypatch.setattr(PostgresConnection, "close_connection", close)
    return handed, returned


def _wrapper() -> PostgresConnection:
    return PostgresConnection(host="h", database="d", user="u", full_details={})


_IMPORT = (
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; "
    "SET TRANSACTION SNAPSHOT %s",
    (SID,),
)


def test_inside_a_block_a_statement_is_one_round_trip_on_the_threads_session(pool):
    handed, returned = pool
    with read_snapshot(), read_scope():
        first = _wrapper().make_connection()
        first.query("SELECT 1")
        second = _wrapper().make_connection()
        second.query("SELECT 2")
        assert first._connection is None  # borrowed, not pooled by the wrapper
    holder, session = handed
    assert holder.log == [
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; "
        "SELECT pg_export_snapshot()",
        "ROLLBACK",
    ]
    assert session.log == [_IMPORT, "SELECT 1", "SELECT 2", "ROLLBACK"]
    assert sorted(returned) == ["raw-0", "raw-1"] and returned[-1] == "raw-0"


def test_a_transaction_in_a_block_reads_the_snapshot_on_its_own_connection(pool):
    handed, _returned = pool
    with read_snapshot(), read_scope():
        wrapper = _wrapper().make_connection()
        wrapper.begin()
        wrapper.query("SELECT 3")
        wrapper.commit()
    _holder, session, own = handed
    assert own.log == [
        (
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; "
            "SET TRANSACTION SNAPSHOT %s",
            ("0000000A-0000001B-1",),
        ),
        "SELECT 3",
        "COMMIT",
    ]
    assert session.log[-1] == "ROLLBACK" and "SELECT 3" not in session.log


def test_a_failed_statement_on_a_session_is_followed_by_a_fresh_import(pool, monkeypatch):
    handed, _returned = pool
    calls = {"n": 0}
    real = PostgresConnection.__mro__[1].statement

    def flaky(self, query, bindings=()):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("canceling statement")
        return real(self, query, bindings)

    monkeypatch.setattr(PostgresConnection.__mro__[1], "statement", flaky)
    with read_snapshot(), read_scope():
        with pytest.raises(Exception, match="canceling"):
            _wrapper().make_connection().query("SELECT 1")
        _wrapper().make_connection().query("SELECT 2")
    holder, failed, importer = handed
    assert failed.log == [_IMPORT, "ROLLBACK"]  # ended at once, its statement aborted
    assert importer.log == [_IMPORT, "SELECT 2", "ROLLBACK"]
    assert holder.log[-1] == "ROLLBACK"  # the holder ran nothing, ended last


def test_the_postgres_session_module_is_the_one_that_opens_sessions():
    assert PostgresReadSnapshot.open_session.__module__.endswith("PostgresReadSnapshot")
