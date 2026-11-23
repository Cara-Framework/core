"""``read_snapshot``: one instant for a whole block of reads, without a database.

The sessions (a holder that exports; sessions that import, leased by one
statement at a time and reused whichever thread asks), what a failed
statement does to them, the relations built once in a session, and how
``PostgresConnection`` routes its statements and transactions inside a block.
The database half — every statement of an as-of request sees one instant
while another connection commits, a built relation is on no connection the
pool hands on — is the product's: ``api/tests/test_as_of_reads_postgres.py``,
``commons/tests/test_read_scope_built_relations_postgres.py``.
"""

from __future__ import annotations

import importlib
import threading

import pytest

from cara.eloquent import (
    BuiltRelation,
    ReadSnapshot,
    built_table_name,
    read_scope,
    read_snapshot,
)
from cara.eloquent.connections import PostgresReadSnapshot
from cara.eloquent.connections.BaseConnection import BaseConnection
from cara.eloquent.connections.PostgresConnection import PostgresConnection


class _Opener:
    """Fake sessions: the first exports ``1-A``, the others import it."""

    def __init__(self) -> None:
        self.opened: list[str | None] = []
        self.released: list[str] = []

    def __call__(self, exported):
        name = f"session-{len(self.opened)}"
        self.opened.append(exported)
        return (
            name,
            (None if exported else "0000000A-0000001B-1"),
            (lambda: self.released.append(name)),
        )


SID = "0000000A-0000001B-1"


def test_a_holder_exports_and_a_statement_leases_a_session_of_the_block():
    snapshot, opener = ReadSnapshot(), _Opener()
    first = snapshot.session(opener)
    assert first == "session-1"  # session-0 is the holder: it runs nothing
    # While it is leased, another statement gets a session of its own…
    second = snapshot.session(opener)
    assert second == "session-2"
    assert opener.opened == [None, SID, SID]  # exported once, imported per session
    # …and once handed back it serves the next statement, on any thread.
    snapshot.returned(first)
    seen: list[str] = []
    worker = threading.Thread(target=lambda: seen.append(snapshot.session(opener)))
    worker.start()
    worker.join()
    assert seen == [first] and len(opener.opened) == 3
    assert snapshot.owns(first) and not snapshot.owns("session-0")
    snapshot.close()
    assert sorted(opener.released) == ["session-0", "session-1", "session-2"]
    assert opener.released[-1] == "session-0"  # the holder ends last
    with pytest.raises(RuntimeError, match="ended"):
        snapshot.session(opener)


def test_a_failed_statement_ends_its_session_and_the_next_imports_afresh():
    snapshot, opener = ReadSnapshot(), _Opener()
    session = snapshot.session(opener)
    snapshot.failed(session)
    assert opener.released == [session]  # aborted: ended at once
    snapshot.returned(session)  # the wrapper's hand-back of a gone session: nothing
    again = snapshot.session(opener)
    assert again not in (session, "session-0") and opener.opened[-1] == SID
    snapshot.close()
    assert opener.released[-1] == "session-0"


class _Builder:
    """Records what it is asked to build; rows are read FROM ``built.<table>``."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def __call__(self, connection, exported, reopened, orders):
        self.calls.append((connection, exported, reopened, [o[0] for o in orders]))
        return {name: f"built.{table}" for name, table, _sql, _bound in orders}


def test_a_relation_is_built_once_in_a_session_whatever_the_statements():
    snapshot, opener, builder = ReadSnapshot(), _Opener(), _Builder()
    session = snapshot.session(opener)
    wanted = (("listing", "SELECT 1", ("tenant_id",)),)
    table = built_table_name("listing", "SELECT 1")
    for _statement in range(3):
        assert snapshot.relations(session, wanted, builder) == {
            "listing": f"built.{table}"
        }
    assert builder.calls == [(session, SID, False, ["listing"])]
    # A second relation: only the missing one is built, in a session that
    # has built before.
    both = (*wanted, ("product", "SELECT 2", ()))
    assert set(snapshot.relations(session, both, builder)) == {"listing", "product"}
    assert builder.calls[1] == (session, SID, True, ["product"])
    snapshot.close()


def test_the_idle_session_that_built_the_most_serves_the_next_statement():
    snapshot, opener, builder = ReadSnapshot(), _Opener(), _Builder()
    plain, built = snapshot.session(opener), snapshot.session(opener)
    snapshot.relations(built, (("listing", "SELECT 1", ()),), builder)
    snapshot.returned(plain)
    snapshot.returned(built)
    assert snapshot.session(opener) == built
    snapshot.close()


def test_what_was_built_is_the_sessions_own_and_goes_with_it():
    snapshot, opener, builder = ReadSnapshot(), _Opener(), _Builder()
    wanted = (("listing", "SELECT 1", ()),)
    first = snapshot.session(opener)
    snapshot.relations(first, wanted, builder)
    # Another session of the block builds its own: built rows are not shared.
    second = snapshot.session(opener)
    snapshot.relations(second, wanted, builder)
    assert [call[0] for call in builder.calls] == [first, second]
    # A failed session is ended with what it built; a new one builds afresh.
    snapshot.failed(first)
    third = snapshot.session(opener)
    snapshot.relations(third, wanted, builder)
    assert builder.calls[-1] == (third, SID, False, ["listing"])
    # A connection that is no session of the block is refused.
    with pytest.raises(RuntimeError, match="not a session"):
        snapshot.relations("stranger", wanted, builder)
    snapshot.close()


def test_a_built_table_is_named_by_its_relation_and_its_sql():
    one = built_table_name("listing", "SELECT * FROM public.listing WHERE tenant_id = 1")
    other = built_table_name(
        "listing", "SELECT * FROM public.listing WHERE tenant_id = 2"
    )
    assert one != other and one.startswith("cara_read_listing_")
    assert one == built_table_name(
        "listing", "SELECT * FROM public.listing WHERE tenant_id = 1"
    )
    assert len(built_table_name("x" * 80, "SELECT 1")) <= 63


def test_a_value_is_derived_once_for_the_block_and_waited_for_by_other_threads():
    snapshot = ReadSnapshot()
    calls: list[int] = []
    started, finish = threading.Event(), threading.Event()

    def compute():
        calls.append(1)
        started.set()
        finish.wait(5)
        return {"lines": 3}

    seen: list[dict] = []
    first = threading.Thread(target=lambda: seen.append(snapshot.once("k", compute)))
    first.start()
    assert started.wait(5)
    # A reader arriving while the first computes waits for it.
    second = threading.Thread(target=lambda: seen.append(snapshot.once("k", compute)))
    second.start()
    finish.set()
    first.join()
    second.join()
    assert len(calls) == 1 and seen[0] is seen[1]
    assert snapshot.once("k", compute) is seen[0] and len(calls) == 1
    assert snapshot.once("another key", lambda: 7) == 7

    def failing():
        raise RuntimeError("the read failed")

    # A compute that raises leaves nothing: the next reader computes.
    with pytest.raises(RuntimeError, match="the read failed"):
        snapshot.once("bad", failing)
    assert snapshot.once("bad", lambda: 1) == 1


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

    def fetchall(self):
        # The catalog's answer to "which indexes has this table": key columns.
        return [(["id"],), (["tenant_id"],), (["product_id"],), (["id", "tenant_id"],)]

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
            self._return_read_session()
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


def test_inside_a_block_a_statement_is_one_round_trip_on_a_leased_session(pool):
    handed, returned = pool
    with read_snapshot(), read_scope():
        first = _wrapper().make_connection()
        first.query("SELECT 1")
        second = _wrapper().make_connection()
        second.query("SELECT 2")
        assert first._connection is None  # leased, not pooled by the wrapper
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
    real = BaseConnection.statement

    def flaky(self, query, bindings=()):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("canceling statement")
        return real(self, query, bindings)

    monkeypatch.setattr(BaseConnection, "statement", flaky)
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


# ── relations built once in a session (BuiltRelation) ───────────────────────

_LISTING = BuiltRelation(
    "SELECT * FROM public.listing WHERE tenant_id = %s", [7], bound=("tenant_id",)
)
_LISTING_SQL = "SELECT * FROM public.listing WHERE tenant_id = 7"
_LISTING_TABLE = f'"{built_table_name("listing", _LISTING_SQL)}"'
_LISTING_ROWS = f"pg_temp.{_LISTING_TABLE}"
_FROM_BUILT = f'WITH "listing" AS NOT MATERIALIZED (SELECT * FROM {_LISTING_ROWS}) '
_GUARDED = "SAVEPOINT cara_read_only; SET LOCAL transaction_read_only = on"


@pytest.fixture
def catalog(monkeypatch):
    """Every test reads the (fake) catalog itself: no index list of another."""
    monkeypatch.setattr(PostgresReadSnapshot, "_INDEXED", {})


def test_a_built_relation_is_built_by_the_first_statement_and_read_by_the_rest(
    pool, catalog
):
    handed, _returned = pool
    with read_snapshot(), read_scope({"listing": _LISTING}):
        _wrapper().make_connection().query("SELECT 1")  # names nothing: nothing built
        _wrapper().make_connection().query("SELECT id FROM listing")
        _wrapper().make_connection().query("SELECT count(*) FROM listing")
    _holder, session = handed
    assert session.log == [
        _IMPORT,
        "SELECT 1",
        (PostgresReadSnapshot._INDEXES, ('"listing"',)),
        # ONE round trip: the session is reopened in the same snapshot, the
        # rows are built with their table's keys (never the bound column,
        # one key per leading column), and it is read-only again.
        "ROLLBACK; BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; "
        f"SET TRANSACTION SNAPSHOT '{SID}'; "
        f"CREATE TEMPORARY TABLE {_LISTING_TABLE} ON COMMIT DROP AS {_LISTING_SQL}; "
        f'CREATE INDEX ON {_LISTING_ROWS} ("id"); '
        f'CREATE INDEX ON {_LISTING_ROWS} ("product_id"); '
        f"ANALYZE {_LISTING_ROWS}; " + _GUARDED,
        _FROM_BUILT + "SELECT id FROM listing",
        _FROM_BUILT + "SELECT count(*) FROM listing",
        # The session ends by ROLLBACK and nothing else: what it built is
        # undone with the transaction that created it.
        "ROLLBACK",
    ]


def test_a_later_build_of_the_session_keeps_what_it_built_and_guards_again(pool, catalog):
    handed, _returned = pool
    product = BuiltRelation("SELECT * FROM public.product WHERE tenant_id = %s", [7])
    with read_snapshot(), read_scope({"listing": _LISTING}):
        _wrapper().make_connection().query("SELECT id FROM listing")
        with read_scope({"product": product}):
            _wrapper().make_connection().query(
                "SELECT 1 FROM product JOIN listing ON TRUE"
            )
    _holder, session = handed
    builds = [
        sent
        for sent in session.log
        if isinstance(sent, str) and "CREATE TEMPORARY" in sent
    ]
    assert len(builds) == 2 and builds[0].startswith("ROLLBACK; BEGIN TRANSACTION")
    # The second build leaves the read-only savepoint (which only read),
    # builds at the top level and guards again; the first table is not rebuilt.
    assert builds[1].startswith(
        "ROLLBACK TO SAVEPOINT cara_read_only; RELEASE SAVEPOINT cara_read_only; "
        "CREATE TEMPORARY TABLE "
    )
    assert builds[1].endswith(_GUARDED) and _LISTING_TABLE not in builds[1]
    assert _LISTING_ROWS in session.log[-2] and "cara_read_product_" in session.log[-2]


def test_a_build_that_fails_ends_the_session_and_the_next_statement_builds_afresh(
    pool, catalog, monkeypatch
):
    handed, _returned = pool
    scoped = importlib.import_module("cara.eloquent.connections.PostgresScopedReads")
    real, calls = scoped.build_relations, {"n": 0}

    def flaky(connection, exported, reopened, orders):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("permission denied to create temporary tables")
        return real(connection, exported, reopened, orders)

    monkeypatch.setattr(scoped, "build_relations", flaky)
    with read_snapshot(), read_scope({"listing": _LISTING}):
        with pytest.raises(Exception, match="temporary tables"):
            _wrapper().make_connection().query("SELECT id FROM listing")
        _wrapper().make_connection().query("SELECT id FROM listing")
    _holder, failed, fresh = handed
    assert failed.log == [_IMPORT, "ROLLBACK"]  # never used again, never pooled dirty
    assert fresh.log[0] == _IMPORT and fresh.log[-2:] == [
        _FROM_BUILT + "SELECT id FROM listing",
        "ROLLBACK",
    ]


def test_a_transaction_in_a_block_reads_a_built_relation_inline(pool, catalog):
    handed, _returned = pool
    with read_snapshot(), read_scope({"listing": _LISTING}):
        wrapper = _wrapper().make_connection()
        wrapper.begin()
        wrapper.query("SELECT id FROM listing")
        wrapper.commit()
    own = handed[-1]
    assert own.log[1] == (
        f'WITH "listing" AS NOT MATERIALIZED ({_LISTING_SQL}) SELECT id FROM listing'
    )
    assert not any("CREATE TEMPORARY" in str(sent) for raw in handed for sent in raw.log)


def test_outside_a_snapshot_a_built_relation_is_read_inline(pool, catalog):
    handed, _returned = pool
    with read_scope({"listing": _LISTING}):
        _wrapper().make_connection().query("SELECT id FROM listing")
    sent = [entry for raw in handed for entry in raw.log]
    assert (
        f'WITH "listing" AS NOT MATERIALIZED ({_LISTING_SQL}) SELECT id FROM listing'
        in sent
    )
    assert not any("CREATE TEMPORARY" in str(entry) for entry in sent)


def test_the_lookup_keys_of_a_built_relation():
    indexes = [
        ["id"],
        ["tenant_id"],
        ["product_id"],
        ["id", "tenant_id"],
        ["tenant_id", "channel_id"],
        ["channel_id", "external_id"],
    ]
    # Without the bound column; one key per leading column, the shortest.
    assert PostgresReadSnapshot.lookup_keys(indexes, ("tenant_id",)) == (
        ("id",),
        ("product_id",),
        ("channel_id",),
    )
    assert PostgresReadSnapshot.lookup_keys([], ()) == ()


def test_a_build_refuses_what_is_not_a_snapshot_id():
    with pytest.raises(RuntimeError, match="snapshot id"):
        PostgresReadSnapshot.build_relations(_Raw("x"), "1'; DROP TABLE t; --", False, [])
