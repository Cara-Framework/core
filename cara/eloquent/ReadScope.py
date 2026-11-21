"""ReadScope — a block of work that only reads, over relations it may rename.

Some reads must be guaranteed side-effect free at the DATABASE, not only by
the code that happens to run inside them (a temporal read of a product at a
past instant, an operator's inspection): a write there is a bug, and the
database is the one place that refuses every writer alike — an ORM save, a
raw ``DB.statement``, a nested ``atomic`` block, a library's own insert.

Inside ``read_scope(...)`` every statement any connection runs, on any thread
that copied the context (``ExecutionContext.run_in_thread`` does), executes
in a ``READ ONLY`` transaction:

* a connection in autocommit (the pooled default) wraps the one statement in
  ``BEGIN TRANSACTION READ ONLY`` … ``ROLLBACK`` — no session state is left
  on the connection when it goes back to the pool;
* a connection already inside a transaction (an ``atomic`` block opened in
  the scope, a test's pinned connection) runs the statement in a savepoint
  with ``SET LOCAL transaction_read_only = on`` and rolls that savepoint
  back, so the enclosing transaction is untouched.

A write in either form fails with SQLSTATE 25006 (``read_only_sql_transaction``).

**Relations.** A scope may also name relations: ``{"product": (sql, params)}``.
Every ``SELECT`` / ``WITH`` statement of the scope that mentions one of those
names is run as ``WITH product AS NOT MATERIALIZED (<sql>) <statement>``, so
each unqualified ``product`` in it — the ``FROM``, a join, a subquery, an
``EXISTS`` — reads the named relation instead of the table. That is the SQL
scoping rule (a ``WITH`` name hides a table of the same name in its
statement), not a rewrite of the statement: its own text is unchanged. A
schema-qualified name (``public.product``) still reads the table, and that is
how a relation's own SQL reads the table it stands for. ``NOT MATERIALIZED``
lets each reference inline, so the statement's predicates reach the
relation's own indexes. A statement that defines its own ``WITH`` of the same
name keeps its own.

A relation's SQL is rendered once, at scope entry, with its parameters as
literals (the form psycopg2 sends a bind in anyway); the rendered text holds
no ``%``, so it composes with a statement whether or not that statement has
bindings.

**Built relations.** A relation given as a ``BuiltRelation`` costs real work
to derive, and is derived ONCE: inside ``read_snapshot()`` the first
statement that names it builds it in its session (a temporary table that
lives and dies with that session's transaction), and from then on a
statement carries ``WITH product AS NOT MATERIALIZED (SELECT * FROM <the
built rows>)`` — the same scoping rule over rows already derived. Outside
``read_snapshot()`` it is read inline, like any relation.

**One instant.** ``read_snapshot()`` makes the whole block ONE database
snapshot (``cara.eloquent.ReadSnapshot``): every statement of every thread of
the block, in a ``REPEATABLE READ, READ ONLY`` transaction that imports the
same exported snapshot. A ``read_scope`` inside it keeps the snapshot. Without
it each statement is its own read-only transaction, as above. Because the
block is one instant, what a reader derives from it by several statements
can be kept for the block: :func:`read_once`.

**The scope's clock.** A scope may fix what "now" means for its reads
(``clock=<aware datetime>``). Application code reads it with
:func:`current_read_clock`; SQL reads it through ONE expression,
:data:`READ_CLOCK_SQL` — the scope binds its instant as the transaction-local
setting ``cara.read_clock`` in front of every read statement that names it
(``SET LOCAL``, same round trip, gone with the statement's transaction), and
outside a clocked scope the expression is the database clock. So a predicate
written once (``ship_by < {READ_CLOCK_SQL}``) answers at the scope's instant inside
it and at now everywhere else, with no parameter threaded through its callers.
The same setting read as an upper bound, :data:`READ_CLOCK_BOUND_SQL`, holds rows
the relations do not bound (a function over a log) to the scope's instant, and
bounds nothing outside a clocked scope. Python reads the clock through
:func:`read_now` (the instant, or the aware wall clock).
A read statement of a clocked scope that reads the database clock directly
(``now()``, ``CURRENT_DATE``, ``CURRENT_TIMESTAMP``, ``LOCALTIMESTAMP``,
``clock_timestamp()``, …) is refused with :class:`ReadScopeRefused` — it would
answer today's clock under the scope's instant.

**Refused relations.** A scope may name relations its statements must not read
(``refuse=("payout_summary", …)``: tables that hold today only). A read
statement naming one unqualified is refused with :class:`ReadScopeRefused`.

Generic: no table or domain name lives here; the caller names the relations.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pendulum

from .BuiltRelation import BuiltRelation
from .ReadScopeRefused import ReadScopeRefused
from .ReadSnapshot import ReadSnapshot

_SAVEPOINT = "cara_read_scope"
_PLACEHOLDER = re.compile(r"%s|%%")
_LEADING_NOISE = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|\Z)|/\*.*?\*/)*", re.DOTALL)
_READ_STATEMENT = re.compile(r"\A(?:SELECT|WITH|VALUES|TABLE|\()", re.IGNORECASE)
_WITH_HEAD = re.compile(r"\AWITH(\s+RECURSIVE)?\s+", re.IGNORECASE)


def _literal(value: Any) -> str:
    """One parameter as the SQL literal psycopg2 would send for it."""
    from psycopg2.extensions import adapt  # local: heavy optional dep

    quoted = adapt(value).getquoted()
    return quoted.decode("utf-8") if isinstance(quoted, bytes) else str(quoted)


def render_literals(sql: str, params: Sequence[Any]) -> str:
    """``sql`` with each ``%s`` replaced by its parameter's literal and ``%%``
    by ``%``. Refuses a count mismatch and a rendered ``%`` (the text must
    compose with a statement that has its own bindings)."""
    values = list(params)
    position = 0

    def _substitute(match: re.Match[str]) -> str:
        nonlocal position
        if match.group(0) == "%%":
            return "%"
        if position >= len(values):
            raise ValueError("render_literals: more %s placeholders than parameters")
        rendered = _literal(values[position])
        position += 1
        return rendered

    rendered = _PLACEHOLDER.sub(_substitute, sql)
    if position != len(values):
        raise ValueError("render_literals: more parameters than %s placeholders")
    if "%" in rendered:
        raise ValueError("a scoped relation may not render a '%' (it would read as a bind)")
    return rendered


#: The transaction-local setting a clocked scope binds its instant to.
READ_CLOCK_SETTING = "cara.read_clock"
#: The request clock in SQL: the scope's instant inside a clocked scope, the
#: database clock (transaction start) everywhere else. The one sanctioned
#: ``now()`` of a read: the guard below reads past it.
READ_CLOCK_SQL = (
    f"COALESCE(NULLIF(current_setting('{READ_CLOCK_SETTING}', true), '')::timestamptz, now())"
)
#: The request clock as an UPPER BOUND on what a read sees — for rows the
#: scope's relations do not bound (a function over an append-only log):
#: ``recorded_at <= {READ_CLOCK_BOUND_SQL}`` is the rows recorded by the scope's
#: instant inside a clocked scope, and NO bound everywhere else
#: (``'infinity'``) — never the transaction's start, which would hide a row
#: the same transaction wrote a moment ago. Bound like :data:`READ_CLOCK_SQL`.
READ_CLOCK_BOUND_SQL = (
    f"COALESCE(NULLIF(current_setting('{READ_CLOCK_SETTING}', true), '')::timestamptz, "
    "'infinity'::timestamptz)"
)

#: The database's own clocks (a function call or the SQL-standard keyword).
_CLOCK = re.compile(
    r"(?<!\w)(?:now|clock_timestamp|statement_timestamp|transaction_timestamp|timeofday)"
    r"\s*\(|(?<![\w.\"])(?:current_date|current_timestamp|current_time|localtimestamp|"
    r"localtime)(?![\w\"])",
    re.IGNORECASE,
)
_CLOCK_WORDS = ("now", "current_", "local", "_timestamp", "timeofday")
#: String literals and comments: text, not SQL (a word there reads nothing).
_INERT = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def _reads_clock(sql: str) -> bool:
    """Does ``sql`` read the database clock other than through READ_CLOCK_SQL?"""
    lowered = sql.lower()
    if not any(word in lowered for word in _CLOCK_WORDS):
        return False
    return _CLOCK.search(_INERT.sub(" ", sql.replace(READ_CLOCK_SQL, " "))) is not None


@dataclass(frozen=True, slots=True)
class ReadScope:
    """The active scope: every statement read-only, these names renamed;
    optionally one snapshot, a fixed clock and refused relations."""

    relations: tuple[tuple[str, str], ...] = ()
    clock: datetime | None = None
    refused: tuple[str, ...] = ()
    snapshot: ReadSnapshot | None = None
    #: The relations to build once per session (``BuiltRelation``): name →
    #: the columns every row holds one value in.
    built: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.relations)

    def statement(self, sql: str, tables: Mapping[str, str] | None = None) -> str:
        """``sql`` with the scope's relations it mentions in front of it, and
        the scope's clock bound when it reads :data:`READ_CLOCK_SQL`.
        ``tables`` names, per relation, the rows already built for the
        session that runs the statement (what to read ``FROM``): such a
        relation is read from them instead of derived inline.
        Raises :class:`ReadScopeRefused` for a read the scope cannot answer."""
        if not isinstance(sql, str):
            return sql
        noise = _LEADING_NOISE.match(sql)
        body = sql[noise.end() if noise else 0 :]
        if not _READ_STATEMENT.match(body):
            return sql
        self._refuse(body)
        scoped = self._with_relations(sql, body, tables or {})
        if READ_CLOCK_SETTING not in body:
            return scoped
        # Every scope binds it (a scope without a clock binds '' = the
        # database clock), so a session reused within one transaction never
        # reads an instant another statement set.
        instant = self.clock.isoformat() if self.clock is not None else ""
        return f"SET LOCAL {READ_CLOCK_SETTING} = '{instant}'; {scoped}"

    def built_in(self, sql: str) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
        """The relations ``sql`` reads that are built once per session:
        ``(name, relation sql, bound columns)`` each. Empty for a statement
        that is no read, or names none."""
        if not self.built or not isinstance(sql, str):
            return ()
        noise = _LEADING_NOISE.match(sql)
        body = sql[noise.end() if noise else 0 :]
        if not _READ_STATEMENT.match(body):
            return ()
        bound = dict(self.built)
        return tuple(
            (name, relation, bound[name])
            for name, relation in self._wanted(body)
            if name in bound
        )

    def _wanted(self, body: str) -> list[tuple[str, str]]:
        """The scope's relations ``body`` reads and does not define itself."""
        if not self.relations:
            return []
        mentioned = _mentioned(body, self.names())
        if not mentioned:
            return []
        head = _WITH_HEAD.match(body)
        return [
            (name, relation)
            for name, relation in self.relations
            if name in mentioned and not (head and _defines(body, name))
        ]

    def _with_relations(self, sql: str, body: str, tables: Mapping[str, str]) -> str:
        wanted = self._wanted(body)
        if not wanted:
            return sql
        ctes = ", ".join(
            f'"{name}" AS NOT MATERIALIZED '
            f"({f'SELECT * FROM {tables[name]}' if name in tables else relation})"
            for name, relation in wanted
        )
        head = _WITH_HEAD.match(body)
        if head is None:
            return f"WITH {ctes} {body}"
        recursive = head.group(1) or ""
        return f"WITH{recursive} {ctes}, {body[head.end():]}"

    def _refuse(self, body: str) -> None:
        if self.clock is not None and _reads_clock(body):
            raise ReadScopeRefused(
                "a read inside a scope with a fixed clock reads the database clock "
                "(now() / CURRENT_*): bind the scope's clock (current_read_clock()) "
                f"instead: {body[:240]}"
            )
        if self.refused:
            named = _mentioned(body, self.refused)
            if named:
                raise ReadScopeRefused(
                    f"a read inside this scope names {sorted(named)}, which the scope "
                    f"refuses (it holds no state at the scope's instant): {body[:240]}"
                )


def _mentioned(sql: str, names: tuple[str, ...]) -> set[str]:
    """Which of ``names`` ``sql`` reads as an unqualified relation, in one
    pass. (A false positive only adds an unread ``WITH``; a qualified
    ``schema.name`` or a column ``alias.name`` is not a mention.)"""
    # Only the names the text holds at all reach the pattern (a substring
    # test is the cheap filter; the pattern decides what is a relation).
    candidates = tuple(name for name in names if name in sql)
    if not candidates:
        return set()
    return {
        match.group("bare") or match.group("quoted")
        for match in _mention_pattern(candidates).finditer(sql)
    }


@functools.lru_cache(maxsize=512)
def _mention_pattern(names: tuple[str, ...]) -> re.Pattern[str]:
    """One pattern for a scope's names (compiled once per set of names): a
    bare name not inside a longer word nor after ``.`` / ``"``, or the name
    quoted, not after ``.``. Longest first, so a name never shadows a longer
    one it begins."""
    alternatives = "|".join(re.escape(name) for name in sorted(names, key=len, reverse=True))
    return re.compile(
        rf'(?<![\w."])(?P<bare>{alternatives})(?![\w"])|(?<![\w.])"(?P<quoted>{alternatives})"'
    )


def _defines(sql: str, name: str) -> bool:
    """Does the statement's own ``WITH`` define ``name``?"""
    pattern = rf'(?:\bWITH(?:\s+RECURSIVE)?|,)\s*"?{re.escape(name)}"?\s+AS\b'
    return re.search(pattern, sql, re.IGNORECASE) is not None


_SCOPE: ContextVar[ReadScope | None] = ContextVar("cara_read_scope", default=None)


def current_read_scope() -> ReadScope | None:
    """The scope this context runs in, or None."""
    return _SCOPE.get()


def current_read_clock() -> datetime | None:
    """The instant "now" means in this scope, or None (the wall clock)."""
    scope = _SCOPE.get()
    return scope.clock if scope is not None else None


def read_now() -> datetime:
    """THE READ'S NOW: the scope's clock inside a clocked scope, the
    framework's clock (``pendulum.now("UTC")``, aware) everywhere else — the
    Python twin of :data:`READ_CLOCK_SQL`. Every relative-time computation of a
    read ("the last 30 days", an age, a deadline) takes its now here, so a read
    at T measures from T in Python as its SQL does. It is the clock every
    writer stamps with (model timestamps, ``DateManager.now``): a deadline
    stamped by ``pendulum.now`` and judged by another clock disagreed wherever
    that clock is moved (a frozen test clock, a simulation's virtual one)."""
    return current_read_clock() or pendulum.now("UTC")


def read_once(key: Any, compute: Callable[[], Any]) -> Any:
    """``compute()``'s value, computed ONCE for the ``read_snapshot()`` block
    this context reads in — for what a read derives from the database by
    several statements and more than one of its readers needs (a page's
    figures recomputed at the scope's instant). The value is kept under
    ``key`` and the scope it was computed in (its relations and its clock):
    another scope of the same block computes its own. Readers on other
    threads of the block wait for the first instead of computing again.
    Outside a block there is nothing to keep it in: ``compute()`` runs.

    ``compute`` must be a pure function of the database as the scope shows
    it, and its value must not be mutated by the readers that share it."""
    scope = _SCOPE.get()
    if scope is None or scope.snapshot is None:
        return compute()
    return scope.snapshot.once((key, scope.clock, scope.relations), compute)


_PLAIN_NAME = re.compile(r"[a-z_][a-z0-9_]*")


@contextlib.contextmanager
def read_scope(
    relations: Mapping[str, tuple[str, Sequence[Any]] | BuiltRelation] | None = None,
    *,
    clock: datetime | None = None,
    refuse: Sequence[str] = (),
) -> Iterator[ReadScope]:
    """Run the block read-only at the database, over ``relations``.

    ``relations`` maps an unqualified relation name to ``(sql, params)``: a
    SELECT whose columns are the named table's, with ``%s`` binds — or to a
    ``BuiltRelation`` of the same, derived once per session of the block's
    snapshot instead of by every statement (module docstring). ``clock``
    (aware) fixes what "now" means: read statements may not read the
    database clock. ``refuse`` names relations no read statement may name.
    Nesting keeps the outer scope's relations (the inner add or replace),
    its snapshot and its clock (unless the inner sets one), and refuses the
    union of both.
    """
    if clock is not None and clock.tzinfo is None:
        raise ValueError("read_scope: the clock must be an aware datetime")
    outer = _SCOPE.get()
    named = dict(outer.relations) if outer else {}
    built = dict(outer.built) if outer else {}
    for name, relation in (relations or {}).items():
        if not _PLAIN_NAME.fullmatch(name):
            raise ValueError(f"read_scope: {name!r} is not a plain relation name")
        # The inner scope's word replaces the outer's, its form included.
        built.pop(name, None)
        if isinstance(relation, BuiltRelation):
            for column in relation.bound:
                if not _PLAIN_NAME.fullmatch(column):
                    raise ValueError(f"read_scope: {column!r} is not a plain column name")
            built[name] = tuple(relation.bound)
            sql, params = relation.sql, relation.params
        else:
            sql, params = relation
        named[name] = render_literals(sql, params)
    for name in refuse:
        if not _PLAIN_NAME.fullmatch(name):
            raise ValueError(f"read_scope: {name!r} is not a plain relation name")
    refused = tuple(sorted({*(outer.refused if outer else ()), *refuse}))
    scope = ReadScope(
        tuple(sorted(named.items())),
        clock=clock if clock is not None else (outer.clock if outer else None),
        refused=refused,
        snapshot=outer.snapshot if outer else None,
        built=tuple(sorted(built.items())),
    )
    token = _SCOPE.set(scope)
    try:
        yield scope
    finally:
        _SCOPE.reset(token)


@contextlib.contextmanager
def read_snapshot() -> Iterator[ReadSnapshot]:
    """Run the block read-only at the database in ONE snapshot (module
    docstring, ``cara.eloquent.ReadSnapshot``). Inside a block that already
    has one, the block keeps it. The sessions end with the block."""
    outer = _SCOPE.get()
    if outer is not None and outer.snapshot is not None:
        yield outer.snapshot
        return
    snapshot = ReadSnapshot()
    token = _SCOPE.set(dataclasses.replace(outer or ReadScope(), snapshot=snapshot))
    try:
        yield snapshot
    finally:
        _SCOPE.reset(token)
        snapshot.close()


@contextlib.contextmanager
def read_only_statement(connection: Any) -> Iterator[None]:
    """Run one statement of a read scope read-only on ``connection`` (the
    driver connection). The transaction control runs on a cursor of its own,
    so the statement's cursor keeps its result: a client-side cursor holds
    every row once ``execute`` returns, before the ``ROLLBACK``."""
    cursor = connection.cursor()
    autocommit = bool(getattr(connection, "autocommit", False))
    if autocommit:
        cursor.execute("BEGIN TRANSACTION READ ONLY")
        undo = ("ROLLBACK",)
    else:
        cursor.execute(f"SAVEPOINT {_SAVEPOINT}")
        cursor.execute("SET LOCAL transaction_read_only = on")
        undo = (f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}", f"RELEASE SAVEPOINT {_SAVEPOINT}")
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        try:
            for statement in undo:
                if failed:
                    # The statement's own error is the one to see; a dead
                    # session cannot roll back and must not hide it.
                    with contextlib.suppress(Exception):
                        cursor.execute(statement)
                else:
                    cursor.execute(statement)
        finally:
            with contextlib.suppress(Exception):
                cursor.close()


__all__ = [
    "READ_CLOCK_BOUND_SQL",
    "READ_CLOCK_SETTING",
    "READ_CLOCK_SQL",
    "ReadScope",
    "current_read_clock",
    "current_read_scope",
    "read_now",
    "read_once",
    "read_only_statement",
    "read_scope",
    "read_snapshot",
    "render_literals",
]
