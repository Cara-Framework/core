"""A disposable database next to a real one, and how to fill it.

Two commands need the same thing for different reasons. ``schema:verify``
proves the acceptance invariant by migrating an EMPTY scratch from zero;
``schema:plan --rehearse`` proves a derived plan executes by running it
against a scratch holding the deployed SHAPE. Both create a database beside
the configured one, run a child ``craft`` aimed at it, and drop it afterwards.
Written twice, the two would drift on exactly the details that make it safe —
the name check, the maintenance connection, the drop in a ``finally``.

The safety rules are here rather than in either caller:

* The scratch name must be a plain lowercase identifier, because it is
  interpolated into ``CREATE DATABASE`` as an identifier. The CONFIGURED name
  never is, so it may be as exotic as the operator likes (``synkronus.io`` is
  real) — only the derived or supplied scratch must be boring.
* A scratch equal to the configured database is refused. Everything here ends
  in ``DROP DATABASE``, and the one mistake that must be impossible is
  dropping the database being examined.
* ``CREATE``/``DROP DATABASE`` cannot run inside a transaction, nor from
  within the database being dropped, so they go through an autocommit
  connection to the server's ``postgres`` maintenance database using the
  application's own credentials.

The child-process boundary is deliberate too. ``migrate``, ``schema:check``
and ``schema:apply`` read their connection at boot; re-pointing them inside
one process would mean mutating live configuration and trusting every cached
connection to notice. A child with ``DB_DATABASE`` overridden is the
product's own documented way to select a database, so what runs against the
scratch exercises the exact wiring production boot uses.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from cara.configuration import config
from cara.exceptions import ScratchDatabaseException

#: Scratch names are interpolated into DDL as identifiers; keep them boring
#: instead of quoting our way around exotic ones.
SAFE_DB_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
#: NAMEDATALEN - 1: the longest identifier the server stores uncut.
_MAX_IDENTIFIER_BYTES = 63


def derive_name(configured: str, suffix: str) -> str:
    """A boring scratch identifier derived from any configured name."""
    plain = re.sub(r"[^a-z0-9_]", "_", configured.lower())
    if not re.match(r"^[a-z]", plain):
        plain = f"v_{plain}"
    return f"{plain}_{suffix}"


def connection_params(config) -> dict:
    """The default connection's parameters, or ValueError when unusable."""
    name = config("database.default", "app")
    drivers = config("database.drivers", {}) or {}
    params = dict(drivers.get(name) or {})
    if (params.get("driver") or "") != "postgres":
        raise ValueError(
            f"A scratch database requires the postgres driver; connection "
            f"'{name}' declares '{params.get('driver') or 'nothing'}'."
        )
    if not params.get("database"):
        raise ValueError(
            f"Connection '{name}' has no database configured — nothing to "
            f"derive a scratch name from."
        )
    return params


def validate_name(scratch: str, configured: str) -> None:
    """Raise unless ``scratch`` is safe to create and, later, to drop."""
    if scratch == configured:
        raise ScratchDatabaseException(
            f"Scratch database '{scratch}' is the configured database itself "
            f"— refusing to drop it."
        )
    if not SAFE_DB_NAME.match(scratch):
        raise ScratchDatabaseException(
            f"Scratch database name '{scratch}' is not a plain lowercase "
            f"identifier — refusing to interpolate it into DDL."
        )
    # The server cuts an identifier to 63 bytes, so a scratch derived from a
    # 63-byte configured name plus a suffix IS the configured name once
    # stored — and everything here ends in DROP DATABASE.
    if len(scratch.encode()) > _MAX_IDENTIFIER_BYTES:
        raise ScratchDatabaseException(
            f"Scratch database name '{scratch}' is longer than "
            f"{_MAX_IDENTIFIER_BYTES} bytes; PostgreSQL would store it cut "
            f"short, possibly as another database's name. Name a shorter one."
        )


def admin_sql(params: dict, statements: list[str]) -> None:
    """Run maintenance DDL over an autocommit maintenance connection."""
    connection = admin_connection(params)
    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
    finally:
        connection.close()


def admin_rows(params: dict, query: str, arguments: tuple = ()) -> list[tuple]:
    """Read the server catalog (``pg_database``, ``pg_stat_activity``) over the
    same maintenance connection the DDL uses."""
    connection = admin_connection(params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, arguments)
            return list(cursor.fetchall())
    finally:
        connection.close()


def migrations_fingerprint(directory: str | os.PathLike) -> str:
    """What a database migrated from zero out of ``directory`` is a function
    of: the directory's files and the executor and schema builder that run
    them. Equal fingerprints mean byte-identical inputs, so a database built
    from one stands in for a fresh migration from the other. Stdlib only and
    cheap (no boot, no model import), so a test process can check a template
    before cloning it."""
    digest = hashlib.sha256()
    eloquent = Path(__file__).resolve().parents[1] / "eloquent"
    roots = [
        (Path(directory), (".py", ".sql")),
        (eloquent / "migrations", (".py",)),
        (eloquent / "schema", (".py",)),
    ]
    for root, suffixes in roots:
        for path in sorted(
            p for p in root.rglob("*") if p.is_file() and p.suffix in suffixes
        ):
            if "__pycache__" in path.parts:
                continue
            digest.update(str(path.relative_to(root)).encode() + b"\0")
            digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()[:24]


@contextmanager
def advisory_lock(params: dict, key: int) -> Iterator[None]:
    """Hold the server-wide ``pg_advisory_lock(key)`` for the block, so two
    processes building the same scratch state build it once."""
    connection = admin_connection(params)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (key,))
        yield
    finally:
        connection.close()  # a session-level lock ends with its session


def admin_connection(params: dict):
    """An autocommit connection to the server's ``postgres`` maintenance
    database with the application's own credentials. The caller closes it;
    a session-level advisory lock taken on it lives exactly as long."""
    import psycopg2  # local: heavy optional dep

    options = params.get("options") or {}
    connect_kwargs = {
        "host": params.get("host"),
        "port": params.get("port"),
        "user": params.get("user"),
        "password": params.get("password"),
        "dbname": "postgres",
    }
    if options.get("sslmode"):
        connect_kwargs["sslmode"] = options["sslmode"]

    connection = psycopg2.connect(**connect_kwargs)
    connection.autocommit = True
    return connection


def recreate(params: dict, scratch: str) -> None:
    """Drop and create the scratch, so a leftover cannot be mistaken for it."""
    admin_sql(
        params,
        [
            f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)',
            f'CREATE DATABASE "{scratch}"',
        ],
    )


def drop(params: dict, scratch: str) -> None:
    admin_sql(params, [f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)'])


def run_craft(arguments: list[str], scratch: str, cwd: str) -> int:
    """Run a craft subcommand in a child process aimed at the scratch."""
    env = {**os.environ, "DB_DATABASE": scratch}
    return subprocess.call([sys.executable, "craft", *arguments], cwd=cwd, env=env)


def _resolve_client_binary(name: str, config_key: str) -> str | None:
    """The PostgreSQL client binary to shell out to.

    An explicit override wins over PATH, because the host that most needs a
    rehearsal is the one whose client tools live outside PATH or whose PATH
    resolves to a client older than the server. The products declare
    ``PSQL_BIN`` / ``PG_DUMP_BIN`` for exactly this and nothing read them, so
    an operator following their own config watched the setting change
    nothing. An override that does not resolve to an executable is a typo,
    not a fallback: silently reverting to PATH would run a DIFFERENT binary
    than the operator named.
    """
    try:
        override = config(config_key, None)
    except Exception:  # a rehearsal must not need a booted config
        override = None
    if override:
        resolved = shutil.which(str(override))
        if resolved is None:
            raise ScratchDatabaseException(
                f"{config_key} points at '{override}', which is not an "
                f"executable. Fix the path or unset it to use PATH's {name}."
            )
        return resolved
    return shutil.which(name)


def client_binaries() -> tuple[str, str]:
    """``(pg_dump, psql)`` for a structure clone, or a refusal naming what is missing.

    Checked BEFORE a rehearsal creates its scratch, so a host or image without
    the client tools is told which binary it lacks and where to point the
    override, instead of a scratch database being created and dropped for a
    clone that could never run.
    """
    found: dict[str, str | None] = {
        "pg_dump": _resolve_client_binary("pg_dump", "database.pg_dump_bin"),
        "psql": _resolve_client_binary("psql", "database.psql_bin"),
    }
    missing = [name for name, path in found.items() if not path]
    if missing:
        knobs = " / ".join(f"{name.upper()}_BIN" for name in missing)
        raise ScratchDatabaseException(
            f"A structure clone needs {' and '.join(repr(m) for m in missing)}, "
            "not found on PATH; they are how Postgres describes its own schema. "
            "Install the PostgreSQL client tools of the server's major version, "
            f"set {knobs} to their absolute paths, or run the plan without "
            "--rehearse."
        )
    return str(found["pg_dump"]), str(found["psql"])


def clone_structure(params: dict, source: str, scratch: str) -> None:
    """Copy ``source``'s SHAPE into ``scratch`` — no rows, ever.

    ``pg_dump --schema-only`` rather than a re-render of the introspected
    schema, for the same reason a new table is created by running its own
    generated migration: one renderer per kind of truth. Postgres describes its
    own schema better than a reconstruction from ``information_schema`` could,
    including the objects a reconstruction quietly forgets — partial index
    predicates, trigger bodies, collations.

    Rows are excluded on purpose, and that is the boundary between the two
    checks. A rehearsal answers *does this plan EXECUTE against this shape*;
    whether it succeeds against the rows is preflight's question, asked against
    production itself where the rows actually are. A rehearsal that copied
    production data to answer it would be a far larger promise — and a copy of
    production in a scratch database nobody is watching.
    """
    pg_dump, psql = client_binaries()

    env = {**os.environ}
    if params.get("password"):
        env["PGPASSWORD"] = str(params["password"])
    connection_flags = [
        "--host",
        str(params.get("host") or "localhost"),
        "--port",
        str(params.get("port") or 5432),
        "--username",
        str(params.get("user") or ""),
        "--no-password",
    ]

    dumped = subprocess.run(
        [
            pg_dump,
            *connection_flags,
            "--schema-only",
            "--no-owner",
            "--no-privileges",
            "--dbname",
            source,
        ],
        capture_output=True,
        env=env,
    )
    if dumped.returncode != 0:
        raise ScratchDatabaseException(
            f"pg_dump could not read the schema of '{source}': "
            f"{dumped.stderr.decode(errors='replace').strip()}"
        )

    loaded = subprocess.run(
        [
            psql,
            *connection_flags,
            "--quiet",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--dbname",
            scratch,
        ],
        input=dumped.stdout,
        capture_output=True,
        env=env,
    )
    if loaded.returncode != 0:
        raise ScratchDatabaseException(
            f"Loading the dumped schema into '{scratch}' failed: "
            f"{loaded.stderr.decode(errors='replace').strip()}"
        )


__all__ = [
    "SAFE_DB_NAME",
    "admin_connection",
    "admin_rows",
    "admin_sql",
    "advisory_lock",
    "clone_structure",
    "connection_params",
    "derive_name",
    "drop",
    "migrations_fingerprint",
    "recreate",
    "run_craft",
    "validate_name",
]
