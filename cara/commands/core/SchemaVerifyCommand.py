"""``schema:verify`` — the acceptance invariant as one command.

The migration convention exists to protect exactly one provable statement:

    An empty database migrated from zero equals the models' schema.

``migrations:check`` audits the directory statically and bare
``make:migration`` reports model↔directory drift, but neither EXECUTES the
directory — a generated file can parse cleanly and still fail against a real
PostgreSQL (an index expression that isn't IMMUTABLE, a forward reference the
dependency order got wrong, a default the server rejects). Before this
command, proving the invariant meant hand-running three steps against a
scratch database and remembering the env override; a proof that requires
choreography stops being run.

``schema:verify`` is that choreography, owned:

1. DROP + CREATE a scratch database next to the configured one
   (``<database>_verify``), through a maintenance connection.
2. ``craft migrate`` with ``DB_DATABASE`` pointed at the scratch — the real
   executor, the real files, the real server.
3. ``craft schema:check`` against the same scratch — the full structured
   model↔database differ (columns, types, nullability, defaults, indexes,
   checks, timezone drift).
4. DROP the scratch (kept only under ``--keep``, for autopsy).

Exit status is the proof: 0 means the invariant holds; anything else is the
failing step's own exit code, with the scratch dropped either way.

The subprocess boundary is deliberate. ``migrate`` and ``schema:check`` read
their connection at boot; re-pointing them inside one process would mean
mutating live configuration and trusting every cached connection to notice.
A child process with ``DB_DATABASE`` overridden is the product's own
documented contract for selecting a database, so the verify exercises the
exact wiring production boot uses.

Postgres only, and refused in production — the invariant is a statement about
the repository, and the place to prove it is a disposable database.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

from cara.commands.CommandBase import CommandBase
from cara.configuration import config
from cara.decorators import command
from cara.support import base_path

#: Scratch names are interpolated into DDL as identifiers; keep them boring
#: instead of quoting our way around exotic ones.
_SAFE_DB_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def _derive_scratch_name(configured: str) -> str:
    """A boring scratch identifier derived from any configured name.

    The CONFIGURED name is never interpolated into DDL here, so it may be as
    exotic as the operator likes (``synkronus.io`` is real); only the scratch
    must satisfy :data:`_SAFE_DB_NAME`. Squash everything else to ``_`` and
    guarantee a leading letter — an explicit ``--database`` still has to pass
    the safety check on its own.
    """
    plain = re.sub(r"[^a-z0-9_]", "_", configured.lower())
    if not re.match(r"^[a-z]", plain):
        plain = f"v_{plain}"
    return f"{plain}_verify"


@command(
    name="schema:verify",
    help=(
        "Prove the acceptance invariant: create a scratch database, run the "
        "generated migrations into it from zero, then schema:check the result "
        "against the models. Exit 0 = an empty database migrated from zero "
        "equals the models' schema. The scratch is dropped afterwards "
        "(--keep preserves it for autopsy). Development-only, Postgres-only."
    ),
    options={
        "--keep": "Keep the scratch database after the run (for autopsy)",
        "--database=?": "Scratch database name (default: <configured>_verify)",
    },
)
class SchemaVerifyCommand(CommandBase):
    def handle(self):
        """Create scratch → migrate from zero → schema:check → drop."""
        if (config("app.env", "") or "").lower() in ("production", "prod"):
            self.error(
                "Refusing to run schema:verify in production. The invariant is "
                "proved against a disposable scratch database in development; "
                "production schema state is the evolve workflow's concern."
            )
            return 2

        try:
            params = self._connection_params()
        except ValueError as exc:
            self.error(str(exc))
            return 2

        scratch = self.option("database") or _derive_scratch_name(params["database"])
        if scratch == params["database"]:
            self.error(
                f"Scratch database '{scratch}' is the configured database "
                f"itself — refusing to drop it."
            )
            return 2
        if not _SAFE_DB_NAME.match(scratch):
            self.error(
                f"Scratch database name '{scratch}' is not a plain lowercase "
                f"identifier — refusing to interpolate it into DDL."
            )
            return 2

        self.info(f"Scratch database: {scratch}")
        try:
            self._admin_sql(
                params,
                [
                    f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)',
                    f'CREATE DATABASE "{scratch}"',
                ],
            )
        except Exception as exc:
            self.error(f"Could not create the scratch database: {exc}")
            return 2

        try:
            self.info("Running the generated migrations from zero...")
            code = self._run_craft(["migrate"], scratch)
            if code:
                self.error(
                    "migrate failed against an EMPTY database — the generated "
                    "directory does not install from zero. That is the "
                    "invariant broken at step one; the failing migration is in "
                    "the output above."
                )
                return code

            self.info("Comparing the migrated scratch against the models...")
            code = self._run_craft(["schema:check"], scratch)
            if code:
                self.error(
                    "The migrated-from-zero schema differs from the models. "
                    "The drift listed above is what the generated directory "
                    "fails to reproduce — regenerate with 'make:migration "
                    "--overwrite' and re-run."
                )
                return code
        finally:
            if self.option("keep"):
                self.warning(
                    f"--keep: scratch database '{scratch}' left in place. "
                    f"Drop it yourself when done."
                )
            else:
                try:
                    self._admin_sql(
                        params, [f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)']
                    )
                except Exception as exc:
                    self.warning(
                        f"Could not drop scratch database '{scratch}': {exc} — "
                        f"drop it manually."
                    )

        self.success(
            "Invariant holds: an empty database migrated from zero equals the "
            "models' schema."
        )
        return 0

    # ── seams (unit tests replace these; nothing else may talk to the world) ─

    def _connection_params(self) -> dict:
        """The default connection's parameters, or ValueError when unusable."""
        name = config("database.default", "app")
        drivers = config("database.drivers", {}) or {}
        params = dict(drivers.get(name) or {})
        if (params.get("driver") or "") != "postgres":
            raise ValueError(
                f"schema:verify supports the postgres driver only; connection "
                f"'{name}' declares '{params.get('driver') or 'nothing'}'."
            )
        if not params.get("database"):
            raise ValueError(
                f"Connection '{name}' has no database configured — nothing to "
                f"derive a scratch name from."
            )
        return params

    def _admin_sql(self, params: dict, statements: list[str]) -> None:
        """Run maintenance DDL over an autocommit connection.

        ``CREATE DATABASE`` / ``DROP DATABASE`` cannot run inside a
        transaction block, and neither can they target the database being
        dropped — so this connects to the server's ``postgres`` maintenance
        database with the same credentials the app uses.
        """
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
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
        finally:
            connection.close()

    def _run_craft(self, arguments: list[str], scratch_database: str) -> int:
        """Run a craft subcommand in a child process aimed at the scratch.

        The child inherits the parent's interpreter (the project venv) and
        environment, with only ``DB_DATABASE`` overridden — the same knob the
        products document for selecting a database, so the verify exercises
        the exact configuration path production boot uses.
        """
        env = {**os.environ, "DB_DATABASE": scratch_database}
        return subprocess.call(
            [sys.executable, "craft", *arguments],
            cwd=base_path(),
            env=env,
        )
