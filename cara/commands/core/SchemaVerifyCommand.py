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

import cara.schema.Scratch as Scratch
from cara.commands.CommandBase import CommandBase
from cara.configuration import config
from cara.decorators import command
from cara.support import base_path

#: Scratch names are interpolated into DDL as identifiers; keep them boring
#: instead of quoting our way around exotic ones.
_SAFE_DB_NAME = Scratch.SAFE_DB_NAME


def _derive_scratch_name(configured: str) -> str:
    """A boring scratch identifier derived from any configured name."""
    return Scratch.derive_name(configured, "verify")


@command(
    name="schema:verify",
    help=(
        "Prove the acceptance invariant: create a scratch database, run the "
        "generated migrations into it from zero, then schema:check the result "
        "against the models. Exit 0 = an empty database migrated from zero "
        "equals the models' schema. The scratch is dropped afterwards "
        "(--keep preserves it for autopsy). Development-only, Postgres-only."
    ),
    options=[
        {
            "name": "--keep",
            "help": "Keep the scratch database after the run (for autopsy)",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--database",
            "help": "Scratch database name (default: <configured>_verify)",
            "type": str,
            "default": None,
            "is_flag": False,
        },
    ],
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

        missing = self._missing_dependencies()
        if missing:
            self.error(
                f"This deployable does not carry {', '.join(missing)}, so the "
                f"invariant cannot be proved from here — the child process "
                f"would fail on a command that is not registered, and a run "
                f"that cannot execute the directory has no verdict to give.\n"
                f"   Run 'craft schema:verify' from the deployable that owns "
                f"the schema (the one with the migration commands). This is "
                f"about WHERE it was run, not about the migrations."
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

    #: The child commands this one is built out of. Not a doc comment — the
    #: pre-flight below reads it.
    REQUIRES = ("migrate", "schema:check")

    def _missing_dependencies(self) -> list[str]:
        """Which of :attr:`REQUIRES` this deployable's CLI does not register.

        A worker repository deliberately strips the commands that WRITE a
        schema, ``migrate`` among them — so ``schema:verify`` spawned there
        died on "No such command 'migrate'" and reported it as
        "the generated directory does not install from zero". That is a false
        accusation about the migration directory, produced by a command that
        could not find its own dependency, and it is exactly the failure this
        whole workflow exists to prevent: a tool that says something untrue
        with confidence.

        Asked of the runner rather than assumed from the deployable's name,
        because the strip list is the product's decision and differs between
        them — cheapa's services keeps ``migrate`` for its reset workflow,
        synkronus' does not.
        """
        try:
            console = self.application.make("commands").runner.console_app
            registered = {
                getattr(command, "name", None) for command in console.registered_commands
            }
        except Exception:
            # No registry to ask means no evidence of absence. Proceed and let
            # the child speak for itself rather than refusing on a guess.
            return []
        return [name for name in self.REQUIRES if name not in registered]

    def _connection_params(self) -> dict:
        """The default connection's parameters, or ValueError when unusable."""
        return Scratch.connection_params(config)

    def _admin_sql(self, params: dict, statements: list[str]) -> None:
        """Run maintenance DDL over an autocommit connection."""
        Scratch.admin_sql(params, statements)

    def _run_craft(self, arguments: list[str], scratch_database: str) -> int:
        """Run a craft subcommand in a child process aimed at the scratch."""
        return Scratch.run_craft(arguments, scratch_database, base_path())
