from __future__ import annotations

import contextlib

from cara.commands import CommandBase, missing_optional
from cara.decorators import command
from cara.support import paths


@command(
    name="migrate",
    help="Run the database migrations.",
    options={
        "--m|migration=all": "Migration's name to be migrated",
        "--c|connection=default": "The connection you want to run migrations on",
        "--f|force": "Force migrations without prompt in production",
        "--show": "Show SQL output without executing migrations",
        "--schema=?": "Sets the schema to be migrated",
        "--d|directory=?": "The location of the migration directory",
    },
)
class MigrateCommand(CommandBase):
    def handle(self):
        """Execute database migrations with enhanced UX."""
        # Lazy DB import: ``cara.eloquent`` pulls psycopg2/faker (the optional
        # 'db' extra). Defer it to run time so a DB-less service can still
        # import this command module — and fail LOUD here, not at module load.
        global Migration
        try:
            from cara.eloquent.migrations import Migration
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        self.info("Starting database migration...")

        # Check for production environment
        if (
            self._is_production()
            and not self.option("force")
            and not self._confirm_production()
        ):
            self.info("× Migration aborted by user.")
            return

        try:
            # Get migration manager
            migration_manager = self._build_migration()

            self.info("Checking migration table...")

            # Ensure migrations table exists
            migration_manager.create_table_if_not_exists()

            # Check for pending migrations
            pending = migration_manager.get_unran_migrations()
            if not pending:
                self.success("No pending migrations. Database is up to date!")
                return

            # Show what will be migrated
            self._show_pending_migrations(pending)

            if self.option("show"):
                self.info("SQL Preview Mode - No changes will be made:")
                self._show_sql_preview(migration_manager, pending)
                return

            # Run migrations
            self.info("Running migrations...")
            name = self.option("migration") or "all"
            migration_manager.migrate(name, output=True)

            self.success("All migrations completed successfully!")

        except Exception as e:
            self.error(f"× Migration failed: {str(e)}")
            self.error("Try running with --show to see the SQL that would be executed")
            # Fail-fast: a failed migration MUST exit non-zero so callers (CI,
            # the coordinated regen+reset `&&` chain) stop instead of marching
            # on against a half-built schema. The runner maps this return into
            # ``typer.Exit(code=1)``; the previous bare fall-through returned
            # None → exit 0, which silently masked a mid-migration failure.
            return 1

    def _confirm_production(self) -> bool:
        """Confirm migration in production environment."""
        self.warning("⚠ You are about to run migrations in PRODUCTION!")
        self.warning("   This operation may modify your database schema.")
        self.warning("   Make sure you have a backup before proceeding.")
        return self._confirm_yes_no()

    def _build_migration(self) -> Migration:
        """Build migration instance with proper configuration."""
        connection = self.option("connection") or "default"
        directory = self.option("directory") or paths("migrations")
        schema = self.option("schema")
        show_mode = self.option("show")

        self._show_configuration(connection, directory, schema, show_mode)

        return Migration(
            command_class=self,
            connection=connection,
            migration_directory=directory,
            schema=schema,
            dry=show_mode,
        )

    def _show_configuration(
        self,
        connection: str,
        directory: str,
        schema: str | None = None,
        show_mode: bool = False,
    ):
        """Display migration configuration."""
        self.info("Configuration:")
        self.info(f"   Connection: {connection}")
        self.info(f"   Directory: {directory}")
        if schema:
            self.info(f"   Schema: {schema}")
        if show_mode:
            self.info("   Mode: DRY RUN (no changes will be made)")

    def _show_pending_migrations(self, migrations: list):
        """Display pending migrations list."""
        self.info(f"Found {len(migrations)} pending migration(s):")

        # Show first 5 migrations
        display_count = min(5, len(migrations))
        for mig_path in migrations[:display_count]:
            # Extract just the filename from the full path
            mig_name = mig_path.split("/")[-1].replace(".py", "")
            self.info(f"   • {mig_name}")

        # Show remaining count if more than 5
        if len(migrations) > 5:
            self.info(f"   ... and {len(migrations) - 5} more")

    def _show_migration_preview(self, migration_files: list):
        """Show simplified migration preview."""
        self.info("SQL that would be executed:")
        self.info("=" * 60)

        for i, file_path in enumerate(migration_files, 1):
            migration_name = file_path.split("/")[-1].replace(".py", "")
            self.info(f"\n{i}. Migration: {migration_name}")
            self.info("-" * 40)
            self.info(
                f"-- Would execute: {migration_name} (CREATE/ALTER TABLE operations)"
            )

        self.info("\n" + "=" * 60)
        self.info("No actual changes were made to the database.")

    def _show_sql_preview(self, migration_manager: Migration, pending: list):
        """Print the SQL each pending migration WOULD run, without touching the DB.

        Each migration is loaded and instantiated in DRY mode: the base
        ``Migration.__init__`` builds ``self.schema = Schema(dry=True, ...)``
        so every schema statement the migration's ``up()`` produces is
        *compiled* and routed through ``SchemaQueryExecutor.execute_query``,
        which — when ``dry`` is set — STORES the SQL and returns it instead of
        opening a connection / executing. No DB writes happen.

        ``SchemaQueryExecutor`` only retains the *last* statement in ``_sql``,
        so we wrap the executor's ``execute_query`` / ``get_query_result`` to
        record every statement in order, giving a complete per-migration
        preview (a single CREATE TABLE migration emits the table DDL plus its
        index/constraint statements).
        """
        connection = self.option("connection") or "default"
        directory = self.option("directory") or paths("migrations")
        schema = self.option("schema")

        self.info("SQL that would be executed:")
        self.info("=" * 60)

        for index, file_path in enumerate(pending, 1):
            migration_name = migration_manager.file_manager.get_migration_name_from_file(
                file_path
            )
            self.info(f"\n{index}. Migration: {migration_name}")
            self.info("-" * 40)

            statements = self._collect_migration_sql(
                migration_manager,
                file_path,
                connection=connection,
                directory=directory,
                schema=schema,
            )

            if statements:
                for sql in statements:
                    # Terminate each statement so the preview reads as a
                    # runnable script; the dry executor stores them bare.
                    self.line(f"{sql.rstrip(';')};")
            else:
                self.info("-- (no SQL statements compiled for this migration)")

        self.info("\n" + "=" * 60)
        self.info("No actual changes were made to the database.")

    def _collect_migration_sql(
        self,
        migration_manager: Migration,
        file_path: str,
        connection: str,
        directory: str | None,
        schema: str | None,
    ) -> list[str]:
        """Build a single migration in dry mode and collect the SQL it emits.

        Returns the ordered list of SQL statements ``up()`` would execute.
        Never opens a write connection — the dry executor short-circuits
        before touching the pool.
        """
        statements: list[str] = []

        try:
            migration_class = migration_manager.file_manager.load_migration_class(
                file_path
            )
            # Instantiate in dry mode: this wires ``self.schema`` to a
            # ``Schema(dry=True)`` whose executor collects (never runs) SQL.
            migration_instance = migration_class(
                connection=connection,
                dry=True,
                command_class=self,
                migration_directory=directory,
                schema=schema,
            )

            self._install_sql_recorder(migration_instance.schema, statements)
            # Some migrations bypass ``self.schema`` and run raw SQL straight
            # through the ``DB`` facade (``DB.statement(...)``), which has NO
            # dry-run awareness and would EXECUTE against the live database
            # during a preview. Guard the facade's mutating methods for the
            # duration of ``up()`` so they record + skip instead of running.
            restore_db = self._install_db_facade_guard(statements)
            try:
                migration_instance.up()
            finally:
                restore_db()
        except Exception as exc:  # noqa: BLE001 — preview must not abort the loop
            self.warning(f"   Could not compile SQL for this migration: {exc}")

        return statements

    @staticmethod
    def _install_db_facade_guard(sink: list[str]):
        """Patch the bound ``DB`` manager so raw SQL is recorded, not executed.

        Migrations that call ``DB.statement(...)`` / ``DB.select(...)`` resolve
        to the singleton ``DatabaseManager``. During a dry preview we must NOT
        touch the database, so we temporarily replace its mutating methods with
        recorders that append the SQL to ``sink`` and return a benign value.
        Returns a ``restore()`` callable that puts the originals back — always
        call it in a ``finally``.
        """
        try:
            from cara.eloquent.DatabaseManager import DatabaseManager

            db = DatabaseManager.get_instance()
        except Exception:  # noqa: BLE001 — no DB manager → nothing to guard
            return lambda: None

        guarded = ("statement", "select", "select_one")
        originals = {name: getattr(db, name, None) for name in guarded}

        def _record(sql) -> None:
            if isinstance(sql, (list, tuple)):
                for one in sql:
                    if one and str(one).strip():
                        sink.append(str(one).strip())
            elif sql and str(sql).strip():
                sink.append(str(sql).strip())

        def _make_recorder(returns):
            def _recorder(query, bindings=(), connection=None):
                _record(query)
                return returns
            return _recorder

        # statement → bool-ish; select → rows; select_one → row.
        db.statement = _make_recorder(True)
        db.select = _make_recorder([])
        db.select_one = _make_recorder(None)

        def restore() -> None:
            for name, original in originals.items():
                if original is not None:
                    setattr(db, name, original)
                else:
                    # Method was instance-shadowed; drop the shadow so the
                    # class method shows through again.
                    with contextlib.suppress(AttributeError):
                        delattr(db, name)

        return restore

    @staticmethod
    def _install_sql_recorder(schema, sink: list[str]) -> None:
        """Wrap a dry Schema's query executor so every statement lands in ``sink``.

        The wrapper delegates to the real (dry) methods — which only store +
        return the SQL — and appends each statement to ``sink`` in order, so
        all statements are captured (the executor itself keeps only the last).
        """
        executor = schema.query_executor
        original_execute = executor.execute_query
        original_get_result = executor.get_query_result

        def _record(sql) -> None:
            if isinstance(sql, (list, tuple)):
                for one in sql:
                    if one and str(one).strip():
                        sink.append(str(one).strip())
            elif sql and str(sql).strip():
                sink.append(str(sql).strip())

        def execute_query(sql, bindings=()):
            result = original_execute(sql, bindings)
            _record(sql)
            return result

        def get_query_result(sql, bindings=()):
            result = original_get_result(sql, bindings)
            _record(sql)
            return result

        executor.execute_query = execute_query
        executor.get_query_result = get_query_result
