"""Migration tracker — knows what has run, persists new entries.

Connection discipline: every method that runs SQL borrows a fresh pool
slot via ``_get_connection`` and MUST release it through
``_release_connection`` in a ``finally``. Without that, a sweep of N
migrations leaks N pool slots and the runner hits ``pool_max`` after
~50 migrations (each migration calls ``record_migration``).
"""


def _release(connection) -> None:
    """Return a borrowed connection to the pool (best-effort)."""
    if connection is None:
        return
    try:
        close = getattr(connection, "close_connection", None)
        if callable(close):
            close()
    except Exception:
        # Cleanup must never mask the caller's primary error.
        pass


class MigrationTracker:
    """Single Responsibility: Tracks migration state in database."""

    def __init__(self, db_manager, table_name="migrations"):
        self.db_manager = db_manager
        self.table_name = table_name

    # ── Connection helpers ────────────────────────────────────────────
    def _get_connection(self):
        """Get database connection from manager."""
        return self.db_manager.create_connection_instance()

    def _get_driver_type(self):
        """Get the database driver type."""
        connection_info = self.db_manager.get_connection_info()
        return connection_info.get("driver", "sqlite")

    def _get_placeholder(self):
        """Get the correct placeholder for the database driver."""
        driver = self._get_driver_type()
        if driver in ["postgres", "postgresql"]:
            return "%s"
        else:  # sqlite, mysql
            return "?"

    # ── Schema bootstrap ──────────────────────────────────────────────
    def ensure_migrations_table(self):
        """Create migrations table if it doesn't exist."""
        if not self._table_has_correct_structure():
            connection = self._get_connection()
            try:
                try:
                    connection.query(f"DROP TABLE IF EXISTS {self.table_name}")
                except Exception:
                    pass
                self._create_migrations_table(connection)
            finally:
                _release(connection)

    def _table_exists(self):
        """Check if migrations table exists."""
        connection = self._get_connection()
        try:
            connection.query(f"SELECT 1 FROM {self.table_name} LIMIT 1")
            return True
        except Exception:
            return False
        finally:
            _release(connection)

    def _table_has_correct_structure(self):
        """Check if migrations table exists with correct structure."""
        connection = self._get_connection()
        try:
            connection.query(
                f"SELECT id, migration, batch FROM {self.table_name} LIMIT 1"
            )
            return True
        except Exception:
            return False
        finally:
            _release(connection)

    def _create_migrations_table(self, connection):
        """Create the migrations tracking table.

        Caller owns the connection lifecycle — do NOT release here.
        """
        driver = self._get_driver_type()
        if driver in ["postgres", "postgresql"]:
            sql = f"""
            CREATE TABLE {self.table_name} (
                id SERIAL PRIMARY KEY,
                migration VARCHAR(255) NOT NULL,
                batch INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        else:
            sql = f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration VARCHAR(255) NOT NULL,
                batch INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        connection.query(sql)

    # ── Read APIs ─────────────────────────────────────────────────────
    def get_ran_migrations(self):
        """Get list of migrations that have been run."""
        connection = self._get_connection()
        try:
            result = connection.query(
                f"SELECT migration FROM {self.table_name} ORDER BY batch, id"
            )
            migrations = []
            if result:
                for row in result:
                    if hasattr(row, "get"):  # dict-like (PostgreSQL)
                        migrations.append(row.get("migration"))
                    else:  # tuple-like (SQLite)
                        migrations.append(row[0])
            return migrations
        except Exception:
            return []
        finally:
            _release(connection)

    def get_last_batch_number(self):
        """Get the last batch number."""
        connection = self._get_connection()
        try:
            result = connection.query(f"SELECT MAX(batch) FROM {self.table_name}")
            if result and result[0]:
                row = result[0]
                if hasattr(row, "get"):
                    batch = row.get("max") or 0
                else:
                    batch = row[0] if row[0] is not None else 0
            else:
                batch = 0
            return batch
        except Exception:
            return 0
        finally:
            _release(connection)

    def get_migrations_by_batch(self, batch):
        """Get migrations from specific batch."""
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            sql = (
                f"SELECT migration FROM {self.table_name} "
                f"WHERE batch = {placeholder} ORDER BY id DESC"
            )
            result = connection.query(sql, (batch,))
            migrations = []
            if result:
                for row in result:
                    if hasattr(row, "get"):
                        migrations.append(row.get("migration"))
                    else:
                        migrations.append(row[0])
            return migrations
        finally:
            _release(connection)

    # ── Write APIs ────────────────────────────────────────────────────
    def record_migration(self, migration_name, batch):
        """Record that a migration has been run."""
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            sql = (
                f"INSERT INTO {self.table_name} (migration, batch) "
                f"VALUES ({placeholder}, {placeholder})"
            )
            connection.query(sql, (migration_name, batch))
        finally:
            _release(connection)

    def remove_migration(self, migration_name):
        """Remove migration record (for rollback)."""
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            sql = f"DELETE FROM {self.table_name} WHERE migration = {placeholder}"
            connection.query(sql, (migration_name,))
        finally:
            _release(connection)
