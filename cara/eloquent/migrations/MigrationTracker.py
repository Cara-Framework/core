class MigrationTracker:
    """Single Responsibility: Tracks migration state in database"""

    def __init__(self, db_manager, table_name="migrations"):
        self.db_manager = db_manager
        self.table_name = table_name

    def ensure_migrations_table(self):
        """Create migrations table if it doesn't exist"""
        connection = self._get_connection()

        # Check if table exists with correct structure
        if not self._table_has_correct_structure():
            # Drop and recreate table to ensure correct structure
            try:
                connection.query(f"DROP TABLE IF EXISTS {self.table_name}")
            except:
                pass

            self._create_migrations_table(connection)

    def _get_connection(self):
        """Get database connection from manager"""
        # Use default connection - consistent with Migration class pattern
        return self.db_manager.create_connection_instance()

    def _get_driver_type(self):
        """Get the database driver type"""
        connection_info = self.db_manager.get_connection_info()
        return connection_info.get("driver", "sqlite")

    def _get_placeholder(self):
        """Get the correct placeholder for the database driver"""
        driver = self._get_driver_type()
        if driver in ["postgres", "postgresql"]:
            return "%s"
        else:  # sqlite, mysql
            return "?"

    def _table_exists(self):
        """Check if migrations table exists"""
        connection = self._get_connection()

        try:
            result = connection.query(f"SELECT 1 FROM {self.table_name} LIMIT 1")
            return True
        except:
            return False

    def _create_migrations_table(self, connection):
        """Create the migrations tracking table"""
        driver = self._get_driver_type()

        if driver in ["postgres", "postgresql"]:
            # PostgreSQL syntax
            sql = f"""
            CREATE TABLE {self.table_name} (
                id SERIAL PRIMARY KEY,
                migration VARCHAR(255) NOT NULL,
                batch INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        else:
            # SQLite/MySQL syntax
            sql = f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration VARCHAR(255) NOT NULL,
                batch INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """

        connection.query(sql)

    def get_ran_migrations(self):
        """Get list of migrations that have been run"""
        connection = self._get_connection()

        try:
            result = connection.query(
                f"SELECT migration FROM {self.table_name} ORDER BY batch, id"
            )
            migrations = []
            if result:
                for row in result:
                    # Handle both dict-like (PostgreSQL) and tuple-like (SQLite) results
                    if hasattr(row, "get"):  # dict-like
                        migrations.append(row.get("migration"))
                    else:  # tuple-like
                        migrations.append(row[0])
            return migrations
        except Exception:
            return []

    def record_migration(self, migration_name, batch):
        """Record that a migration has been run"""
        connection = self._get_connection()
        placeholder = self._get_placeholder()

        sql = f"INSERT INTO {self.table_name} (migration, batch) VALUES ({placeholder}, {placeholder})"
        connection.query(sql, (migration_name, batch))

    def remove_migration(self, migration_name):
        """Remove migration record (for rollback)"""
        connection = self._get_connection()
        placeholder = self._get_placeholder()

        sql = f"DELETE FROM {self.table_name} WHERE migration = {placeholder}"
        connection.query(sql, (migration_name,))

    def get_last_batch_number(self):
        """Get the last batch number"""
        connection = self._get_connection()

        try:
            result = connection.query(f"SELECT MAX(batch) FROM {self.table_name}")

            if result and result[0]:
                # Handle both dict-like (PostgreSQL) and tuple-like (SQLite) results
                row = result[0]
                if hasattr(row, "get"):  # dict-like
                    batch = row.get("max") or 0
                else:  # tuple-like
                    batch = row[0] if row[0] is not None else 0
            else:
                batch = 0
            return batch
        except Exception:
            return 0

    def get_migrations_by_batch(self, batch):
        """Get migrations from specific batch"""
        connection = self._get_connection()
        placeholder = self._get_placeholder()

        sql = f"SELECT migration FROM {self.table_name} WHERE batch = {placeholder} ORDER BY id DESC"
        result = connection.query(sql, (batch,))
        migrations = []
        if result:
            for row in result:
                # Handle both dict-like (PostgreSQL) and tuple-like (SQLite) results
                if hasattr(row, "get"):  # dict-like
                    migrations.append(row.get("migration"))
                else:  # tuple-like
                    migrations.append(row[0])
        return migrations

    def _table_has_correct_structure(self):
        """Check if migrations table exists with correct structure"""
        connection = self._get_connection()

        try:
            # Try to query with the expected columns
            result = connection.query(
                f"SELECT id, migration, batch FROM {self.table_name} LIMIT 1"
            )
            return True
        except:
            return False
