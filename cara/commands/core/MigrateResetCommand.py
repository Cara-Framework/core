"""
Database Reset Command for the Cara framework.

This module provides a CLI command to completely reset the database schema.
"""

import os
from typing import List

from cara.commands import CommandBase
from cara.decorators import command
from cara.eloquent.DatabaseManager import get_database_manager


@command(
    name="migrate:reset",
    help="Drop all tables and reset the database schema completely.",
    options={
        "--c|connection=default": "The connection key from config to run reset on",
        "--f|force": "Force reset without prompt in production",
        "--schema=public": "Sets the schema to be reset (PostgreSQL only)",
        "--confirm": "Additional confirmation flag for safety",
    },
)
class MigrateResetCommand(CommandBase):
    """Database reset command with enhanced safety and database-specific operations."""

    def handle(self):
        """Main command handler."""
        self._display_warning()

        if self._should_block_execution():
            return

        if not self._get_user_confirmation():
            self.info("❌ Reset operation cancelled by user.")
            return

        config = self._get_configuration()
        self._display_configuration(config)

        try:
            self._execute_reset(config)
            self._display_success_message()
        except Exception as e:
            self._handle_error(e)

    def _display_warning(self) -> None:
        """Display initial warning."""
        self.warning("💥 DATABASE RESET - DESTRUCTIVE OPERATION")
        self.warning("   This will DROP ALL TABLES and DATA in your database!")
        self.warning("   This operation is IRREVERSIBLE!")

    def _should_block_execution(self) -> bool:
        """Check if execution should be blocked."""
        if self._is_production():
            self.error("❌ migrate:reset is DISABLED in production environment!")
            self.error("   This command is too dangerous for production use.")
            self.error("   Use migrate:rollback instead for safer rollbacks.")
            return True
        return False

    def _get_user_confirmation(self) -> bool:
        """Get user confirmation."""
        print("\n⚠️  FINAL WARNING:")
        print("   • ALL TABLES will be DROPPED")
        print("   • ALL DATA will be LOST")
        print("   • This action is IRREVERSIBLE")
        print("   • Make sure you have backups!")

        while True:
            answer = (
                input("\n🤔 Are you sure you want to continue? (yes/no): ")
                .strip()
                .lower()
            )
            if answer in ["yes", "y"]:
                return True
            elif answer in ["no", "n"]:
                return False
            else:
                print("Please answer 'yes' or 'no'")

    def _get_configuration(self) -> dict:
        """Get command configuration."""
        return {
            "connection_name": self.option("connection") or "default",
            "schema": self.option("schema") or "public",
        }

    def _display_configuration(self, config: dict) -> None:
        """Display configuration."""
        self.info("🔧 Configuration:")
        self.info(f"   Connection: {config['connection_name']}")
        self.info(f"   Schema: {config['schema']}")

    def _execute_reset(self, config: dict) -> None:
        """Execute the database reset."""
        connection = self._get_database_connection(config["connection_name"])
        db_type = self._get_database_type(connection)

        self.info(f"🗄️  Database: {db_type}")
        self.info("⚡ Executing reset operation...")

        if "postgres" in db_type.lower():
            self._reset_postgresql(connection, config["schema"])
        elif "mysql" in db_type.lower():
            self._reset_mysql(connection)
        elif "sqlite" in db_type.lower():
            self._reset_sqlite(connection)
        else:
            raise Exception(f"Unsupported database type: {db_type}")

    def _get_database_connection(self, connection_name: str):
        """Get database connection."""
        db_manager = get_database_manager()
        return db_manager.create_connection_instance(connection_name)

    def _get_database_type(self, connection) -> str:
        """Get database type from connection."""
        connection_type = type(connection).__name__

        if "postgres" in connection_type.lower():
            return "PostgreSQL"
        elif "mysql" in connection_type.lower():
            return "MySQL"
        elif "sqlite" in connection_type.lower():
            return "SQLite"
        else:
            return connection_type

    def _reset_postgresql(self, connection, schema: str = "public") -> None:
        """Reset PostgreSQL database by dropping all objects with CASCADE."""
        reset_sql = f"""
DO $do$
DECLARE
    r RECORD;
BEGIN
    -- Drop all tables with CASCADE to handle dependencies
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '{schema}')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS {schema}.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    -- Drop all sequences
    FOR r IN (SELECT sequencename FROM pg_sequences WHERE schemaname = '{schema}')
    LOOP
        EXECUTE 'DROP SEQUENCE IF EXISTS {schema}.' || quote_ident(r.sequencename) || ' CASCADE';
    END LOOP;

    -- Drop all views
    FOR r IN (SELECT viewname FROM pg_views WHERE schemaname = '{schema}')
    LOOP
        EXECUTE 'DROP VIEW IF EXISTS {schema}.' || quote_ident(r.viewname) || ' CASCADE';
    END LOOP;

    -- Drop all functions
    FOR r IN (SELECT proname, oidvectortypes(proargtypes) as argtypes
              FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
              WHERE n.nspname = '{schema}')
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS {schema}.' || quote_ident(r.proname) || '(' || r.argtypes || ') CASCADE';
    END LOOP;

    -- Drop all custom types
    FOR r IN (SELECT typname FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
              WHERE n.nspname = '{schema}' AND t.typtype = 'c')
    LOOP
        EXECUTE 'DROP TYPE IF EXISTS {schema}.' || quote_ident(r.typname) || ' CASCADE';
    END LOOP;
END $do$;"""

        connection.query(reset_sql)

    def _reset_mysql(self, connection) -> None:
        """Reset MySQL database by dropping all tables."""
        tables = self._get_mysql_tables(connection)
        if not tables:
            return

        # Disable foreign key checks
        connection.query("SET FOREIGN_KEY_CHECKS = 0")

        # Drop all tables
        for table in tables:
            connection.query(f"DROP TABLE IF EXISTS `{table}`")

        # Re-enable foreign key checks
        connection.query("SET FOREIGN_KEY_CHECKS = 1")

    def _get_mysql_tables(self, connection) -> List[str]:
        """Get all table names from MySQL."""
        result = connection.query("SHOW TABLES")
        return [list(row.values())[0] for row in result]

    def _reset_sqlite(self, connection) -> None:
        """Reset SQLite database by dropping all tables."""
        tables = self._get_sqlite_tables(connection)
        if not tables:
            return

        # Drop all tables
        for table in tables:
            connection.query(f"DROP TABLE IF EXISTS `{table}`")

    def _get_sqlite_tables(self, connection) -> List[str]:
        """Get all table names from SQLite excluding system tables."""
        result = connection.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [row["name"] for row in result]

    def _display_success_message(self) -> None:
        """Display success message."""
        self.info("✅ Database reset completed successfully!")
        self.info("💡 You can now run 'python craft migrate' to rebuild your schema.")

    def _handle_error(self, error: Exception) -> None:
        """Handle errors."""
        self.error(f"❌ Reset failed: {str(error)}")
        raise

    def _is_production(self) -> bool:
        """Check if running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]
