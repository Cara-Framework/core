"""Durable migration history, checksums, and deployment serialization."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from cara.exceptions import ORMException

_logger = logging.getLogger("cara.migrations")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQLITE_LOCKS: dict[str, threading.Lock] = {}
_SQLITE_LOCKS_GUARD = threading.Lock()


def _release(connection) -> None:
    """Return an owned connection, never an executor transaction handle."""
    if connection is None:
        return
    transaction_level = getattr(connection, "transaction_level", 0)
    if isinstance(transaction_level, (int, float)) and transaction_level > 0:
        return
    try:
        close = getattr(connection, "close_connection", None)
        if callable(close):
            close()
    except Exception:
        _logger.debug("migration connection close failed", exc_info=True)


def _row_value(row, key: str, index: int = 0):
    if hasattr(row, "get"):
        return row.get(key)
    return row[index]


class MigrationTracker:
    """Tracks applied files and protects the migration critical section."""

    def __init__(self, db_manager, table_name: str = "migrations"):
        if not _IDENTIFIER.fullmatch(table_name or ""):
            raise ORMException(f"Invalid migrations table identifier: {table_name!r}")
        self.db_manager = db_manager
        self.table_name = table_name

    def _get_connection(self):
        return self.db_manager.create_connection_instance()

    def _get_driver_type(self) -> str:
        info = self.db_manager.get_connection_info() or {}
        return str(info.get("driver", "sqlite")).lower()

    def _get_placeholder(self) -> str:
        return (
            "%s"
            if self._get_driver_type() in {"postgres", "postgresql", "mysql"}
            else "?"
        )

    def _select_one(self, columns: str) -> str:
        if self._get_driver_type() in {"mssql", "sqlserver"}:
            return f"SELECT TOP 1 {columns} FROM {self.table_name}"
        return f"SELECT {columns} FROM {self.table_name} LIMIT 1"

    # ── Schema bootstrap ──────────────────────────────────────────────
    def ensure_migrations_table(self) -> None:
        """Create/upgrade the tracker without ever deleting its history."""
        connection = self._get_connection()
        try:
            self._create_migrations_table(connection)
            self._assert_base_structure(connection)
            self._ensure_checksum_column(connection)
            self._ensure_unique_migration_index(connection)
        except ORMException:
            raise
        except Exception as exc:
            raise ORMException(
                f"Could not initialize migrations table '{self.table_name}': {exc}"
            ) from exc
        finally:
            _release(connection)

    def _create_migrations_table(self, connection) -> None:
        driver = self._get_driver_type()
        if driver in {"postgres", "postgresql"}:
            identity = "BIGSERIAL PRIMARY KEY"
        elif driver == "mysql":
            identity = "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
        elif driver in {"mssql", "sqlserver"}:
            identity = "BIGINT IDENTITY(1,1) PRIMARY KEY"
        else:
            identity = "INTEGER PRIMARY KEY AUTOINCREMENT"
        created_at = (
            "DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()"
            if driver in {"mssql", "sqlserver"}
            else "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
        body = f"""
            CREATE TABLE {self.table_name} (
                id {identity},
                migration VARCHAR(255) NOT NULL,
                batch INTEGER NOT NULL,
                checksum VARCHAR(64) NULL,
                created_at {created_at}
            )
        """
        if driver in {"mssql", "sqlserver"}:
            connection.query(
                f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL BEGIN {body} END"
            )
        else:
            connection.query(
                body.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            )

    def _assert_base_structure(self, connection) -> None:
        try:
            connection.query(self._select_one("id, migration, batch"))
        except Exception as exc:
            raise ORMException(
                f"Migrations table '{self.table_name}' has an unexpected schema; "
                "required columns are id, migration, and batch."
            ) from exc

    def _ensure_checksum_column(self, connection) -> None:
        try:
            connection.query(self._select_one("checksum"))
            return
        except Exception:
            pass
        try:
            add = (
                "ADD"
                if self._get_driver_type() in {"mssql", "sqlserver"}
                else "ADD COLUMN"
            )
            connection.query(
                f"ALTER TABLE {self.table_name} {add} checksum VARCHAR(64) NULL"
            )
            connection.query(self._select_one("checksum"))
        except Exception as exc:
            raise ORMException(
                f"Could not add checksum tracking to '{self.table_name}'."
            ) from exc

    def _ensure_unique_migration_index(self, connection) -> None:
        index_name = f"{self.table_name}_migration_unique"
        driver = self._get_driver_type()
        try:
            if driver == "mysql":
                rows = connection.query(
                    f"SHOW INDEX FROM {self.table_name} WHERE Key_name = %s",
                    (index_name,),
                )
                if not rows:
                    connection.query(
                        f"CREATE UNIQUE INDEX {index_name} "
                        f"ON {self.table_name} (migration)"
                    )
            elif driver in {"mssql", "sqlserver"}:
                connection.query(
                    f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = ?) "
                    f"CREATE UNIQUE INDEX {index_name} "
                    f"ON {self.table_name} (migration)",
                    (index_name,),
                )
            else:
                connection.query(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
                    f"ON {self.table_name} (migration)"
                )
        except Exception as exc:
            raise ORMException(
                f"Could not enforce unique migration names in '{self.table_name}'. "
                "Remove duplicate tracker rows before retrying."
            ) from exc

    # ── Deployment lock ──────────────────────────────────────────────
    @contextmanager
    def migration_lock(self, timeout_seconds: int = 60):
        """Serialize migration runners with a database advisory lock."""
        if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
            raise ORMException("Migration lock timeout must be a positive integer.")
        driver = self._get_driver_type()
        if driver == "sqlite":
            # SQLite only serializes each individual write. Without an outer
            # lock, two runners can both calculate the same pending set before
            # either writes. A host-level lock covers the whole plan/apply
            # critical section; SQLite is not a multi-host database.
            with self._sqlite_migration_lock(timeout_seconds):
                yield
            return

        connection = self._get_connection()
        lock_key = self._migration_lock_key()
        began = False
        mysql_locked = False
        try:
            begin = getattr(connection, "begin", None)
            if callable(begin):
                begin()
                began = True

            if driver in {"postgres", "postgresql"}:
                connection.query(
                    f"SET LOCAL lock_timeout = '{int(timeout_seconds) * 1000}ms'"
                )
                connection.query(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (lock_key,),
                )
            elif driver == "mysql":
                rows = connection.query(
                    "SELECT GET_LOCK(%s, %s) AS acquired",
                    (lock_key, int(timeout_seconds)),
                )
                acquired = _row_value(rows[0], "acquired") if rows else 0
                if int(acquired or 0) != 1:
                    raise ORMException("Timed out waiting for the migration lock.")
                mysql_locked = True
            elif driver in {"mssql", "sqlserver"}:
                rows = connection.query(
                    "DECLARE @result int; "
                    "EXEC @result = sp_getapplock @Resource=?, "
                    "@LockMode='Exclusive', @LockOwner='Session', @LockTimeout=?; "
                    "SELECT @result AS lock_result",
                    (lock_key, int(timeout_seconds) * 1000),
                )
                result = _row_value(rows[0], "lock_result") if rows else -999
                if int(result) < 0:
                    raise ORMException("Timed out waiting for the migration lock.")
            yield
        finally:
            if mysql_locked:
                try:
                    connection.query("SELECT RELEASE_LOCK(%s)", (lock_key,))
                except Exception:
                    _logger.error("failed to release migration lock", exc_info=True)
            if began:
                try:
                    connection.rollback()
                except Exception:
                    _logger.debug(
                        "migration lock transaction cleanup failed", exc_info=True
                    )
            _release(connection)

    def _migration_lock_key(self) -> str:
        info = self.db_manager.get_connection_info() or {}
        database = info.get("database") or info.get("name") or "default"
        return f"cara:migrations:{database}:{self.table_name}"

    @contextmanager
    def _sqlite_migration_lock(self, timeout_seconds: int):
        lock_key = self._migration_lock_key()
        with _SQLITE_LOCKS_GUARD:
            thread_lock = _SQLITE_LOCKS.setdefault(lock_key, threading.Lock())
        if not thread_lock.acquire(timeout=timeout_seconds):
            raise ORMException("Timed out waiting for the SQLite migration lock.")

        digest = hashlib.sha256(lock_key.encode("utf-8")).hexdigest()[:24]
        lock_path = Path(tempfile.gettempdir()) / f"cara-migrations-{digest}.lock"
        handle = None
        process_locked = False
        try:
            handle = lock_path.open("a+b")
            handle.seek(0)
            if handle.read(1) == b"":
                handle.write(b"\0")
                handle.flush()
            deadline = time.monotonic() + timeout_seconds
            while True:
                try:
                    if os.name == "nt":  # pragma: no cover - Windows CI only
                        import msvcrt

                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    process_locked = True
                    break
                except (BlockingIOError, OSError):
                    if time.monotonic() >= deadline:
                        raise ORMException(
                            "Timed out waiting for the SQLite migration lock."
                        ) from None
                    time.sleep(0.05)
            yield
        finally:
            if handle is not None:
                if process_locked:
                    try:
                        if os.name == "nt":  # pragma: no cover - Windows CI only
                            import msvcrt

                            handle.seek(0)
                            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            import fcntl

                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        _logger.debug("SQLite migration unlock failed", exc_info=True)
                handle.close()
            thread_lock.release()

    # ── Read APIs ────────────────────────────────────────────────────
    def get_ran_migrations(self) -> list[str]:
        return [row["migration"] for row in self.get_ran_migration_records()]

    def get_ran_migration_records(self) -> list[dict[str, str | None]]:
        connection = self._get_connection()
        try:
            rows = (
                connection.query(
                    f"SELECT migration, checksum FROM {self.table_name} ORDER BY batch, id"
                )
                or []
            )
            return [
                {
                    "migration": _row_value(row, "migration", 0),
                    "checksum": _row_value(row, "checksum", 1),
                }
                for row in rows
            ]
        finally:
            _release(connection)

    def get_last_batch_number(self) -> int:
        connection = self._get_connection()
        try:
            rows = (
                connection.query(f"SELECT MAX(batch) AS max_batch FROM {self.table_name}")
                or []
            )
            if not rows:
                return 0
            value = _row_value(rows[0], "max_batch", 0)
            return int(value or 0)
        finally:
            _release(connection)

    def get_migrations_by_batch(self, batch: int) -> list[str]:
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            rows = (
                connection.query(
                    f"SELECT migration FROM {self.table_name} "
                    f"WHERE batch = {placeholder} ORDER BY id DESC",
                    (batch,),
                )
                or []
            )
            return [_row_value(row, "migration", 0) for row in rows]
        finally:
            _release(connection)

    # ── Write APIs ───────────────────────────────────────────────────
    def record_migration(self, migration_name: str, batch: int, checksum: str) -> None:
        if not checksum or len(checksum) != 64:
            raise ORMException("A SHA-256 migration checksum is required.")
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            connection.query(
                f"INSERT INTO {self.table_name} (migration, batch, checksum) "
                f"VALUES ({placeholder}, {placeholder}, {placeholder})",
                (migration_name, batch, checksum),
            )
        finally:
            _release(connection)

    def set_migration_checksum(self, migration_name: str, checksum: str) -> None:
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            connection.query(
                f"UPDATE {self.table_name} SET checksum = {placeholder} "
                f"WHERE migration = {placeholder} AND checksum IS NULL",
                (checksum, migration_name),
            )
        finally:
            _release(connection)

    def replace_migration_history(
        self, records: list[tuple[str, str]], *, batch: int = 1
    ) -> None:
        """Atomically replace history after an explicit, schema-verified baseline."""
        if not records or int(batch) <= 0:
            raise ORMException("Baseline history and batch must be non-empty.")
        names = [name for name, _checksum in records]
        if len(names) != len(set(names)):
            raise ORMException("Baseline migration names must be unique.")
        if any(not checksum or len(checksum) != 64 for _name, checksum in records):
            raise ORMException("Every baseline migration requires a SHA-256 checksum.")

        connection = self._get_connection()
        began = False
        try:
            begin = getattr(connection, "begin", None)
            if callable(begin):
                begin()
                began = True
            connection.query(f"DELETE FROM {self.table_name}")
            placeholder = self._get_placeholder()
            for name, checksum in records:
                connection.query(
                    f"INSERT INTO {self.table_name} (migration, batch, checksum) "
                    f"VALUES ({placeholder}, {placeholder}, {placeholder})",
                    (name, int(batch), checksum),
                )
            commit = getattr(connection, "commit", None)
            if began and callable(commit):
                commit()
        except Exception:
            rollback = getattr(connection, "rollback", None)
            if began and callable(rollback):
                rollback()
            raise
        finally:
            _release(connection)

    def remove_migration(self, migration_name: str) -> None:
        connection = self._get_connection()
        try:
            placeholder = self._get_placeholder()
            connection.query(
                f"DELETE FROM {self.table_name} WHERE migration = {placeholder}",
                (migration_name,),
            )
        finally:
            _release(connection)
