"""
MigrationGenerator: Generate migration content from model definitions.
"""

from __future__ import annotations

import contextlib
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path

from cara.exceptions import InvalidArgumentException
from cara.support import ProcessFileLock, modules, paths

from . import _MigrationRendering

_COUNTER_THREAD_LOCK = threading.Lock()
_GENERATION_THREAD_LOCK = threading.Lock()


@contextmanager
def _counter_lock(migrations_dir: Path):
    """Serialize counter reads/writes across threads and local processes."""
    migrations_dir.mkdir(parents=True, exist_ok=True)
    with _COUNTER_THREAD_LOCK:
        lock_path = migrations_dir / ".migration_counter.lock"
        with ProcessFileLock(lock_path):
            yield


def _atomic_write(path: Path, content: str) -> None:
    """Write a complete file then atomically publish it with ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


# Cara field-type -> Postgres type for the rename API (table.rename needs the
# column's SQL type since RENAME COLUMN goes through the same path as retype).
_RENAME_SQL_TYPE = {
    "string": "varchar",
    "char": "varchar",
    "text": "text",
    "integer": "integer",
    "tiny_integer": "smallint",
    "small_integer": "smallint",
    "medium_integer": "integer",
    "big_integer": "bigint",
    "unsigned_integer": "integer",
    "unsigned_big_integer": "bigint",
    "boolean": "boolean",
    "decimal": "numeric",
    "float": "double precision",
    "double": "double precision",
    "jsonb": "jsonb",
    "json": "json",
    # ``datetime`` is tz-AWARE; rendering it as bare ``timestamp`` here silently
    # downgraded a renamed/retyped column to naive.
    "timestamp": "timestamp",
    "datetime": "timestamptz",
    "date": "date",
    "time": "time",
    "uuid": "uuid",
    "binary": "bytea",
    "enum": "varchar",
    "increments": "serial",
    "big_increments": "bigserial",
}
# Field builders that declare no column of their own: ``timestamps`` /
# ``soft_deletes`` expand into named datetime columns (each of which reaches
# the rename path under its OWN type) and ``foreign`` / ``foreign_key``
# declare a constraint. Nothing here can be renamed as a single column, so
# they are deliberately absent rather than forgotten — the guard test in
# ``tests/migrations/`` asserts this partition against the field-type SSOT.
_NON_COLUMN_FIELD_TYPES = frozenset(
    {"timestamps", "soft_deletes", "foreign", "foreign_key"}
)


def _as_change(line: str) -> str:
    """Append ``.change()`` to a column line so the blueprint ALTERs it in place."""
    line = line.rstrip()
    return line if line.endswith(".change()") else line + ".change()"


class MigrationGenerator:
    """Generate migration files from model definitions."""

    def __init__(self):
        self.migrations_dir = Path(paths("migrations"))
        self.counter_file = self.migrations_dir / ".migration_counter"
        self._fresh_counter_batch = False

    @contextmanager
    def generation_lock(self):
        """Serialize complete make:migration runs, including overwrite."""
        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        with _GENERATION_THREAD_LOCK:
            lock_path = self.migrations_dir / ".migration_generation.lock"
            with ProcessFileLock(lock_path):
                yield

    def _get_counter(self):
        """Get current counter value from file."""
        if self.counter_file.exists():
            try:
                return int(self.counter_file.read_text().strip())
            except ValueError, FileNotFoundError:
                return 0
        return 0

    def _increment_counter(self):
        """Atomically reserve the next process-safe migration sequence."""
        with _counter_lock(self.migrations_dir):
            current = self._get_counter()
            if not self._fresh_counter_batch:
                current = max(current, self._highest_disk_sequence())
            new_counter = current + 1
            _atomic_write(self.counter_file, f"{new_counter}\n")
            return new_counter

    def reset_counter(self):
        """Reset the migration counter for a fresh batch."""
        with _counter_lock(self.migrations_dir):
            self._fresh_counter_batch = True
            _atomic_write(self.counter_file, "0\n")

    def finalize_counter(self):
        """Leave the counter beyond generated and preserved migration files."""
        with _counter_lock(self.migrations_dir):
            final_value = max(self._get_counter(), self._highest_disk_sequence())
            _atomic_write(self.counter_file, f"{final_value}\n")
            self._fresh_counter_batch = False

    def cancel_fresh_counter_batch(self):
        """Restore normal disk-floor behavior after a failed overwrite."""
        self._fresh_counter_batch = False

    def _highest_disk_sequence(self) -> int:
        highest = 0
        for path in self.migrations_dir.glob("*.py"):
            prefix = path.name.split("_", 1)[0]
            if prefix.isdigit():
                highest = max(highest, int(prefix))
        return highest

    def generate_create_migration(
        self, model_info: dict, style: str = "blueprint"
    ) -> str:
        """Generate CREATE TABLE migration content."""
        # Check if model has fields method
        if not model_info.get("has_fields_method", False):
            raise InvalidArgumentException(
                f"❌ Model '{model_info['name']}' does not have a 'fields' property method!\n"
                f"   📁 File: {model_info['filename']}\n"
                f"   💡 Add a 'fields' property method to define table structure:\n"
                f"   \n"
                f"   @property\n"
                f"   def fields(self):\n"
                f"       return Schema.build(\n"
                f"           lambda field: (\n"
                f"               field.string('name'),\n"
                f"               field.timestamps(),\n"
                f"               field.soft_deletes(),\n"
                f"           )\n"
                f"       )\n"
            )

        # Check if fields returns raw SQL (includes materialized views)
        if model_info.get("has_raw_sql", False):
            return self._generate_raw_sql_migration(model_info)

        if style == "sql":
            return self._generate_sql_create_migration(model_info)
        else:
            return self._generate_blueprint_create_migration(model_info)

    def create_migration_file(self, name: str, content: str, dependency_order: int = 0):
        """Create migration file with Laravel 11+ ordering system (no timestamps).

        Args:
            name: Migration name (e.g., "create_users_table")
            content: Migration file content
            dependency_order: Order in dependency chain (0 = no dependencies, 1+ = has dependencies)
        """
        # Increment counter for each migration
        counter = self._increment_counter()

        # New Laravel 11+ format: 0001_01_01_000000_migration_name.py
        # Format parts:
        # - First 4 digits: migration sequence (0001, 0002, etc.)
        # - Next 2 digits: batch within sequence (01, 02, etc.)
        # - Next 2 digits: sub-order (01, 02, etc.)
        # - Last 6 digits: micro-order (000000, 000001, etc.)

        sequence = f"{counter:04d}"  # 0001, 0002, 0003...
        batch = "01"  # Default batch
        sub_order = "01"  # Default sub-order
        micro_order = f"{dependency_order:06d}"  # 000000, 000001...

        # Laravel 11+ format: 0001_01_01_000000_migration_name.py
        filename = f"{sequence}_{batch}_{sub_order}_{micro_order}_{name}.py"
        filepath = self.migrations_dir / filename

        if filepath.exists():
            raise FileExistsError(f"Migration file already exists: {filepath}")
        _atomic_write(filepath, content)

        return filepath

    def _add_field_to_blueprint(self, table, field_name: str, field_info: dict):
        """Add a field to Blueprint table using field info."""
        field_type = field_info.get("type", "string")
        params = field_info.get("params", {})

        # Create the field based on type — capture the returned column
        # object so chained modifiers (.nullable(), .default(), .unique())
        # apply to the correct column instead of the table.
        column = None

        if field_type == "string":
            length = params.get("length", 255)
            column = table.string(field_name, length)
        elif field_type == "integer":
            column = table.integer(field_name)
        elif field_type == "unsigned_integer":
            column = table.unsigned_integer(field_name)
        elif field_type == "unsigned_big_integer":
            column = table.unsigned_big_integer(field_name)
        elif field_type == "text":
            column = table.text(field_name)
        elif field_type == "boolean":
            column = table.boolean(field_name)
        elif field_type == "decimal":
            precision = params.get("precision", 10)
            scale = params.get("scale", 2)
            column = table.decimal(field_name, precision, scale)
        elif field_type == "datetime":
            column = table.datetime(field_name)
        elif field_type == "timestamp":
            column = table.timestamp(field_name)
        elif field_type == "date":
            column = table.date(field_name)
        elif field_type == "time":
            column = table.time(field_name)
        elif field_type == "enum":
            options = params.get("options", [])
            column = table.enum(field_name, options)
        elif field_type == "json":
            column = table.json(field_name)
        elif field_type == "jsonb":
            column = table.jsonb(field_name)
        elif field_type == "float":
            column = table.float(field_name)
        elif field_type == "binary":
            column = table.binary(field_name)
        elif field_type == "uuid":
            column = table.uuid(field_name)
        elif field_type == "double":
            column = table.double(field_name)
        elif field_type == "char":
            length = params.get("length", 255)
            column = table.char(field_name, length)
        elif field_type == "tiny_integer":
            column = table.tiny_integer(field_name)
        elif field_type == "small_integer":
            column = table.small_integer(field_name)
        elif field_type == "medium_integer":
            column = table.medium_integer(field_name)
        elif field_type == "big_integer":
            column = table.big_integer(field_name)
        elif field_type == "increments":
            column = table.increments(field_name)
        elif field_type == "big_increments":
            column = table.big_increments(field_name)
        elif field_type == "timestamps":
            table.timestamps()
            return  # timestamps() doesn't return a column to modify
        elif field_type == "soft_deletes":
            table.soft_deletes()
            return  # soft_deletes() doesn't return a column to modify
        else:
            # Default to string for unknown types
            column = table.string(field_name)

        if column is None:
            return

        # Apply modifiers to the column object, not the table
        if params.get("nullable", False):
            column.nullable()

        if "default" in params:
            column.default(params["default"])

        if params.get("use_current", False):
            column.use_current()

        if params.get("unique", False):
            column.unique()

        # Handle foreign keys
        foreign_key_info = field_info.get("foreign_key")
        if foreign_key_info:
            references = foreign_key_info.get("references")
            on_table = foreign_key_info.get("on")
            on_delete = foreign_key_info.get("on_delete", "CASCADE")

            if references and on_table:
                table.foreign(field_name).references(references).on(on_table).on_delete(
                    on_delete
                )

    def _generate_field_line(self, field_name: str, field_info: dict) -> str:
        """Generate blueprint field line from field info."""
        field_method = field_info.get("type", "string")
        params = field_info.get("params", {})

        # Handle special field types that don't take field names. Foreign-key
        # fields are emitted separately.
        special_fields = {
            "foreign_key": "",
            "soft_deletes": "table.soft_deletes()",
            "timestamps": "table.timestamps()",
        }
        if field_method in special_fields:
            return special_fields[field_method]

        # Build method call based on field type
        if field_method == "decimal":
            precision = params.get("precision", 10)
            scale = params.get("scale", 2)
            blueprint_call = f'table.{field_method}("{field_name}", {precision}, {scale})'
        elif field_method in ("string", "char"):
            length = params.get("length", 255)
            blueprint_call = f'table.{field_method}("{field_name}", {length})'
        elif field_method == "enum":
            # Handle enum with options
            options = params.get("options", [])
            if options:
                options_str = ", ".join([f'"{opt}"' for opt in options])
                blueprint_call = f'table.{field_method}("{field_name}", [{options_str}])'
            else:
                blueprint_call = f'table.{field_method}("{field_name}", [])'
        elif field_method in ["increments", "big_increments"] or field_method in [
            "integer",
            "tiny_integer",
            "small_integer",
            "medium_integer",
            "big_integer",
            "unsigned_integer",
            "unsigned_big_integer",
            "text",
            "boolean",
            "datetime",
            "timestamp",
            "date",
            "time",
            "json",
            "jsonb",
            "float",
            "binary",
        ]:
            blueprint_call = f'table.{field_method}("{field_name}")'
        else:
            blueprint_call = f'table.{field_method}("{field_name}")'

        # Add modifiers
        if params.get("nullable", False):
            blueprint_call += ".nullable()"

        if "default" in params:
            default_val = params["default"]
            if params.get("default_is_raw"):
                # Expression default (DB.raw("now()"), an enum member, a named
                # constant) — emit verbatim, NEVER quoted.
                blueprint_call += f".default({default_val})"
            elif isinstance(default_val, bool):
                blueprint_call += f".default({default_val})"
            elif isinstance(default_val, str):
                # ``repr`` escapes embedded quotes/newlines, so a default like
                # ``27"`` can't emit a SyntaxError into the generated migration.
                blueprint_call += f".default({default_val!r})"
            else:
                blueprint_call += f".default({default_val})"

        if params.get("use_current", False):
            blueprint_call += ".use_current()"

        # Always add unique constraint if present, even if field also has foreign key
        if params.get("unique", False):
            blueprint_call += ".unique()"

        return blueprint_call

    @staticmethod
    def _composite_args(declaration: dict) -> str:
        """Render the argument list for ``table.index(...)``/``table.unique(...)``
        from a ``{"columns": [...], "name": str | None}`` declaration."""
        cols_str = ", ".join(f'"{c}"' for c in declaration["columns"])
        name = declaration.get("name")
        if name:
            return f'[{cols_str}], name="{name}"'
        return f"[{cols_str}]"

    def _generate_foreign_key_line(self, foreign_key_info: dict) -> str:
        """Generate foreign key constraint line from foreign key info.

        Scalar (``field``/``references`` are strings):
            ``table.foreign("a").references("x").on("t")``
        Composite (``columns``/``references`` are lists):
            ``table.foreign(["a", "b"]).references(["x", "y"]).on("t")``

        Composite entries come from ``model_info["composite_foreign_keys"]`` and
        carry their local columns under ``columns`` (the scalar shape uses
        ``field``); both forms share ``on``/``on_delete``/``on_update``.
        """
        columns = foreign_key_info.get("columns")
        references = foreign_key_info.get("references")
        on_table = foreign_key_info.get("on")
        on_delete = foreign_key_info.get("on_delete")
        on_update = foreign_key_info.get("on_update")

        if columns is not None:
            # Composite FK — both sides are column lists of matching length.
            if (
                not isinstance(columns, list)
                or not isinstance(references, list)
                or not on_table
                or len(columns) != len(references)
            ):
                return ""
            cols_str = ", ".join(f'"{c}"' for c in columns)
            refs_str = ", ".join(f'"{c}"' for c in references)
            fk_line = (
                f'table.foreign([{cols_str}]).references([{refs_str}]).on("{on_table}")'
            )
        else:
            # Scalar FK — unchanged.
            field = foreign_key_info.get("field")
            if not field or not references or not on_table:
                return ""
            # Build foreign key constraint: table.foreign("field").references("column").on("table")
            fk_line = (
                f'table.foreign("{field}").references("{references}").on("{on_table}")'
            )

        # Add ON DELETE clause if specified
        if on_delete:
            fk_line += f'.on_delete("{on_delete}")'

        # Add ON UPDATE clause if specified
        if on_update:
            fk_line += f'.on_update("{on_update}")'

        return fk_line

    def _get_create_stub_path(self) -> Path:
        """Get path to create migration stub."""
        return (
            Path(__file__).parent.parent.parent
            / "commands"
            / "stubs"
            / "CreateMigration.stub"
        )

    def _get_raw_sql_stub_path(self) -> Path:
        """Get path to raw SQL migration stub."""
        return (
            Path(__file__).parent.parent.parent
            / "commands"
            / "stubs"
            / "RawSqlMigration.stub"
        )

    def _get_raw_sql_stub_content(self) -> str:
        """Read raw SQL migration stub content."""
        stub_path = self._get_raw_sql_stub_path()
        return stub_path.read_text(encoding="utf-8")

    def _generate_raw_sql_migration(self, model_info: dict) -> str:
        """Generate migration using stub template."""
        model_name = model_info["name"]

        # Determine import path dynamically from file location
        model_file = model_info.get("file", "")
        model_import_path = self._generate_import_path(model_file, model_name)

        # Read stub template
        stub_content = self._get_raw_sql_stub_content()

        # Replace placeholders
        migration_content = stub_content.replace("{{ model_name }}", model_name)
        migration_content = migration_content.replace(
            "{{ model_import_path }}", model_import_path
        )

        return migration_content

    _generate_blueprint_create_migration = (
        _MigrationRendering._migration_generate_blueprint_create_migration
    )
    _prettify_sql = _MigrationRendering._migration_prettify_sql
    _prettify_create_table_sql = _MigrationRendering._migration_prettify_create_table_sql
    _prettify_alter_table_sql = _MigrationRendering._migration_prettify_alter_table_sql
    _generate_sql_create_migration = (
        _MigrationRendering._migration_generate_sql_create_migration
    )

    def _generate_import_path(self, model_file: str, model_name: str) -> str:
        """Generate Python import path from file path dynamically using Cara's module system."""

        # Use Cara's dynamic module system to get the models location
        models_location = modules("models")
        return models_location

    def _inject_views_into_migration(
        self, migration_content: str, views: list[dict], table_name: str
    ) -> str:
        """Inject VIEW SQL statements into a generated migration.

        Adds ``DB.statement(...)`` calls after the ``CREATE TABLE`` block
        in ``up()`` and ``DROP VIEW`` calls before ``schema.drop()`` in
        ``down()``.
        """
        # Build the up() VIEW statements
        view_up_lines = []
        view_down_lines = []
        for view in views:
            sql = view["sql"].strip()
            name = view["name"]
            view_up_lines.append(
                f'\n        DB.statement("""\n            {sql}\n        """)'
            )
            view_down_lines.append(f'        DB.statement("DROP VIEW IF EXISTS {name}")')

        # Ensure ``from cara.facades import DB`` is present
        if "from cara.facades import DB" not in migration_content:
            migration_content = migration_content.replace(
                "from cara.eloquent.migrations import Migration",
                "from cara.eloquent.migrations import Migration\n"
                "from cara.facades import DB",
            )

        # Insert VIEW creation after the ``with self.schema.create(...)``
        # block closes (the line that starts the down method).
        # We look for the blank line between up() body end and down() def.
        up_view_block = "\n".join(view_up_lines)
        migration_content = migration_content.replace(
            "\n    def down(self):",
            f"{up_view_block}\n\n    def down(self):",
        )

        # Insert VIEW drops before ``self.schema.drop(...)``
        down_view_block = "\n".join(view_down_lines)
        migration_content = migration_content.replace(
            f'        self.schema.drop("{table_name}")',
            f'{down_view_block}\n        self.schema.drop("{table_name}")',
        )

        return migration_content

    def _inject_indexes_into_migration(
        self, migration_content: str, indexes: list[dict], table_name: str
    ) -> str:
        """Inject raw-SQL index/constraint/generated-column statements.

        Adds each entry's ``up`` SQL as a ``DB.statement(...)`` after the
        ``CREATE TABLE`` block in ``up()`` (declared order — so a GENERATED
        column is added before the index that reads it), and the ``down`` SQL
        into ``down()`` in REVERSE order (drop the index before the column it
        depends on) before ``self.schema.drop(...)``.
        """
        up_lines = []
        for entry in indexes:
            # The model's DDL is also consumed by ``schema:apply`` against
            # already-live tables, where CONCURRENTLY avoids blocking writes.
            # A generated CREATE migration has just created an empty table and
            # runs transactionally, so CONCURRENTLY is unnecessary and illegal
            # inside that transaction. Keep the live declaration safe while
            # rendering the fresh-install form here.
            sql = re.sub(
                r"\bCONCURRENTLY\s+", "", entry["up"].strip(), flags=re.IGNORECASE
            )
            up_lines.append(
                f'\n        DB.statement("""\n            {sql}\n        """)'
            )
        down_lines = []
        for entry in reversed(indexes):
            sql = re.sub(
                r"\bCONCURRENTLY\s+", "", entry["down"].strip(), flags=re.IGNORECASE
            )
            down_lines.append(
                f'        DB.statement("""\n            {sql}\n        """)'
            )

        # Ensure ``from cara.facades import DB`` is present.
        if "from cara.facades import DB" not in migration_content:
            migration_content = migration_content.replace(
                "from cara.eloquent.migrations import Migration",
                "from cara.eloquent.migrations import Migration\n"
                "from cara.facades import DB",
            )

        up_block = "\n".join(up_lines)
        migration_content = migration_content.replace(
            "\n    def down(self):",
            f"{up_block}\n\n    def down(self):",
        )

        down_block = "\n".join(down_lines)
        migration_content = migration_content.replace(
            f'        self.schema.drop("{table_name}")',
            f'{down_block}\n        self.schema.drop("{table_name}")',
        )

        return migration_content
