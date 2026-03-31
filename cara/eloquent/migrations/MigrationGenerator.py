"""
MigrationGenerator: Generate migration content from model definitions.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from cara.support import paths


class MigrationGenerator:
    """Generate migration files from model definitions."""

    def __init__(self):
        self.migrations_dir = Path(paths("migrations"))
        self.counter_file = self.migrations_dir / ".migration_counter"

    def _get_counter(self):
        """Get current counter value from file."""
        if self.counter_file.exists():
            try:
                return int(self.counter_file.read_text().strip())
            except (ValueError, FileNotFoundError):
                return 0
        return 0

    def _increment_counter(self):
        """Increment and save counter."""
        current = self._get_counter()
        new_counter = current + 1
        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        self.counter_file.write_text(str(new_counter))
        return new_counter

    def reset_counter(self):
        """Reset the migration counter for a fresh batch."""
        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        self.counter_file.write_text("0")

    def generate_create_migration(
        self, model_info: Dict, style: str = "blueprint"
    ) -> str:
        """Generate CREATE TABLE migration content."""
        # Check if model has fields method
        if not model_info.get("has_fields_method", False):
            raise ValueError(
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

    def generate_update_migration(
        self, model_info: Dict, diff: List[str], style: str = "blueprint"
    ) -> str:
        """Generate ALTER TABLE migration content."""
        if style == "sql":
            return self._generate_sql_update_migration(model_info, diff)
        else:
            return self._generate_blueprint_update_migration(model_info, diff)

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

        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content, encoding="utf-8")

        return filepath

    def _generate_blueprint_create_migration(self, model_info: Dict) -> str:
        """Generate blueprint-style CREATE TABLE migration."""
        stub_path = self._get_create_stub_path()
        stub_content = stub_path.read_text()
        table_name = model_info["table"]
        class_name = f"Create{model_info['name']}Table"

        # Generate table fields
        fields_code = []
        foreign_keys = []

        for field_name, field_info in model_info["fields"].items():
            # Skip foreign key fields - they will be handled separately
            if field_info.get("type") == "foreign_key":
                continue

            field_line = self._generate_field_line(field_name, field_info)
            fields_code.append(f"            {field_line}")

            # Check if this field has foreign key constraint
            foreign_key_info = field_info.get("foreign_key")
            if foreign_key_info:
                fk_line = self._generate_foreign_key_line(foreign_key_info)
                if fk_line:
                    foreign_keys.append(f"            {fk_line}")

        # Handle standalone foreign key definitions
        for field_name, field_info in model_info["fields"].items():
            if field_info.get("type") == "foreign_key":
                fk_line = self._generate_foreign_key_line(field_info)
                if fk_line:
                    foreign_keys.append(f"            {fk_line}")

        # Add primary key if not already present - check if any field contains increments
        has_primary_key = False
        for field in fields_code:
            if "table.increments(" in field or "table.big_increments(" in field:
                has_primary_key = True
                break

        if not has_primary_key:
            fields_code.insert(0, '            table.increments("id")')

        # Combine fields and foreign keys
        all_lines = fields_code + foreign_keys

        replacements = {
            "{{ class }}": class_name,
            "{{ table }}": table_name,
            "{{ fields }}": "\n".join(all_lines),
        }

        result = stub_content
        for placeholder, replacement in replacements.items():
            result = result.replace(placeholder, replacement)

        return result

    def _generate_blueprint_update_migration(
        self, model_info: Dict, diff: List[str]
    ) -> str:
        """Generate blueprint-style ALTER TABLE migration."""
        stub_path = self._get_update_stub_path()
        stub_content = stub_path.read_text()
        table_name = model_info["table"]
        class_name = f"Update{model_info['name']}Table"

        # Extract only the changed fields
        changed_fields = []
        drop_fields = []
        foreign_keys = []

        for diff_line in diff:
            if "Added field:" in diff_line:
                field_name = diff_line.replace("Added field: ", "").split(" (")[0]
                if field_name in model_info["fields"]:
                    field_info = model_info["fields"][field_name]

                    # Handle foreign key fields separately
                    if field_info.get("type") == "foreign_key":
                        fk_line = self._generate_foreign_key_line(field_info)
                        if fk_line:
                            foreign_keys.append(f"            {fk_line}")
                    else:
                        field_line = self._generate_field_line(field_name, field_info)
                        if field_line:  # Only add non-empty field lines
                            changed_fields.append(f"            {field_line}")

                    drop_fields.append(f'            table.drop_column("{field_name}")')
            elif "Removed field:" in diff_line:
                # Parse: "Removed field: field_name -> table.string("field_name")"
                parts = diff_line.replace("Removed field: ", "").split(" -> ")
                field_name = parts[0]
                field_definition = (
                    parts[1] if len(parts) > 1 else f'table.string("{field_name}")'
                )
                # For removed fields, we drop in up() and add back in down()
                changed_fields.append(f'            table.drop_column("{field_name}")')
                drop_fields.append(f"            {field_definition}")

        # Combine fields and foreign keys
        all_fields = changed_fields + foreign_keys

        replacements = {
            "{{ class }}": class_name,
            "{{ table }}": table_name,
            "{{ fields }}": "\n".join(all_fields),
            "{{ drop_fields }}": "\n".join(drop_fields),
        }

        result = stub_content
        for placeholder, replacement in replacements.items():
            result = result.replace(placeholder, replacement)

        return result

    def _prettify_sql(self, sql: str) -> str:
        """Clean up SQL formatting."""
        # Remove extra whitespace
        sql = re.sub(r"\s+", " ", sql)
        # Clean up around commas and parentheses
        sql = re.sub(r"\s*,\s*", ", ", sql)
        sql = re.sub(r"\s*\(\s*", "(", sql)
        sql = re.sub(r"\s*\)\s*", ")", sql)
        # Clean up quotes
        sql = re.sub(r"'\s*,\s*'", "', '", sql)
        # Add spaces around operators
        sql = re.sub(r"([<>=!]+)", r" \1 ", sql)
        sql = re.sub(r"\s+", " ", sql)  # Remove duplicate spaces
        return sql.strip()

    def _prettify_create_table_sql(self, sql: str) -> str:
        """Format CREATE TABLE SQL nicely."""
        if not sql.strip():
            return sql

        # Replace common patterns for better formatting
        sql = sql.replace("CREATE TABLE ", "CREATE TABLE\n    ")
        sql = sql.replace(" (", "\n(\n    ")
        sql = sql.replace(", ", ",\n    ")
        sql = sql.replace(");", "\n);")

        # Clean up spacing
        lines = []
        for line in sql.split("\n"):
            line = line.strip()
            if line:
                lines.append(line)

        # Indent field definitions
        formatted_lines = []
        for line in lines:
            if line.startswith("CREATE TABLE"):
                formatted_lines.append(line)
            elif line == "(":
                formatted_lines.append("(")
            elif line.endswith(");"):
                formatted_lines.append(");")
            elif line.endswith(","):
                formatted_lines.append(f"    {line}")
            else:
                formatted_lines.append(f"    {line}")

        return "\n".join(formatted_lines)

    def _prettify_alter_table_sql(self, sql: str) -> str:
        """Format ALTER TABLE SQL nicely."""
        if not sql.strip():
            return sql

        # Basic formatting for ALTER statements
        sql = sql.replace("ALTER TABLE ", "ALTER TABLE\n    ")
        sql = sql.replace(" ADD COLUMN ", "\n    ADD COLUMN ")
        sql = sql.replace(" DROP COLUMN ", "\n    DROP COLUMN ")
        sql = sql.replace(" MODIFY COLUMN ", "\n    MODIFY COLUMN ")
        sql = sql.replace(";", ";\n")

        # Clean up extra newlines
        lines = [line.strip() for line in sql.split("\n") if line.strip()]
        return "\n".join(lines)

    def _generate_sql_create_migration(self, model_info: Dict) -> str:
        """Generate SQL-style CREATE TABLE migration using Blueprint's to_sql()."""
        from cara.eloquent.schema import Schema

        table_name = model_info["table"]
        class_name = f"Create{model_info['name']}Table"

        # Create a schema in dry-run mode to get SQL without executing
        schema = Schema(dry=True)

        # Use dry-run mode to get SQL without executing
        with schema.create(table_name) as table:
            # Add all fields from the model
            for field_name, field_info in model_info["fields"].items():
                self._add_field_to_blueprint(table, field_name, field_info)

            # Get the SQL from Blueprint without executing
            raw_sql = table.to_sql()
            # Join SQL statements if it's a list
            if isinstance(raw_sql, list):
                create_sql = "\n            ".join(
                    [self._prettify_sql(sql) for sql in raw_sql]
                )
            else:
                create_sql = self._prettify_sql(raw_sql)

        # Generate migration template with formatted SQL
        return f'''from cara.eloquent.migrations import Migration


class {class_name}(Migration):
    def up(self):
        self.schema.new_connection().query(
            """
            {create_sql}
            """
        )

    def down(self):
        self.schema.new_connection().query("DROP TABLE IF EXISTS {table_name};")
'''

    def _generate_sql_update_migration(self, model_info: Dict, diff: List[str]) -> str:
        """Generate SQL-style ALTER TABLE migration using Blueprint's to_sql()."""
        from cara.eloquent.schema import Schema

        table_name = model_info["table"]
        class_name = f"Update{model_info['name']}Table"

        # Create a schema and get the SQL using Blueprint
        schema = Schema(dry=True)

        # For updates, we'll generate individual ALTER statements
        alter_statements = []
        rollback_statements = []

        for diff_line in diff:
            if "Added field:" in diff_line:
                field_name = diff_line.replace("Added field: ", "").split(" (")[0]
                if field_name in model_info["fields"]:
                    field_info = model_info["fields"][field_name]

                    # Use Blueprint to generate ADD COLUMN SQL
                    with schema.table(table_name) as table:
                        self._add_field_to_blueprint(table, field_name, field_info)
                        raw_alter_sql = table.to_sql()

                    # Join SQL statements if it's a list and prettify
                    if isinstance(raw_alter_sql, list):
                        alter_sql = "\n            ".join(
                            [self._prettify_sql(sql) for sql in raw_alter_sql]
                        )
                    else:
                        alter_sql = self._prettify_sql(raw_alter_sql)

                    alter_statements.append(alter_sql)
                    rollback_statements.append(
                        f'ALTER TABLE "{table_name}" DROP COLUMN "{field_name}";'
                    )

            elif "Removed field:" in diff_line:
                # Parse: "Removed field: years -> table.string("years")"
                field_name = diff_line.replace("Removed field: ", "").split(" ->")[0]
                alter_statements.append(
                    f'ALTER TABLE "{table_name}" DROP COLUMN "{field_name}";'
                )
                rollback_statements.append(
                    f"-- TODO: Add back removed column {field_name}"
                )

        up_sql = (
            "\n            ".join(alter_statements)
            if alter_statements
            else "-- No changes"
        )
        down_sql = (
            "\n            ".join(rollback_statements)
            if rollback_statements
            else "-- No rollback needed"
        )

        return f'''from cara.eloquent.migrations import Migration


class {class_name}(Migration):
    def up(self):
        self.schema.new_connection().query(
            """
            {up_sql}
            """
        )

    def down(self):
        self.schema.new_connection().query(
            """
            {down_sql}
            """
        )
'''

    def _add_field_to_blueprint(self, table, field_name: str, field_info: Dict):
        """Add a field to Blueprint table using field info."""
        field_type = field_info.get("type", "string")
        params = field_info.get("params", {})

        # Create the field based on type
        if field_type == "string":
            length = params.get("length", 255)
            table.string(field_name, length)
        elif field_type == "integer":
            table.integer(field_name)
        elif field_type == "text":
            table.text(field_name)
        elif field_type == "boolean":
            table.boolean(field_name)
        elif field_type == "decimal":
            precision = params.get("precision", 10)
            scale = params.get("scale", 2)
            table.decimal(field_name, precision, scale)
        elif field_type == "datetime":
            table.datetime(field_name)
        elif field_type == "timestamp":
            table.timestamp(field_name)
        elif field_type == "date":
            table.date(field_name)
        elif field_type == "time":
            table.time(field_name)
        elif field_type == "enum":
            options = params.get("options", [])
            table.enum(field_name, options)
        elif field_type == "json":
            table.json(field_name)
        elif field_type == "float":
            table.float(field_name)
        elif field_type == "binary":
            table.binary(field_name)
        elif field_type == "char":
            length = params.get("length", 255)
            table.char(field_name, length)
        elif field_type == "increments":
            table.increments(field_name)
        elif field_type == "big_increments":
            table.big_increments(field_name)
        elif field_type == "timestamps":
            table.timestamps()
            return  # timestamps() doesn't return a column to modify
        elif field_type == "soft_deletes":
            table.soft_deletes()
            return  # soft_deletes() doesn't return a column to modify
        else:
            # Default to string for unknown types
            table.string(field_name)

        # Apply modifiers
        if params.get("nullable", False):
            table.nullable()

        if "default" in params:
            table.default(params["default"])

        if params.get("unique", False):
            table.unique()

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

    def _generate_field_line(self, field_name: str, field_info: Dict) -> str:
        """Generate blueprint field line from field info."""
        field_method = field_info.get("type", "string")
        params = field_info.get("params", {})

        # Handle special field types that don't take field names
        if field_method == "timestamps":
            return "table.timestamps()"
        elif field_method == "soft_deletes":
            return "table.soft_deletes()"
        elif field_method == "foreign_key":
            # Foreign key fields are handled separately, return empty string
            return ""

        # Build method call based on field type
        if field_method == "decimal":
            length = params.get("precision", 10)
            precision = params.get("scale", 2)
            blueprint_call = (
                f'table.{field_method}("{field_name}", {length}, {precision})'
            )
        elif field_method == "string":
            length = params.get("length", 255)
            blueprint_call = f'table.{field_method}("{field_name}", {length})'
        elif field_method == "char":
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
        elif field_method in ["increments", "big_increments"]:
            blueprint_call = f'table.{field_method}("{field_name}")'
        elif field_method in [
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
            if isinstance(default_val, str):
                blueprint_call += f'.default("{default_val}")'
            elif isinstance(default_val, bool):
                blueprint_call += f".default({str(default_val)})"
            else:
                blueprint_call += f".default({default_val})"

        # Always add unique constraint if present, even if field also has foreign key
        if params.get("unique", False):
            blueprint_call += ".unique()"

        return blueprint_call

    def _generate_foreign_key_line(self, foreign_key_info: Dict) -> str:
        """Generate foreign key constraint line from foreign key info."""
        # Handle both old format (foreign_key_info) and new format (direct field_info)
        if "field" in foreign_key_info:
            # Old format: field has foreign_key property
            field = foreign_key_info.get("field")
            references = foreign_key_info.get("references")
            on_table = foreign_key_info.get("on")
            on_delete = foreign_key_info.get("on_delete")
            on_update = foreign_key_info.get("on_update")
        else:
            # New format: field_info is the foreign key definition itself
            field = foreign_key_info.get("field")
            references = foreign_key_info.get("references")
            on_table = foreign_key_info.get("on")
            on_delete = foreign_key_info.get("on_delete")
            on_update = foreign_key_info.get("on_update")

        if not field or not references or not on_table:
            return ""

        # Build foreign key constraint: table.foreign("field").references("column").on("table")
        fk_line = f'table.foreign("{field}").references("{references}").on("{on_table}")'

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

    def _get_update_stub_path(self) -> Path:
        """Get path to update migration stub."""
        return (
            Path(__file__).parent.parent.parent
            / "commands"
            / "stubs"
            / "UpdateMigration.stub"
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



    def _generate_raw_sql_migration(self, model_info: Dict) -> str:
        """Generate migration using stub template."""
        model_name = model_info["name"]
        
        # Determine import path dynamically from file location
        model_file = model_info.get('file', '')
        model_import_path = self._generate_import_path(model_file, model_name)
        
        # Read stub template
        stub_content = self._get_raw_sql_stub_content()
        
        # Replace placeholders
        migration_content = stub_content.replace("{{ model_name }}", model_name)
        migration_content = migration_content.replace("{{ model_import_path }}", model_import_path)
        
        return migration_content

    def _generate_import_path(self, model_file: str, model_name: str) -> str:
        """Generate Python import path from file path dynamically using Cara's module system."""
        from cara.support import modules

        # Use Cara's dynamic module system to get the models location
        models_location = modules("models")
        return models_location


    def _generate_migration_filename(self, name: str) -> str:
        """Generate timestamped migration filename."""
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        return f"{timestamp}_{name}.py"
