"""
ModelMigrationComparator: Compare model definitions with migration files.
Uses migration files as source of truth.
"""

import re
from pathlib import Path
from typing import Dict, List

from cara.support import paths


class ModelMigrationComparator:
    """Compare model definitions with migration files as source of truth."""

    def __init__(self):
        self.migrations_dir = Path(paths("migrations"))

    def compare_model_with_migrations(self, model_info: Dict) -> List[str]:
        """Compare model definition with migration files to find differences."""
        table_name = model_info["table"]

        # Parse migration files to get current schema state
        migration_schema = self._parse_migration_schema(table_name)

        if not migration_schema["table_exists"]:
            # Table doesn't exist in migrations, all fields are new
            differences = []
            for field_name, field_info in model_info["fields"].items():
                field_method = field_info.get("type", "string")
                differences.append(f"Added field: {field_name} ({field_method})")
            return differences

        # Compare model fields with migration fields
        differences = []
        migration_fields = migration_schema["fields"]
        field_definitions = migration_schema["field_definitions"]

        # Check for new fields in model
        for field_name, field_info in model_info["fields"].items():
            field_method = field_info.get("type", "string")

            if field_name not in migration_fields:
                differences.append(f"Added field: {field_name} ({field_method})")

        # Check for removed fields (fields in migration but not in model)
        for field_name in migration_fields:
            if field_name not in model_info["fields"] and field_name not in [
                "id",
                "created_at",
                "updated_at",
            ]:
                field_def = field_definitions.get(
                    field_name, f'table.string("{field_name}")'
                )
                differences.append(f"Removed field: {field_name} -> {field_def}")

        return differences

    def table_exists_in_migrations(self, table_name: str) -> bool:
        """Check if table exists in migration files."""
        migration_schema = self._parse_migration_schema(table_name)
        return migration_schema["table_exists"]

    def _parse_migration_schema(self, table_name: str) -> Dict:
        """Parse migration files to extract current schema state for a table."""
        schema = {
            "table_exists": False,
            "fields": set(),
            "field_definitions": {},  # Store field definitions for DOWN migrations
        }

        if not self.migrations_dir.exists():
            return schema

        # Get all migration files for this table
        migration_files = self._get_table_migration_files(table_name)

        # Process migrations chronologically
        for migration_file in sorted(migration_files):
            self._process_migration_file(migration_file, table_name, schema)

        return schema

    def _get_table_migration_files(self, table_name: str) -> List[Path]:
        """Get all migration files related to a specific table."""
        migration_files = []

        # Look for both create and update migrations
        patterns = [f"*create_{table_name}_table.py", f"*update_{table_name}_table.py"]

        for pattern in patterns:
            files = list(self.migrations_dir.glob(pattern))
            migration_files.extend(files)

        return migration_files

    def _process_migration_file(
        self, migration_file: Path, table_name: str, schema: Dict
    ):
        """Process a single migration file to update schema state."""
        try:
            content = migration_file.read_text()

            # Check if this is a create migration
            if f"create_{table_name}_table" in migration_file.name:
                self._process_create_migration(content, schema)

            # Check if this is an update migration
            elif f"update_{table_name}_table" in migration_file.name:
                self._process_update_migration(content, schema)

        except Exception:
            # Skip files that can't be read
            pass

    def _process_create_migration(self, content: str, schema: Dict):
        """Process CREATE TABLE migration to extract initial fields."""
        schema["table_exists"] = True

        # Check if this is a SQL-style migration (contains CREATE TABLE in query)
        if "CREATE TABLE" in content and "self.schema.new_connection().query(" in content:
            self._parse_sql_create_table(content, schema)
        else:
            # Blueprint-style migration
            self._parse_blueprint_create_table(content, schema)

    def _parse_sql_create_table(self, content: str, schema: Dict):
        """Parse SQL CREATE TABLE statement to extract fields."""
        # Try multiple patterns for extracting SQL
        patterns = [
            # Triple quotes pattern
            r'self\.schema\.new_connection\(\)\.query\s*\(\s*"""(.*?)"""\s*\)',
            # Triple quotes with different whitespace
            r'query\s*\(\s*"""(.*?)"""\s*\)',
            # Single quotes
            r"self\.schema\.new_connection\(\)\.query\s*\(\s*\'\'\'(.*?)\'\'\'\s*\)",
        ]

        sql_content = None
        for i, pattern in enumerate(patterns):
            sql_match = re.search(pattern, content, re.DOTALL)
            if sql_match:
                sql_content = sql_match.group(1).strip()
                break

        if not sql_content:
            return

        # Extract table creation part: CREATE TABLE "table_name" (...)
        # Use a more robust pattern that handles multi-line SQL properly
        create_table_pattern = r'CREATE TABLE\s+["\']?(\w+)["\']?\s*\((.*)\)\s*;?'
        create_match = re.search(
            create_table_pattern, sql_content, re.DOTALL | re.IGNORECASE
        )

        if not create_match:
            return

        table_name = create_match.group(1)
        columns_part = create_match.group(2).strip()

        # Parse each line for column definitions
        lines = columns_part.split("\n")

        for i, line in enumerate(lines):
            line = line.strip()

            if line and not line.startswith(")"):
                # Remove trailing comma
                line = line.rstrip(",")

                # Extract column name (first quoted part)
                col_name_match = re.match(r'["\'](\w+)["\']', line)
                if col_name_match:
                    field_name = col_name_match.group(1)

                    # Skip auto-generated fields
                    if field_name not in ["id", "created_at", "updated_at"]:
                        schema["fields"].add(field_name)
                        # Store field definition for down migrations
                        schema["field_definitions"][field_name] = (
                            f'table.string("{field_name}")'
                        )

    def _parse_blueprint_create_table(self, content: str, schema: Dict):
        """Parse Blueprint-style CREATE TABLE migration."""
        # Extract fields from table.* calls with full definitions
        field_patterns = [
            r'table\.(\w+)\(["\'](\w+)["\'][^)]*\)',  # table.string("name", 255).nullable()
        ]

        for pattern in field_patterns:
            matches = re.findall(pattern, content)
            for method, field_name in matches:
                # Skip built-in fields
                if field_name not in ["id"]:
                    schema["fields"].add(field_name)
                    # Store full field definition for DOWN migrations
                    full_line = self._extract_full_field_line(content, field_name)
                    if full_line:
                        schema["field_definitions"][field_name] = full_line

        # Handle timestamps() call
        if "table.timestamps()" in content:
            schema["fields"].add("created_at")
            schema["fields"].add("updated_at")
            schema["field_definitions"]["created_at"] = 'table.timestamp("created_at")'
            schema["field_definitions"]["updated_at"] = 'table.timestamp("updated_at")'

    def _process_update_migration(self, content: str, schema: Dict):
        """Process ALTER TABLE migration to update field list."""
        # Check if this is a SQL-style migration
        if "ALTER TABLE" in content and "self.schema.new_connection().query(" in content:
            self._parse_sql_update_migration(content, schema)
        else:
            # Blueprint-style migration
            self._parse_blueprint_update_migration(content, schema)

    def _parse_sql_update_migration(self, content: str, schema: Dict):
        """Parse SQL ALTER TABLE statements to update schema."""
        # Extract up() method content
        up_section = self._extract_up_method(content)
        if not up_section:
            return

        # Extract SQL query from up() method - handle triple quotes
        patterns = [
            r'self\.schema\.new_connection\(\)\.query\s*\(\s*"""(.*?)"""\s*\)',
            r"self\.schema\.new_connection\(\)\.query\s*\(\s*'''(.*?)'''\s*\)",
            r'self\.schema\.new_connection\(\)\.query\s*\(\s*"([^"]*?)"\s*\)',
            r"self\.schema\.new_connection\(\)\.query\s*\(\s*'([^']*?)'\s*\)",
        ]

        sql_content = None
        for pattern in patterns:
            sql_match = re.search(pattern, up_section, re.DOTALL)
            if sql_match:
                sql_content = sql_match.group(1).strip()
                break

        if not sql_content:
            return

        # Parse ADD COLUMN statements line by line
        lines = sql_content.split("\n")
        for line in lines:
            line = line.strip()

            # ADD COLUMN pattern
            add_match = re.search(
                r'ALTER TABLE\s+["\']?\w+["\']?\s+ADD COLUMN\s+["\'](\w+)["\']',
                line,
                re.IGNORECASE,
            )
            if add_match:
                field_name = add_match.group(1)
                if field_name not in ["id", "created_at", "updated_at"]:
                    schema["fields"].add(field_name)
                    schema["field_definitions"][field_name] = (
                        f'table.string("{field_name}")'
                    )

            # DROP COLUMN pattern
            drop_match = re.search(
                r'ALTER TABLE\s+["\']?\w+["\']?\s+DROP COLUMN\s+["\'](\w+)["\']',
                line,
                re.IGNORECASE,
            )
            if drop_match:
                field_name = drop_match.group(1)
                schema["fields"].discard(field_name)
                schema["field_definitions"].pop(field_name, None)

    def _parse_blueprint_update_migration(self, content: str, schema: Dict):
        """Parse Blueprint-style ALTER TABLE migration."""
        # Extract added fields from table.* calls in up() method
        up_section = self._extract_up_method(content)
        if not up_section:
            return

        # Extract added fields
        field_patterns = [
            r'table\.(\w+)\(["\'](\w+)["\']',  # table.string("name")
            r'table\.(\w+)\(["\'](\w+)["\'],',  # table.string("name", 255)
        ]

        for pattern in field_patterns:
            matches = re.findall(pattern, up_section)
            for method, field_name in matches:
                schema["fields"].add(field_name)
                # Store the field definition for this added field
                full_line = self._extract_full_field_line(up_section, field_name)
                if full_line:
                    schema["field_definitions"][field_name] = full_line

        # Extract dropped fields from up() method
        drop_patterns = [
            r'table\.drop_column\(["\'](\w+)["\']',  # table.drop_column("name")
        ]

        for pattern in drop_patterns:
            matches = re.findall(pattern, up_section)
            for field_name in matches:
                # Remove dropped fields from schema
                schema["fields"].discard(field_name)

        # Extract dropped fields from down() method
        down_section = self._extract_down_method(content)
        if down_section:
            drop_patterns = [
                r'table\.drop_column\(["\'](\w+)["\']',  # table.drop_column("name")
            ]

            for pattern in drop_patterns:
                matches = re.findall(pattern, down_section)
                for field_name in matches:
                    # These are fields that would be dropped in rollback,
                    # meaning they are added in this migration
                    schema["fields"].add(field_name)

    def _extract_up_method(self, content: str) -> str:
        """Extract the up() method content from migration."""
        # Simple regex to extract up method content
        pattern = r"def up\(self\):(.*?)def down\(self\):"
        match = re.search(pattern, content, re.DOTALL)
        return match.group(1) if match else ""

    def _extract_down_method(self, content: str) -> str:
        """Extract the down() method content from migration."""
        # Simple regex to extract down method content
        pattern = r"def down\(self\):(.*?)$"
        match = re.search(pattern, content, re.DOTALL)
        return match.group(1) if match else ""

    def _extract_full_field_line(self, content: str, field_name: str) -> str:
        """Extract the full field line from the migration file."""
        # Look for lines containing table.method("field_name"...)
        lines = content.split("\n")
        for line in lines:
            if (
                f'"{field_name}"' in line
                and "table." in line
                and "drop_column" not in line
            ):
                # Extract just the table.* part, clean up whitespace
                stripped = line.strip()
                if stripped.startswith("table."):
                    return stripped
                # If line has indentation, extract the table.* part
                table_part = re.search(r"table\.\w+\([^)]*\)(?:\.\w+\([^)]*\))*", line)
                if table_part:
                    return table_part.group(0)
        return ""
