"""Canonical definition of ``FieldBuilder``."""

from __future__ import annotations

from .FieldDefinition import FieldDefinition


class FieldBuilder:
    """Field builder for new Schema.build syntax."""

    @staticmethod
    def _constraint(
        kind: str,
        columns: str | list[str],
        name: str | None,
    ) -> FieldDefinition:
        """Build a standalone index/constraint declaration.

        ``Schema.build`` is executable framework API, not merely syntax for
        the migration AST parser. Keep its runtime shape aligned with the
        parser so model fields can be inspected without special-casing.
        """
        if not isinstance(columns, str | list):
            raise ValueError(f"{kind} columns must be a string or list of strings")
        normalized = [columns] if isinstance(columns, str) else list(columns)
        if not normalized or any(
            not isinstance(column, str) or not column for column in normalized
        ):
            raise ValueError(f"{kind} columns must be non-empty strings")
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError(f"{kind} name must be a non-empty string")
        definition = FieldDefinition(kind, None, columns=normalized)
        definition.params["name"] = name
        return definition

    def string(self, name, length=255):
        return FieldDefinition("string", name, length=length)

    def text(self, name):
        return FieldDefinition("text", name)

    def integer(self, name):
        return FieldDefinition("integer", name)

    def tiny_integer(self, name):
        return FieldDefinition("tiny_integer", name)

    def small_integer(self, name):
        return FieldDefinition("small_integer", name)

    def medium_integer(self, name):
        return FieldDefinition("medium_integer", name)

    def big_integer(self, name):
        return FieldDefinition("big_integer", name)

    def unsigned_integer(self, name):
        return FieldDefinition("unsigned_integer", name)

    def unsigned_big_integer(self, name):
        return FieldDefinition("unsigned_big_integer", name)

    def decimal(self, name, precision=10, scale=2):
        return FieldDefinition("decimal", name, precision=precision, scale=scale)

    def boolean(self, name):
        return FieldDefinition("boolean", name)

    def enum(self, name, options):
        return FieldDefinition("enum", name, options=options)

    def uuid(self, name):
        return FieldDefinition("uuid", name)

    def json(self, name):
        return FieldDefinition("json", name)

    def jsonb(self, name):
        # Postgres-native binary JSON. Several models call
        # ``field.jsonb("metadata")``; without this method the call
        # raised ``AttributeError`` inside ``Schema.build``,
        # ``MakeMigrationCommand`` swallowed it as a generic ValueError,
        # and the column quietly disappeared from every generated
        # migration (every ``metadata`` JSONB field across all tables).
        return FieldDefinition("jsonb", name)

    def timestamp(self, name):
        return FieldDefinition("timestamp", name)

    def datetime(self, name):
        return FieldDefinition("datetime", name)

    def date(self, name):
        return FieldDefinition("date", name)

    def time(self, name):
        return FieldDefinition("time", name)

    def float(self, name):
        return FieldDefinition("float", name)

    def double(self, name):
        # The drift ran the other way for this one: ``ColumnFactory.double``,
        # ``PostgresPlatform`` DOUBLE PRECISION and the migration emitter all
        # speak ``double`` already, but the builder a model actually calls did
        # not, so ``field.double(...)`` raised AttributeError inside
        # ``Schema.build`` and the column vanished.
        return FieldDefinition("double", name)

    def binary(self, name):
        return FieldDefinition("binary", name)

    def char(self, name, length=255):
        return FieldDefinition("char", name, length=length)

    def increments(self, name):
        return FieldDefinition("increments", name)

    def big_increments(self, name):
        return FieldDefinition("big_increments", name)

    def timestamps(self):
        """Create timestamps fields (created_at, updated_at)."""
        return FieldDefinition("timestamps", None)

    def soft_deletes(self):
        """Create soft delete field (deleted_at)."""
        return FieldDefinition("soft_deletes", None)

    def foreign(self, field_name):
        """Create a standalone foreign key definition."""
        fk_definition = FieldDefinition("foreign_key", None)
        fk_definition._is_foreign = True
        fk_definition._foreign_key_config = {
            "field": field_name,
            "references": None,
            "on": None,
            "on_delete": None,
            "on_update": None,
        }
        return fk_definition

    def unique(
        self,
        columns: str | list[str],
        name: str | None = None,
    ) -> FieldDefinition:
        """Declare a standalone single- or multi-column unique constraint."""
        return self._constraint("unique", columns, name)

    def index(
        self,
        columns: str | list[str],
        name: str | None = None,
    ) -> FieldDefinition:
        """Declare a standalone single- or multi-column index."""
        return self._constraint("index", columns, name)
