"""Rendering of generated create-table migrations."""

from __future__ import annotations


def _migration_generate_blueprint_create_migration(self, model_info: dict) -> str:
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
    for _field_name, field_info in model_info["fields"].items():
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
        # A model may key on a natural column (``__primary_key__ = "job_id"``
        # over a VARCHAR) rather than a serial surrogate. Injecting
        # ``increments("id")`` here used to mint a second id column the
        # model never declared — the reason the framework ledger tables
        # could not be model-generated at all. Honour the declared key when
        # it names a real declared field. An EXPLICIT ``__primary_key__ =
        # None`` means keyless by design (a pure membership table addressed
        # only through its parent's composite FK); the id injection remains
        # only for models that declare no key at all.
        declared_pk = model_info.get("primary_key")
        if declared_pk and declared_pk != "id" and declared_pk in model_info["fields"]:
            fields_code.append(f'            table.primary("{declared_pk}")')
        elif not ("primary_key" in model_info and declared_pk is None):
            fields_code.insert(0, '            table.increments("id")')

    # Composite ``field.unique([...])`` and ``field.index([...])``
    # calls were collected by ModelDiscoverer. Emit them as
    # ``table.unique([...])`` / ``table.index([...])`` so Postgres
    # gets the matching constraints (otherwise ``ON CONFLICT
    # (col_a, col_b)`` upserts in seed scripts fail with ``no
    # unique or exclusion constraint matching the ON CONFLICT
    # specification``).
    # Multi-column foreign keys declared as
    # ``field.foreign(["a", "b"]).references(["x", "y"]).on("t")``. They
    # have no per-column ``fields`` entry (the local side is a list), so
    # they are emitted from their own collection here — joining the scalar
    # FK lines above.
    for composite_fk in model_info.get("composite_foreign_keys", []):
        fk_line = self._generate_foreign_key_line(composite_fk)
        if fk_line:
            foreign_keys.append(f"            {fk_line}")

    # A model-declared ``name=`` is carried through verbatim so the object
    # in Postgres matches the model. Without it the Blueprint auto-derives a
    # name that Postgres then truncates at 63 chars, which is what forced
    # hand-written rename migrations.
    composite_lines = []
    for declaration in model_info.get("composite_uniques", []):
        composite_lines.append(
            f"            table.unique({self._composite_args(declaration)})"
        )
    for declaration in model_info.get("composite_indexes", []):
        composite_lines.append(
            f"            table.index({self._composite_args(declaration)})"
        )
    for declaration in model_info.get("checks", []):
        expression = repr(declaration["expression"])
        name = declaration.get("name")
        name_arg = f", name={name!r}" if name else ""
        composite_lines.append(f"            table.check({expression}{name_arg})")

    # Combine: regular fields → foreign keys → composite constraints
    all_lines = fields_code + foreign_keys + composite_lines

    replacements = {
        "{{ class }}": class_name,
        "{{ table }}": table_name,
        "{{ fields }}": "\n".join(all_lines),
    }

    result = stub_content
    for placeholder, replacement in replacements.items():
        result = result.replace(placeholder, replacement)

    # Append raw-SQL indexes/constraints/generated columns (from model
    # __indexes__) so partial-unique / GIN / CHECK / GENERATED objects the
    # Blueprint can't express are (re)created from the model on every
    # ``make:migration --overwrite``. Injected BEFORE views so a view that
    # reads a GENERATED column (e.g. price_history.recorded_at) sees
    # the column already added.
    indexes = model_info.get("indexes", [])
    if indexes:
        result = self._inject_indexes_into_migration(result, indexes, table_name)

    # Append VIEW definitions (from model __views__) after CREATE TABLE.
    views = model_info.get("views", [])
    if views:
        result = self._inject_views_into_migration(result, views, table_name)

    return result
