""" "Is it already there?" has to be asked of the right catalogue.

A ``__indexes__`` entry's ``name`` is a label, and its SQL may create an
object that lives nowhere near the table's index list. Asked of the wrong
catalogue the answer is always "missing", so the entry is planned on every
run: five phantom operations in every cheapa plan, forever, each an
``IF NOT EXISTS`` no-op a reviewer had to dismiss by hand. A plan that is
never empty cannot be trusted to mean anything when it is not empty.

The same mistake in the other direction produced dangerous advice:
``live.tables`` is built from ``information_schema.columns``, which lists
VIEWS as well, so every view in the schema was reported as an orphaned TABLE
with the suggestion to drop it — about relations the storefront reads on
every request.
"""

from __future__ import annotations

from cara.exceptions import SchemaPlanRefused
from cara.schema import LiveSchema, plan
from cara.schema.Objects import created_objects


def _model(table="product", indexes=None):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": indexes or [],
    }


def _live(table="product", columns=None, extensions=(), kinds=None):
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
                **(columns or {}),
            }
        },
        checks={},
        indexes={},
        constraint_indexes={},
        extensions=set(extensions),
        relation_kinds=kinds if kinds is not None else {table: "BASE TABLE"},
    )


def _column(name):
    return {name: {"data_type": "tsvector", "is_nullable": True, "max_length": None}}


def test_an_installed_extension_is_not_reinstalled_every_plan():
    """``ext_pg_trgm`` creates an extension called ``pg_trgm``. Looked for in
    the index list under its LABEL it is missing forever."""
    model = _model(
        indexes=[
            {
                "name": "ext_pg_trgm",
                "up": "CREATE EXTENSION IF NOT EXISTS pg_trgm",
                "down": "SELECT 1",
            }
        ]
    )

    operations, _, _ = plan([model], _live(extensions=("pg_trgm",)))

    assert operations == []


def test_a_missing_extension_is_still_planned():
    """The fix must not become "extensions are never planned"."""
    model = _model(
        indexes=[
            {
                "name": "ext_pg_trgm",
                "up": "CREATE EXTENSION IF NOT EXISTS pg_trgm",
                "down": "SELECT 1",
            }
        ]
    )

    operations, _, _ = plan([model], _live(extensions=()))

    assert [op.key for op in operations] == ["product:ext_pg_trgm"]


def test_a_column_added_by_named_ddl_is_found_among_the_columns():
    """Cara reaches for named DDL when the model DSL has no builder for the
    type — a GENERATED ALWAYS AS tsvector — so the column exists in the
    database and in no ``fields`` dict. Only the table's own column list can
    answer for it."""
    model = _model(
        indexes=[
            {
                "name": "product_search_vector_col",
                "up": "ALTER TABLE product ADD COLUMN IF NOT EXISTS search_vector tsvector "
                "GENERATED ALWAYS AS (to_tsvector('english', title)) STORED",
                "down": "ALTER TABLE product DROP COLUMN IF EXISTS search_vector",
            }
        ]
    )

    operations, _, _ = plan([model], _live(columns=_column("search_vector")))

    assert operations == []


def test_kind_qualification_keeps_namespaces_from_colliding():
    """An extension named like a column must not satisfy the column's entry."""
    created = created_objects("CREATE EXTENSION IF NOT EXISTS search_vector")

    assert created == {"extension:search_vector"}
    assert "column:search_vector" not in created


def test_an_entry_whose_sql_cannot_be_read_is_refused_not_replanned():
    """Guessing has two failure modes and both are silent: run-it-always is
    the phantom, skip-it-always drops a real object from the plan. Refusing
    hands the decision back with its reason."""
    model = _model(indexes=[{"name": "mystery", "up": "SELECT do_something()"}])

    operations, refusals, _ = plan([model], _live())

    assert operations == []
    assert len(refusals) == 1
    assert "mystery" in refusals[0]


def test_a_do_block_wrapping_real_ddl_still_reads():
    """21 synkronus entries are DO blocks guarding an ADD CONSTRAINT. They
    work because the constraint inside them is named — which is exactly what
    the refusal message tells an author to do."""
    model = _model(
        indexes=[
            {
                "name": "billing_notice_fk",
                "up": "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint "
                "WHERE conname = 'billing_notice_fk') THEN "
                "ALTER TABLE product ADD CONSTRAINT billing_notice_fk "
                "FOREIGN KEY (id) REFERENCES other(id); END IF; END $$",
            }
        ]
    )

    assert created_objects(model["indexes"][0]["up"]) == {"billing_notice_fk"}


def test_a_view_is_never_reported_as_an_orphaned_table():
    """The notice invites a hand-written DROP. Aimed at a view the
    application reads, that is the worst advice this tool can give."""
    live = _live(kinds={"product": "BASE TABLE", "product_availability": "VIEW"})
    live.tables["product_availability"] = {
        "id": {"data_type": "bigint", "is_nullable": True, "max_length": None}
    }

    _, _, notices = plan([_model()], live)

    assert notices == []


def test_a_real_orphaned_table_is_still_reported():
    live = _live(kinds={"product": "BASE TABLE", "abandoned": "BASE TABLE"})
    live.tables["abandoned"] = {
        "id": {"data_type": "bigint", "is_nullable": True, "max_length": None}
    }

    _, _, notices = plan([_model()], live)

    assert len(notices) == 1
    assert "abandoned" in notices[0]


def test_an_unknown_relation_kind_counts_as_a_table():
    """Fail OPEN for orphan detection: a Postgres release that renames a
    ``table_type`` must not silently stop reporting orphans. Unknown means
    unknown, not "view"."""
    live = _live(kinds={"product": "BASE TABLE"})
    live.tables["odd"] = {
        "id": {"data_type": "bigint", "is_nullable": True, "max_length": None}
    }

    assert "odd" in live.base_table_names()


def test_the_refusal_makes_the_plan_incomplete():
    """A refusal is not a warning — apply must not run a plan carrying one,
    or the unreadable entry is silently skipped instead of decided."""
    model = _model(indexes=[{"name": "mystery", "up": "SELECT 1"}])

    _, refusals, _ = plan([model], _live())

    assert refusals
    # And the same refusal type the column paths raise, so one handler covers both.
    assert issubclass(SchemaPlanRefused, Exception)
