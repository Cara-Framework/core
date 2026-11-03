"""An index whose DEFINITION changed under an unchanged name.

``schema:check`` and ``schema:plan`` compared indexes by NAME only, so a
changed definition was invisible (Synkronus dev, 2026-11-03):
``message_thread_assignee_idx`` was declared on ``(assigned_to_user_id,
tenant_id)``, the database held it on ``(tenant_id, assigned_to_user_id)``,
``schema:check`` said "No drift" and ``schema:plan`` said "Nothing to do".
``schema:verify`` — a from-zero database compared by ``schema:check`` —
inherited the blindness.

The declaration is now spelled by the SERVER (built on a session TEMP clone
of the table and read back with ``pg_get_indexdef``) and compared to the live
index's own ``pg_get_indexdef``, so a blueprint index and a raw
``__indexes__`` statement are compared the same exact way.
"""

from __future__ import annotations

from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
from cara.schema import (
    LiveSchema,
    declared_indexes,
    drifted_indexes,
    index_shape,
    introspect,
    missing_indexes,
    normalize_declared_indexes,
    orphaned_indexes,
    plan,
    reshape_index_name,
)

_TABLE = "message_threads"
_NAME = "message_thread_assignee_idx"
_LIVE = (
    f"CREATE INDEX {_NAME} ON public.{_TABLE} "
    "USING btree (tenant_id, assigned_to_user_id)"
)
_DECLARED = (
    f"CREATE INDEX {_NAME} ON pg_temp_3._cara_index_probe_1 "
    "USING btree (assigned_to_user_id, tenant_id)"
)
#: The declaration spelled the way the database already has it.
_SAME = (
    f"CREATE INDEX {_NAME} ON pg_temp_3._cara_index_probe_1 "
    "USING btree (tenant_id, assigned_to_user_id)"
)


def _model(table=_TABLE, *, composite=(), uniques=(), indexes=()):
    return {
        "table": table,
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}},
        "indexes": list(indexes),
        "composite_indexes": [
            {"columns": list(columns), "name": name} for name, columns in composite
        ],
        "composite_uniques": [
            {"columns": list(columns), "name": name} for name, columns in uniques
        ],
    }


def _live(
    table=_TABLE,
    *,
    definitions=None,
    declared=None,
    invalid=(),
    uniques=(),
    constraint_definitions=None,
):
    definitions = dict(definitions or {})
    return LiveSchema(
        tables={
            table: {
                "id": {"data_type": "bigint", "is_nullable": False, "max_length": None},
            }
        },
        checks={},
        indexes={table: set(definitions)},
        constraint_indexes={table: set(uniques)},
        constraints={table: set(uniques)},
        relation_kinds={table: "BASE TABLE"},
        constraint_definitions={table: dict(constraint_definitions or {})},
        unique_constraints={table: set(uniques)},
        index_definitions={table: definitions},
        invalid_indexes={table: set(invalid)},
        declared_index_definitions={table: dict(declared or {})},
    )


def _assignee():
    return _model(composite=[(_NAME, ["assigned_to_user_id", "tenant_id"])])


def test_a_reordered_index_is_built_concurrently_then_swapped_in():
    """The MessageThread repro: same name, columns in a different order."""
    live = _live(definitions={_NAME: _LIVE}, declared={_NAME: _DECLARED})

    operations, refusals, _ = plan([_assignee()], live)

    assert refusals == []
    assert [op.kind for op in operations] == ["reshape_index", "swap_index"]
    build, swap = operations
    temp = reshape_index_name(_NAME, index_shape(_DECLARED))
    assert len(temp) <= 63 and temp != _NAME
    assert build.forward_sql == (
        f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {temp} ON "{_TABLE}" '
        '("assigned_to_user_id", "tenant_id")'
    )
    assert build.reverse_sql == f"DROP INDEX CONCURRENTLY IF EXISTS {temp}"
    assert build.safety == "additive" and build.transactional is False
    # One transaction: the table is never without an index of that name.
    assert swap.forward_sql == (
        f"DO $$ BEGIN DROP INDEX {_NAME}; ALTER INDEX {temp} RENAME TO {_NAME}; END $$"
    )
    assert swap.transactional is True and swap.safety == "locking"
    # The reverse puts the database's own definition back.
    assert swap.reverse_sql == (
        f"DO $$ BEGIN ALTER INDEX {_NAME} RENAME TO {temp}; {_LIVE}; END $$"
    )
    assert swap.restores_data is True


def test_the_reorder_is_reported_by_schema_check():
    live = _live(definitions={_NAME: _LIVE}, declared={_NAME: _DECLARED})

    issues = SchemaCheckCommand._diff_objects(object(), _assignee(), _TABLE, live)

    assert len(issues) == 1
    assert f"index '{_NAME}'" in issues[0]
    assert "(tenant_id, assigned_to_user_id)" in issues[0]
    assert "(assigned_to_user_id, tenant_id)" in issues[0]


def test_the_same_definition_spelled_by_the_server_is_not_a_change():
    live = _live(definitions={_NAME: _LIVE}, declared={_NAME: _SAME})

    operations, refusals, _ = plan([_assignee()], live)

    assert (operations, refusals) == ([], [])
    assert SchemaCheckCommand._diff_objects(object(), _assignee(), _TABLE, live) == []


def test_an_index_the_probe_could_not_spell_is_never_called_changed():
    live = _live(definitions={_NAME: _LIVE})

    assert plan([_assignee()], live) == ([], [], [])


def test_a_unique_raw_index_rebuild_keeps_the_duplicate_preflight():
    name = "listing_channel_external_unique"
    up = (
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {name} "
        "ON listing (channel_id, external_id) WHERE deleted_at IS NULL"
    )
    live_def = (
        f"CREATE UNIQUE INDEX {name} ON public.listing USING btree "
        "(external_id) WHERE (deleted_at IS NULL)"
    )
    declared_def = (
        f"CREATE UNIQUE INDEX {name} ON pg_temp_3.probe USING btree "
        "(channel_id, external_id) WHERE (deleted_at IS NULL)"
    )
    model = _model("listing", indexes=[{"name": name, "up": up, "down": "SELECT 1"}])
    live = _live("listing", definitions={name: live_def}, declared={name: declared_def})

    operations, refusals, _ = plan([model], live)

    assert refusals == []
    build, swap = operations
    temp = reshape_index_name(name, index_shape(declared_def))
    # The model's own SQL, retargeted to the temporary name.
    assert build.forward_sql == (
        f'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {temp} ON "listing" '
        "(channel_id, external_id) WHERE deleted_at IS NULL"
    )
    assert build.preflight_sql == (
        'SELECT 1 FROM "listing" WHERE (channel_id, external_id) IS NOT NULL '
        "AND (deleted_at IS NULL) GROUP BY channel_id, external_id "
        "HAVING COUNT(*) > 1 LIMIT 1"
    )
    # A foreign key pointing at the old unique index blocks its drop.
    assert "contype = 'f'" in swap.preflight_sql
    assert f"to_regclass('{name}')" in swap.preflight_sql


def test_a_blueprint_unique_constraint_is_swapped_by_using_index():
    name = "channel_tenant_slug_unique"
    model = _model("channel", uniques=[(name, ["slug", "tenant_id"])])
    live = _live(
        "channel",
        definitions={
            name: f"CREATE UNIQUE INDEX {name} ON public.channel USING btree (tenant_id, slug)"
        },
        declared={
            name: f"CREATE UNIQUE INDEX {name} ON pg_temp_3.probe USING btree (slug, tenant_id)"
        },
        uniques=(name,),
        constraint_definitions={name: "UNIQUE (tenant_id, slug)"},
    )

    operations, refusals, _ = plan([model], live)

    assert refusals == []
    assert [op.kind for op in operations] == ["reshape_index", "swap_index"]
    build, swap = operations
    temp = reshape_index_name(name, "UNIQUE USING btree (slug, tenant_id)")
    assert build.forward_sql.startswith(
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {temp}"
    )
    assert build.preflight_sql is not None
    assert swap.forward_sql == (
        f'ALTER TABLE "channel" DROP CONSTRAINT {name}, '
        f"ADD CONSTRAINT {name} UNIQUE USING INDEX {temp}"
    )
    assert swap.reverse_sql == (
        f'ALTER TABLE "channel" DROP CONSTRAINT {name}, '
        f"ADD CONSTRAINT {name} UNIQUE (tenant_id, slug)"
    )


def test_a_drifted_bare_unique_is_not_adopted_before_its_reshape():
    """Adopting the OLD index as the constraint would make the swap's DROP
    INDEX fail on an index the constraint now requires."""
    name = "channel_tenant_slug_unique"
    model = _model("channel", uniques=[(name, ["slug", "tenant_id"])])
    live = _live(
        "channel",
        definitions={
            name: f"CREATE UNIQUE INDEX {name} ON public.channel USING btree (tenant_id, slug)"
        },
        declared={
            name: f"CREATE UNIQUE INDEX {name} ON pg_temp_3.probe USING btree (slug, tenant_id)"
        },
    )

    assert missing_indexes(model, "channel", live) == []
    operations, _, _ = plan([model], live)
    swap = operations[-1]
    temp = reshape_index_name(name, "UNIQUE USING btree (slug, tenant_id)")
    assert swap.forward_sql == (
        f"DO $$ BEGIN DROP INDEX {name}; "
        f'ALTER TABLE "channel" ADD CONSTRAINT {name} UNIQUE USING INDEX {temp}; END $$'
    )


def test_an_invalid_index_with_the_declared_shape_is_rebuilt_in_place():
    """An interrupted CONCURRENTLY build leaves an INVALID index: maintained on
    every write, read by no query, and invisible to a name comparison."""
    model = _model(composite=[(_NAME, ["tenant_id", "assigned_to_user_id"])])
    live = _live(definitions={_NAME: _LIVE}, declared={_NAME: _SAME}, invalid=(_NAME,))

    operations, refusals, _ = plan([model], live)

    assert refusals == []
    assert [(op.kind, op.forward_sql) for op in operations] == [
        ("rebuild_index", f"REINDEX INDEX CONCURRENTLY {_NAME}")
    ]
    assert operations[0].transactional is False
    issues = SchemaCheckCommand._diff_objects(object(), model, _TABLE, live)
    assert len(issues) == 1 and "INVALID" in issues[0]


def test_an_interrupted_reshape_resumes_from_its_built_index():
    temp = reshape_index_name(_NAME, index_shape(_DECLARED))
    built = f"CREATE INDEX {temp} ON public.{_TABLE} USING btree (assigned_to_user_id, tenant_id)"
    live = _live(definitions={_NAME: _LIVE, temp: built}, declared={_NAME: _DECLARED})

    operations, _, _ = plan([_assignee()], live)

    # Already built: only the swap is left, and the build is not an orphan.
    assert [op.kind for op in operations] == ["swap_index"]
    assert orphaned_indexes(_assignee(), _TABLE, live) == []


def test_an_invalid_reshape_build_is_rebuilt_before_the_swap():
    temp = reshape_index_name(_NAME, index_shape(_DECLARED))
    built = f"CREATE INDEX {temp} ON public.{_TABLE} USING btree (assigned_to_user_id, tenant_id)"
    live = _live(
        definitions={_NAME: _LIVE, temp: built},
        declared={_NAME: _DECLARED},
        invalid=(temp,),
    )

    operations, _, _ = plan([_assignee()], live)

    assert [(op.kind, op.forward_sql) for op in operations] == [
        ("reshape_index", f"REINDEX INDEX CONCURRENTLY {temp}"),
        ("swap_index", operations[1].forward_sql),
    ]


def test_named_ddl_entries_yield_their_index_statements_only():
    model = _model(
        indexes=[
            {
                "name": "message_thread_counterparty_lower_idx",
                "up": (
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                    "message_thread_counterparty_lower_idx "
                    f"ON {_TABLE} (LOWER(counterparty)) "
                    "WHERE counterparty IS NOT NULL"
                ),
            },
            {
                "name": "guard_trigger",
                "up": (
                    "CREATE OR REPLACE FUNCTION guard() RETURNS trigger AS $$ "
                    "BEGIN RETURN NEW; END $$ LANGUAGE plpgsql; "
                    f"CREATE TRIGGER guard_trigger BEFORE UPDATE ON {_TABLE} "
                    "FOR EACH ROW EXECUTE FUNCTION guard()"
                ),
            },
            {
                "name": "elsewhere_idx",
                "up": "CREATE INDEX IF NOT EXISTS elsewhere_idx ON other_table (a); SELECT 1",
            },
        ]
    )

    found = declared_indexes(model, _TABLE)

    assert [(i.name, i.unique, i.rest) for i in found] == [
        (
            "message_thread_counterparty_lower_idx",
            False,
            " (LOWER(counterparty)) WHERE counterparty IS NOT NULL",
        )
    ]


def test_introspection_reads_every_index_definition_and_validity():
    class _Executor:
        def get_query_result(self, sql):
            if "FROM pg_indexes" in sql:
                assert "indexdef" in sql
                return [{"table_name": _TABLE, "index_name": _NAME, "definition": _LIVE}]
            if "indisvalid" in sql:
                return [{"table_name": _TABLE, "index_name": _NAME}]
            return []

    class _Schema:
        query_executor = _Executor()

        def get_schema(self):
            return "public"

    live = introspect(_Schema())

    assert live.indexes == {_TABLE: {_NAME}}
    assert live.index_definitions == {_TABLE: {_NAME: _LIVE}}
    assert live.invalid_indexes == {_TABLE: {_NAME}}


#: ``status IN ('pending', 'sending')`` on a varchar column, as the server first
#: stores it, and as it reads after one more parse — what an index rebuilt by
#: ALTER COLUMN TYPE or restored from a dump holds (Synkronus dev, 2026-11-03:
#: ``notification_destination_delivery_open_index``).
_OPEN = "notification_destination_delivery_open_index"
_ARRAY_CAST = (
    "CREATE INDEX {label} ON {table} USING btree (not_before, id) WHERE "
    "((status)::text = ANY ((ARRAY['pending'::character varying, "
    "'sending'::character varying])::text[]))"
)
_ELEMENT_CAST = (
    "CREATE INDEX {label} ON {table} USING btree (not_before, id) WHERE "
    "((status)::text = ANY (ARRAY[('pending'::character varying)::text, "
    "('sending'::character varying)::text]))"
)


def _probe_schema(sent: list[str]):
    """A live schema whose one connection plays the probe: the first spelling
    of a declaration has the array cast, every re-spelling the element cast."""

    class _Connection:
        def query(self, sql, bindings=()):
            sent.append(sql)
            if sql.startswith("SELECT pg_get_indexdef"):
                label = bindings[0].split('"')[1]
                template = _ARRAY_CAST if label.endswith("_d0") else _ELEMENT_CAST
                return [{"definition": template.format(label=label, table="pg_temp_3.p")}]
            return 0

    class _Manager:
        released = False

        def create_connection_instance(self):
            return _Connection()

        def release(self, connection):
            _Manager.released = True

    class _Executor:
        connection_manager = _Manager()

    class _Schema:
        query_executor = _Executor()

        def get_schema(self):
            return "public"

    return _Schema(), _Manager


def test_the_probe_compares_both_sides_at_the_server_spelling_fixpoint():
    table = "notification_destination_delivery"
    up = (
        f"CREATE INDEX IF NOT EXISTS {_OPEN} ON {table} (not_before, id) "
        "WHERE status IN ('pending', 'sending')"
    )
    model = _model(table, indexes=[{"name": _OPEN, "up": up}])
    stored = _ELEMENT_CAST.format(label=_OPEN, table=f"public.{table}")
    live = _live(table, definitions={_OPEN: stored})
    declared = {
        table: [
            *[(i.name, i.unique, i.rest) for i in declared_indexes(model, table)],
            ("not_in_the_database_idx", False, ' ("id")'),
        ]
    }
    sent: list[str] = []
    schema, manager = _probe_schema(sent)

    spelled = normalize_declared_indexes(schema, live, declared)

    # The same index, whatever its history: no drift, no reshape.
    assert drifted_indexes(model, table, spelled) == []
    assert plan([model], spelled) == ([], [], [])
    assert "ARRAY[('pending'" in spelled.declared_index_definitions[table][_OPEN]
    assert "ARRAY[('pending'" in spelled.respelled_index_definitions[table][_OPEN]
    assert sent[0].startswith("CREATE TEMP TABLE")
    assert f'(LIKE "public"."{table}")' in sent[0]
    # The declaration, its first spelling, and the live definition — each
    # built once; a name the database lacks is not spelled at all.
    creates = [sql for sql in sent if sql.startswith("CREATE INDEX")]
    assert len(creates) == 3
    assert creates[0].endswith(" (not_before, id) WHERE status IN ('pending', 'sending')")
    assert "::text[]" in creates[1] and "ARRAY[('pending'" in creates[2]
    assert all("CONCURRENTLY" not in sql for sql in creates)
    assert sent[-1].startswith("DROP TABLE IF EXISTS")
    assert manager.released


def test_a_single_spelling_would_have_called_the_same_index_changed():
    """Why the fixpoint exists: compared after ONE parse, the stored element
    cast and the declared array cast differ although they mean the same."""
    first = _ARRAY_CAST.format(label=_OPEN, table="pg_temp_3.p")
    stored = _ELEMENT_CAST.format(label=_OPEN, table="public.t")

    assert index_shape(first) != index_shape(stored)
    assert index_shape(_ELEMENT_CAST.format(label="x", table="p")) == index_shape(stored)
