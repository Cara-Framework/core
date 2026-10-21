"""``schema:check`` named-object + column drift — no DB.

Named objects are compared through ``cara.schema.Objects`` against a
``cara.schema.LiveSchema`` — the planner's own functions and inputs — so a
trigger, a function or a raw foreign key the database lacks is drift here
exactly when ``schema:plan`` would create it. The column diff
(``_diff_table`` / ``_diff_column``) is exercised against plain live dicts.
"""

from __future__ import annotations

from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
from cara.schema import LiveSchema


def _cmd() -> SchemaCheckCommand:
    return SchemaCheckCommand(application=None)


def _live(
    indexes=(),
    checks=(),
    constraints=(),
    constraint_indexes=(),
    triggers=(),
    functions=(),
    table="listing",
):
    """A deployed ``listing`` with the given named objects and no others."""
    return LiveSchema(
        tables={table: {"id": {"data_type": "bigint", "is_nullable": False}}},
        checks={table: set(checks)},
        indexes={table: set(indexes)},
        constraint_indexes={table: set(constraint_indexes)},
        constraints={table: set(checks) | set(constraints)},
        triggers={table: set(triggers)},
        functions=set(functions),
    )


# A model double shaped like ModelDiscoverer output, with the ``__indexes__``
# SQL forms the real Listing model uses.
_LISTING_LIKE = {
    "name": "Listing",
    "table": "listing",
    "indexes": [
        {
            "name": "listing_active_by_product",
            "up": "CREATE INDEX IF NOT EXISTS listing_active_by_product "
            "ON listing (product_id, status) WHERE deleted_at IS NULL",
        },
        {
            "name": "listing_marketplace_external_unique",
            "up": "CREATE UNIQUE INDEX IF NOT EXISTS listing_marketplace_external_unique "
            "ON listing (marketplace_id, external_id) "
            "WHERE marketplace_id IS NOT NULL AND external_id IS NOT NULL",
        },
        {
            "name": "listing_current_price_non_negative",
            "up": "ALTER TABLE listing ADD CONSTRAINT listing_current_price_non_negative "
            "CHECK (current_price IS NULL OR current_price >= 0)",
            "down": "ALTER TABLE listing DROP CONSTRAINT IF EXISTS "
            "listing_current_price_non_negative",
        },
    ],
}

_IN_SYNC = _live(
    indexes={"listing_active_by_product", "listing_marketplace_external_unique"},
    checks={"listing_current_price_non_negative"},
)


# ── declared-but-missing is drift ───────────────────────────────────────────


def test_no_drift_when_every_declared_object_is_present():
    assert _cmd()._diff_objects(_LISTING_LIKE, "listing", _IN_SYNC) == []


def test_dropped_unique_index_is_caught():
    drift = _cmd()._diff_objects(
        _LISTING_LIKE,
        "listing",
        _live(
            indexes={"listing_active_by_product"},  # the unique index was DROPPED
            checks={"listing_current_price_non_negative"},
        ),
    )
    assert drift == [
        "object 'listing_marketplace_external_unique' declared in model but "
        "MISSING in database"
    ]


def test_dropped_check_constraint_is_caught():
    drift = _cmd()._diff_objects(
        _LISTING_LIKE,
        "listing",
        _live(
            indexes={"listing_active_by_product", "listing_marketplace_external_unique"}
        ),  # the CHECK was DROPPED
    )
    assert len(drift) == 1
    assert "listing_current_price_non_negative" in drift[0]
    assert "MISSING" in drift[0]


def test_extra_live_checks_are_not_drift():
    # A model is the source of truth for the CHECKs it asserts; extra live ones
    # (system NOT-NULL constraints among them) must NOT flag.
    live = _live(
        indexes={"listing_active_by_product", "listing_marketplace_external_unique"},
        checks={"listing_current_price_non_negative", "some_system_check"},
    )
    assert _cmd()._diff_objects(_LISTING_LIKE, "listing", live) == []


def test_field_level_check_is_compared_by_its_blueprint_name():
    model = {
        "table": "listing",
        "checks": [{"expression": "quantity >= 0"}],
    }
    drift = _cmd()._diff_objects(model, "listing", _live())
    assert drift == [
        "CHECK constraint 'listing_quantity_0_check' declared in model but "
        "MISSING in database"
    ]
    assert (
        _cmd()._diff_objects(model, "listing", _live(checks={"listing_quantity_0_check"}))
        == []
    )


def test_a_missing_trigger_and_its_function_are_drift():
    """The blind spot this gate had: it harvested only CREATE INDEX and CHECK
    names, so a role-scope guard trigger the database lacked printed "No
    drift" while ``schema:plan`` would have re-created it."""
    model = {
        "table": "invitation",
        "indexes": [
            {
                "name": "invitation_role_scope_guard",
                "up": (
                    "CREATE OR REPLACE FUNCTION enforce_role_tenant_scope() "
                    "RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql; "
                    "CREATE TRIGGER invitation_role_scope_guard BEFORE INSERT ON "
                    "invitation FOR EACH ROW EXECUTE FUNCTION "
                    "enforce_role_tenant_scope()"
                ),
            }
        ],
    }
    drift = _cmd()._diff_objects(model, "invitation", _live(table="invitation"))
    assert drift == [
        "object 'enforce_role_tenant_scope' declared in model but MISSING in database",
        "object 'invitation_role_scope_guard' declared in model but MISSING in database",
    ]

    present = _live(
        table="invitation",
        triggers={"invitation_role_scope_guard"},
        functions={"enforce_role_tenant_scope"},
    )
    assert _cmd()._diff_objects(model, "invitation", present) == []


def test_a_raw_foreign_key_entry_is_compared_as_a_named_object():
    model = {
        "table": "listing",
        "indexes": [
            {
                "name": "listing_channel_tenant_fk",
                "up": (
                    "ALTER TABLE listing ADD CONSTRAINT listing_channel_tenant_fk "
                    "FOREIGN KEY (channel_id, tenant_id) REFERENCES channel (id, tenant_id)"
                ),
            }
        ],
    }
    assert len(_cmd()._diff_objects(model, "listing", _live())) == 1
    assert (
        _cmd()._diff_objects(
            model, "listing", _live(constraints={"listing_channel_tenant_fk"})
        )
        == []
    )


def test_an_unreadable_entry_is_reported_with_the_planners_refusal():
    model = {
        "table": "listing",
        "indexes": [{"name": "listing_mystery", "up": "SELECT 1"}],
    }
    drift = _cmd()._diff_objects(model, "listing", _live())
    assert len(drift) == 1
    assert "listing_mystery" in drift[0]
    assert "cannot tell what this SQL creates" in drift[0]


def test_declared_foreign_key_missing_from_the_database_is_drift():
    model = {
        "table": "listing",
        "fields": {
            "channel_id": {
                "type": "big_integer",
                "params": {},
                "foreign_key": {"field": "channel_id", "on": "listing"},
            }
        },
    }
    drift = _cmd()._diff_objects(model, "listing", _live())
    assert drift == [
        "FOREIGN KEY 'listing_channel_id_foreign' declared in model but MISSING "
        "in database"
    ]


# ── existing column diff stays intact ───────────────────────────────────────


def test_column_diff_still_detects_missing_column():
    cmd = _cmd()
    declared = {"id": {"type": "big_integer", "nullable": False}}
    live = {}  # column missing
    issues = cmd._diff_table("listing", declared, live)
    assert any("MISSING in database" in i for i in issues)


def test_column_diff_still_detects_nullability_mismatch():
    cmd = _cmd()
    declared = {"name": {"type": "string", "nullable": False}}
    live = {"name": {"data_type": "character varying", "is_nullable": True}}
    issues = cmd._diff_table("listing", declared, live)
    assert any("nullability differs" in i for i in issues)


# ── narrower-than-declared capacity (the silent-truncate class) ──────────────
#
# The coarse type categories deliberately blur string/varchar/text, which let
# an undersized live varchar hide behind a widened model forever: a trace
# table kept varchar(100) job ids while real ids ran longer, so Postgres
# rejected long inserts and a fail-open writer silently dropped data.
# The check is one-directional
# (live NARROWER than declared = drift; live wider = fine) so it cannot cry
# wolf on the aliases the categories blur.


def test_unbounded_model_over_bounded_live_column_is_drift():
    cmd = _cmd()
    declared = {"job_id": {"type": "text", "nullable": True, "length": None}}
    live = {
        "job_id": {
            "data_type": "character varying",
            "is_nullable": True,
            "max_length": 100,
        }
    }
    issues = cmd._diff_table("listing", declared, live)
    assert any("NARROWER than declared" in i for i in issues)


def test_live_shorter_than_declared_length_is_drift():
    cmd = _cmd()
    declared = {"title": {"type": "string", "nullable": True, "length": 500}}
    live = {
        "title": {
            "data_type": "character varying",
            "is_nullable": True,
            "max_length": 255,
        }
    }
    issues = cmd._diff_table("listing", declared, live)
    assert any("NARROWER than declared" in i for i in issues)


def test_live_wider_than_declared_is_not_drift():
    cmd = _cmd()
    declared = {"title": {"type": "string", "nullable": True, "length": 255}}
    live = {
        "title": {
            "data_type": "character varying",
            "is_nullable": True,
            "max_length": 500,
        }
    }
    assert cmd._diff_table("listing", declared, live) == []


def test_matching_bounds_are_not_drift():
    cmd = _cmd()
    declared = {"title": {"type": "string", "nullable": True, "length": 255}}
    live = {
        "title": {
            "data_type": "character varying",
            "is_nullable": True,
            "max_length": 255,
        }
    }
    assert cmd._diff_table("listing", declared, live) == []


def test_unbounded_both_sides_is_not_drift():
    cmd = _cmd()
    declared = {"body": {"type": "text", "nullable": True, "length": None}}
    live = {"body": {"data_type": "text", "is_nullable": True, "max_length": None}}
    assert cmd._diff_table("listing", declared, live) == []


# ── timezone drift: naive↔aware must be visible AND actionable ──────────────
#
# The two live TIMESTAMP variants used to share one coarse "datetime" category,
# so a naive created_at under a model declaring a tz-aware datetime was
# reported as NO drift at all. That is how a whole database of naive audit
# stamps went unnoticed until a COALESCE index refused to build.


def test_naive_live_column_under_aware_model_is_drift():
    cmd = _cmd()
    declared = {"created_at": {"type": "datetime", "nullable": True}}
    live = {
        "created_at": {
            "data_type": "timestamp without time zone",
            "is_nullable": True,
        }
    }
    issues = cmd._diff_table("user_session", declared, live)
    assert len(issues) == 1
    assert "timezone-NAIVE" in issues[0]


def test_naive_drift_message_carries_the_exact_repair_alter():
    cmd = _cmd()
    declared = {"created_at": {"type": "datetime", "nullable": True}}
    live = {
        "created_at": {
            "data_type": "timestamp without time zone",
            "is_nullable": True,
        }
    }
    message = cmd._diff_table("user_session", declared, live)[0]
    assert (
        "ALTER TABLE user_session ALTER COLUMN created_at TYPE timestamptz "
        "USING created_at AT TIME ZONE 'UTC';" in message
    )


def test_aware_live_column_under_naive_model_is_drift_without_a_lossy_default():
    cmd = _cmd()
    declared = {"wall_clock": {"type": "timestamp", "nullable": True}}
    live = {"wall_clock": {"data_type": "timestamp with time zone", "is_nullable": True}}
    message = cmd._diff_table("widget", declared, live)[0]
    assert "timezone-AWARE" in message
    # Narrowing discards the offset, so the message must say so rather than
    # hand out a repair statement as if it were free.
    assert "DISCARDS the offset" in message


def test_matching_timezone_awareness_is_not_drift():
    cmd = _cmd()
    aware = cmd._diff_table(
        "widget",
        {"created_at": {"type": "datetime", "nullable": True}},
        {"created_at": {"data_type": "timestamp with time zone", "is_nullable": True}},
    )
    naive = cmd._diff_table(
        "widget",
        {"wall_clock": {"type": "timestamp", "nullable": True}},
        {"wall_clock": {"data_type": "timestamp without time zone", "is_nullable": True}},
    )
    assert aware == [] and naive == []


# ── index drift in BOTH directions ──────────────────────────────────────────


def test_index_living_only_in_a_hand_written_migration_is_reported_as_extra():
    # The 37-index class: present in the DB, declared by no model, therefore
    # silently dropped by the next regenerate-from-models sweep.
    live = _live(
        indexes={
            "listing_active_by_product",
            "listing_marketplace_external_unique",
            "listing_hand_written_perf_idx",
        },
        checks={"listing_current_price_non_negative"},
    )
    drift = _cmd()._diff_objects(_LISTING_LIKE, "listing", live)
    assert len(drift) == 1
    assert "listing_hand_written_perf_idx" in drift[0]
    assert "NOT declared in model" in drift[0]


def test_constraint_backed_indexes_are_not_reported_as_extra():
    # Postgres names the index behind a PK/UNIQUE constraint itself; no model
    # declares it, so counting it would make every table report phantom extras.
    live = _live(
        indexes={
            "listing_active_by_product",
            "listing_marketplace_external_unique",
            "listing_pkey",
            "listing_sku_unique",
        },
        checks={"listing_current_price_non_negative"},
        constraint_indexes={"listing_pkey", "listing_sku_unique"},
    )
    assert _cmd()._diff_objects(_LISTING_LIKE, "listing", live) == []


def test_composite_index_declarations_are_declared_indexes():
    # ``field.index(["a", "b"])`` never reaches ``__indexes__``; its live name is
    # the Blueprint default unless the model named it.
    model = {
        "table": "listing",
        "composite_indexes": [
            {"columns": ["product_id", "status"], "name": None},
            {"columns": ["sku"], "name": "listing_sku_lookup"},
        ],
    }
    live = _live(indexes={"listing_product_id_status_index", "listing_sku_lookup"})
    assert _cmd()._diff_objects(model, "listing", live) == []


def test_index_diff_reports_missing_and_extra_together():
    live = _live(
        indexes={"listing_active_by_product", "an_undeclared_index"},
        checks={"listing_current_price_non_negative"},
    )
    drift = _cmd()._diff_objects(_LISTING_LIKE, "listing", live)
    assert len(drift) == 2
    assert any(
        "listing_marketplace_external_unique" in d and "MISSING" in d for d in drift
    )
    assert any("an_undeclared_index" in d and "NOT declared" in d for d in drift)
