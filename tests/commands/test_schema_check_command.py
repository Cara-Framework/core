"""``schema:check`` constraint + unique-index drift — parsing + diff.

Exercises the pure model-side parsing (``_declared_check_constraints`` /
``_declared_unique_indexes``) and the new ``_diff_constraints_and_indexes`` diff
against controlled "live" sets, with no DB. The existing column diff
(``_diff_table`` / ``_diff_column``) is also smoke-tested to confirm it stays
intact.
"""

from __future__ import annotations

from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand


def _cmd() -> SchemaCheckCommand:
    return SchemaCheckCommand(application=None)


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


# ── declared-constraint / declared-index parsing ───────────────────────────


def test_declared_check_constraints_extracts_name():
    checks = SchemaCheckCommand._declared_check_constraints(_LISTING_LIKE)
    assert checks == {"listing_current_price_non_negative"}


def test_declared_unique_indexes_extracts_unique_only():
    # The plain ``CREATE INDEX`` must NOT be reported as a unique index; only
    # the ``CREATE UNIQUE INDEX`` is.
    indexes = SchemaCheckCommand._declared_unique_indexes(_LISTING_LIKE)
    assert indexes == {"listing_marketplace_external_unique"}


def test_plain_index_is_not_a_check_or_unique():
    model = {
        "indexes": [
            {
                "name": "listing_active_by_product",
                "up": "CREATE INDEX IF NOT EXISTS listing_active_by_product "
                "ON listing (product_id, status)",
            }
        ]
    }
    assert SchemaCheckCommand._declared_check_constraints(model) == set()
    assert SchemaCheckCommand._declared_unique_indexes(model) == set()


def test_no_indexes_attribute_is_empty():
    assert SchemaCheckCommand._declared_check_constraints({}) == set()
    assert SchemaCheckCommand._declared_unique_indexes({}) == set()


# ── diff: declared-but-missing is drift ─────────────────────────────────────


def test_no_drift_when_constraint_and_index_present():
    cmd = _cmd()
    drift = cmd._diff_constraints_and_indexes(
        _LISTING_LIKE,
        live_checks={"listing_current_price_non_negative"},
        live_indexes={"listing_marketplace_external_unique", "listing_active_by_product"},
    )
    assert drift == []


def test_dropped_unique_index_is_caught():
    cmd = _cmd()
    drift = cmd._diff_constraints_and_indexes(
        _LISTING_LIKE,
        live_checks={"listing_current_price_non_negative"},
        live_indexes=set(),  # the unique index was DROPPED
    )
    assert len(drift) == 1
    assert "listing_marketplace_external_unique" in drift[0]
    assert "unique index" in drift[0] and "MISSING" in drift[0]


def test_dropped_check_constraint_is_caught():
    cmd = _cmd()
    drift = cmd._diff_constraints_and_indexes(
        _LISTING_LIKE,
        live_checks=set(),  # the CHECK was DROPPED
        live_indexes={"listing_marketplace_external_unique"},
    )
    assert len(drift) == 1
    assert "listing_current_price_non_negative" in drift[0]
    assert "CHECK constraint" in drift[0] and "MISSING" in drift[0]


def test_both_dropped_reports_both():
    cmd = _cmd()
    drift = cmd._diff_constraints_and_indexes(
        _LISTING_LIKE, live_checks=set(), live_indexes=set()
    )
    assert len(drift) == 2


def test_extra_live_constraints_are_not_drift():
    # A model is the source of truth for ITS declared invariants. Extra live
    # constraints/indexes (hand-added, system NOT-NULL checks) must NOT flag.
    cmd = _cmd()
    drift = cmd._diff_constraints_and_indexes(
        _LISTING_LIKE,
        live_checks={"listing_current_price_non_negative", "some_system_check"},
        live_indexes={
            "listing_marketplace_external_unique",
            "listing_active_by_product",
            "an_extra_perf_index",
        },
    )
    assert drift == []


# ── existing column diff stays intact ───────────────────────────────────────


def test_column_diff_still_detects_missing_column():
    cmd = _cmd()
    declared = {"id": {"type": "big_integer", "nullable": False}}
    live = {}  # column missing
    issues = cmd._diff_table(declared, live)
    assert any("MISSING in database" in i for i in issues)


def test_column_diff_still_detects_nullability_mismatch():
    cmd = _cmd()
    declared = {"name": {"type": "string", "nullable": False}}
    live = {"name": {"data_type": "character varying", "is_nullable": True}}
    issues = cmd._diff_table(declared, live)
    assert any("nullability differs" in i for i in issues)
