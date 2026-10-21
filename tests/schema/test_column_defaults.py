"""Column DEFAULTs: declared vs deployed, compared and planned as values.

A from-zero database gets every declared default from its generated
migration; an evolved one only gets what the planner derives. Nothing read
``column_default`` before this, so a nullable ``.default("new")`` column landed
with no default, a changed default never moved, and both gates called the
two databases identical while their INSERTs disagreed.
"""

from __future__ import annotations

import pytest

from cara.exceptions import SchemaPlanRefused
from cara.schema import (
    ADDITIVE,
    LiveSchema,
    declared_columns,
    declared_default_sql,
    defaults_match,
    live_default_sql,
    plan,
)


def _col(data_type="character varying", nullable=True, max_length=255, default=None):
    return {
        "data_type": data_type,
        "is_nullable": nullable,
        "max_length": max_length,
        "default": default,
    }


def _live(columns):
    return LiveSchema(
        tables={
            "product": {
                "id": _col("bigint", False, None, "nextval('product_id_seq'::regclass)"),
                **columns,
            }
        },
        checks={},
        indexes={},
        constraint_indexes={},
    )


def _model(fields):
    return {
        "name": "Product",
        "table": "product",
        "has_fields_method": True,
        "fields": {"id": {"type": "big_increments", "params": {}}, **fields},
        "indexes": [],
    }


def _field(field_type, **params):
    return {"type": field_type, "params": params}


# ── the two spellings of one value ──────────────────────────────────────────


@pytest.mark.parametrize(
    ("params", "live"),
    [
        ({"default": "pending"}, "'pending'::character varying"),
        ({"default": "it's"}, "'it''s'::text"),
        ({"default": "US"}, "'US'::bpchar"),
        ({"default": "[]"}, "'[]'::jsonb"),
        ({"default": 0}, "0"),
        ({"default": -1, "default_is_raw": True}, "'-1'::integer"),
        ({"default": 1.0}, "1.0"),
        ({"default": 1.0}, "1"),
        ({"default": False}, "false"),
        ({"default": True}, "true"),
        ({"use_current": True}, "CURRENT_TIMESTAMP"),
        ({"use_current": True}, "now()"),
        ({"default": "now"}, "now()"),
        ({}, None),
        # A serial column's sequence is the column type's, not a default.
        ({}, "nextval('product_id_seq'::regclass)"),
    ],
)
def test_one_value_in_two_spellings_is_no_drift(params, live):
    assert defaults_match(declared_default_sql(params), live_default_sql(live))


@pytest.mark.parametrize(
    ("params", "live"),
    [
        ({"default": "pending"}, "'draft'::character varying"),
        ({"default": "new"}, None),
        ({}, "'new'::character varying"),
        ({"default": 0}, "1"),
        ({"default": False}, "true"),
        ({"default": "now()"}, "now()"),
    ],
)
def test_a_different_value_is_drift(params, live):
    assert not defaults_match(declared_default_sql(params), live_default_sql(live))


def test_a_python_expression_default_is_refused_not_rendered():
    """The discoverer keeps an unresolvable default as SOURCE text. Rendering
    ``SomeEnum.X`` into DDL would be guessing at what it evaluates to."""
    with pytest.raises(SchemaPlanRefused, match="Python expression"):
        declared_default_sql({"default": "Status.ACTIVE", "default_is_raw": True})


def test_timestamps_carry_the_blueprint_default():
    """``timestamps()`` stamps both columns DEFAULT CURRENT_TIMESTAMP, so the
    expanded columns must say so or every table reads as default drift."""
    columns = declared_columns(_model({"timestamps": _field("timestamps")}))
    assert declared_default_sql(columns["created_at"]["params"]) == "CURRENT_TIMESTAMP"
    assert declared_default_sql(columns["updated_at"]["params"]) == "CURRENT_TIMESTAMP"


# ── the planner repairs what the comparison finds ───────────────────────────


def test_a_changed_default_is_planned_as_an_additive_set_default():
    model = _model({"status": _field("string", length=20, default="pending")})
    operations, refusals, _ = plan(
        [model],
        _live({"status": _col(nullable=False, default="'draft'::character varying")}),
    )

    assert refusals == []
    assert len(operations) == 1
    operation = operations[0]
    assert operation.kind == "set_default"
    assert operation.safety == ADDITIVE
    assert operation.forward_sql == (
        'ALTER TABLE "product" ALTER COLUMN "status" SET DEFAULT \'pending\''
    )
    # The server's own spelling of the old default: an exact restore.
    assert operation.reverse_sql == (
        'ALTER TABLE "product" ALTER COLUMN "status" SET DEFAULT '
        "'draft'::character varying"
    )
    assert operation.restores_data is True


def test_a_default_the_model_no_longer_declares_is_dropped():
    model = _model({"status": _field("string", length=20, nullable=True)})
    operations, _, _ = plan(
        [model], _live({"status": _col(default="'draft'::character varying")})
    )
    assert [op.kind for op in operations] == ["drop_default"]
    assert operations[0].forward_sql.endswith('"status" DROP DEFAULT')


def test_a_matching_default_plans_nothing():
    model = _model(
        {
            "status": _field("string", length=20, default="pending"),
            "attempts": _field("integer", default=0),
            "timestamps": _field("timestamps"),
        }
    )
    live = _live(
        {
            "status": _col(nullable=False, default="'pending'::character varying"),
            "attempts": _col("integer", False, None, "0"),
            "created_at": _col(
                "timestamp with time zone", True, None, "CURRENT_TIMESTAMP"
            ),
            "updated_at": _col("timestamp with time zone", True, None, "now()"),
        }
    )
    operations, refusals, _ = plan([model], live)
    assert refusals == []
    assert operations == []


def test_a_new_column_with_an_unrenderable_default_is_refused():
    model = _model(
        {"status": _field("string", length=20, default="Status.X", default_is_raw=True)}
    )
    operations, refusals, _ = plan([model], _live({}))
    assert operations == []
    assert "Python expression" in refusals[0]


# ── schema:check reports what the planner would repair or refuse ────────────


def _checked(params, live_default, nullable=False):
    from cara.commands.core._SchemaColumnDiff import _SCHEMA_COLUMN_DIFF

    declared = {"type": "string", "nullable": nullable, "length": 3, "params": params}
    return _SCHEMA_COLUMN_DIFF.diff_column(
        "listing",
        "currency",
        declared,
        _col(nullable=nullable, max_length=3, default=live_default),
    )


def test_schema_check_reports_a_default_the_database_kept_after_the_model_dropped_it():
    """The dev database still carried ``DEFAULT 'USD'`` on columns whose
    models had dropped it on purpose: an INSERT omitting the currency silently
    got USD there and failed on a from-zero database."""
    assert _checked({}, "'USD'::character varying") == [
        "column 'currency' default differs: model=none, db='USD'"
    ]
    assert _checked({"default": "USD"}, "'USD'::character varying") == []


def test_schema_check_reports_an_unrenderable_default_with_the_planners_refusal():
    """Skipped here while ``schema:plan`` refused it, the two gates disagreed
    about the same column on every run."""
    (issue,) = _checked({"default": "Status.X", "default_is_raw": True}, None)
    assert issue.startswith("column 'currency': ")
    assert "Python expression" in issue
