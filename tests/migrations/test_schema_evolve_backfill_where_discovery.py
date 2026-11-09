"""``backfill_from(where=...)`` reaches the planner the way a model's source says."""

from __future__ import annotations

from cara.eloquent.schema import Schema

from ._fixtures import write_model


def test_the_builder_carries_the_where():
    (definition,) = Schema.build(
        lambda field: (
            field.datetime("closed_at")
            .nullable()
            .backfill_from("seed", where="status = 'x'"),
        )
    )

    assert definition.to_dict()["params"]["backfill_where"] == "status = 'x'"


def test_a_model_source_with_a_where_is_discovered(discoverer, tmp_path):
    src = """
        from cara.eloquent.schema import Schema

        class Widget(Model):
            __table__ = "widget"

            @property
            def fields(self):
                return Schema.build(
                    lambda field: (
                        field.big_increments("id"),
                        field.datetime("closed_at").nullable().backfill_from(
                            "seed", where="status = 'x'"
                        ),
                    )
                )
    """
    info = discoverer._parse_model_file(write_model(tmp_path, "Widget.py", src))

    params = info["fields"]["closed_at"]["params"]
    assert params["backfill_from"] == "seed"
    assert params["backfill_where"] == "status = 'x'"
