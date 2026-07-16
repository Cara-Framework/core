"""Direct model assignments must persist framework-owned guarded columns."""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.eloquent.models import Model


class _GuardedModel(Model):
    __table__ = "guarded_model"
    __fillable__ = ["name"]


def test_save_persists_directly_assigned_non_fillable_attributes(monkeypatch) -> None:
    builder = MagicMock()
    builder.where.return_value = builder
    recorded: dict[str, object] = {}

    def update(values, **options):
        recorded["values"] = dict(values)
        recorded["options"] = dict(options)
        return 1

    builder.update.side_effect = update
    monkeypatch.setattr(_GuardedModel, "get_builder", lambda _self: builder)
    model = _GuardedModel.hydrate({"id": 7, "name": "Seller", "role": "user"})

    model.role = "root"

    assert model.save() is True
    assert recorded == {
        "values": {"role": "root"},
        "options": {"cast": False, "ignore_mass_assignment": True},
    }
    assert model.role == "root"
