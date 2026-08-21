"""ModelQueryDiscipline: model queries belong to repositories (DOCTRINE §5)."""

from __future__ import annotations

from pathlib import Path

from cara.architecture.scanners.ModelQueryDiscipline import ModelQueryDiscipline

from ._fixtures import make_manifest, write

IMPORT = "from app.models import Widget\n"


def _messages(root: Path, **overrides) -> list[str]:
    return [
        str(finding)
        for finding in ModelQueryDiscipline.scan(make_manifest(root, **overrides))
    ]


def test_a_direct_builder_call_on_an_imported_model_is_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n\ndef run():\n    return Widget.where('status', 'active').get()\n",
    )

    assert any("Widget.where(...)" in message for message in _messages(tmp_path))


def test_the_whole_chain_counts_not_just_its_head(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n\ndef run():\n    return Widget.without_scope().first()\n",
    )

    # ``without_scope`` is not an ORM method, so a head-only scanner sees
    # nothing here — yet the reach is identical to ``Widget.first()``.
    assert any("Widget.first(...)" in message for message in _messages(tmp_path))


def test_a_receiver_that_is_not_an_imported_model_is_left_alone(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "def run(payload, widget):\n"
        "    payload.get('id')\n"
        "    widget.save()\n"
        "    return payload.first\n",
    )

    assert _messages(tmp_path) == []


def test_a_nested_declared_repository_home_is_not_policed(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "gates" / "persistence" / "WidgetRepository.py",
        IMPORT + "\n\ndef all_rows():\n    return Widget.where('active', True).get()\n",
    )

    assert (
        _messages(
            tmp_path,
            raw_sql_homes=frozenset({"app/gates/persistence"}),
        )
        == []
    )


def test_a_single_argument_primary_key_lookup_is_the_documented_carve_out(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n\ndef run(widget_id):\n    return Widget.find(widget_id)\n",
    )

    assert _messages(tmp_path) == []


def test_a_filtered_find_is_not_a_primary_key_lookup(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n\ndef run(a, b):\n    return Widget.find(a, b)\n",
    )

    assert _messages(tmp_path) != []


def test_an_owner_fence_inside_a_locking_transaction_stays_put(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Claim.py",
        IMPORT + "from cara.facades import DB\n"
        "\n"
        "\n"
        "def claim(widget_id):\n"
        "    with DB.transaction():\n"
        "        return Widget.where('id', widget_id).lock_for_update().first()\n",
    )

    assert _messages(tmp_path) == []


def test_the_same_query_without_a_row_lock_is_still_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Claim.py",
        IMPORT + "from cara.facades import DB\n"
        "\n"
        "\n"
        "def claim(widget_id):\n"
        "    with DB.transaction():\n"
        "        return Widget.where('id', widget_id).first()\n",
    )

    assert _messages(tmp_path) != []


def test_an_allow_comment_cannot_create_a_local_exception(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n"
        "\n"
        "def run():\n"
        "    # allow-inline-orm: boot probe runs before the container exists\n"
        "    return Widget.where('status', 'active').get()\n",
    )

    assert _messages(tmp_path) != []


def test_a_pinned_file_reports_only_through_the_shrink_only_ratchet(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "Harness.py",
        IMPORT + "\n\ndef run():\n    return Widget.where('a', 1).get()\n",
    )
    key = "model_query_discipline"

    assert (
        _messages(tmp_path, seam_allowlists={key: {"app/services/Harness.py": 2}}) == []
    )
    assert any(
        "debt grew" in message
        for message in _messages(
            tmp_path, seam_allowlists={key: {"app/services/Harness.py": 1}}
        )
    )


def test_a_model_file_querying_through_cls_is_a_finding(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "models" / "Widget.py",
        "class Widget:\n"
        "    @classmethod\n"
        "    def upsert_observed(cls, owner, values):\n"
        "        row = cls.where('owner_id', owner.id).first()\n"
        "        return row.update(values) if row else cls.create(values)\n",
    )

    # ``cls`` is not an imported name, so a receiver rule that only accepted
    # imported names left every model-resident use-case unscanned.
    assert any("cls.where(...)" in message for message in _messages(tmp_path))
    assert any("cls.create(...)" in message for message in _messages(tmp_path))


def test_a_model_file_querying_through_self_dunder_class_is_a_finding(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "models" / "Widget.py",
        "class Widget:\n"
        "    def rotate(self):\n"
        "        self.__class__.where('id', self.id).update({'epoch': 1})\n",
    )

    assert any("self.__class__.update(...)" in message for message in _messages(tmp_path))


def test_a_loaded_rows_own_update_stays_the_models_intrinsic_transition(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "models" / "Widget.py",
        "class Widget:\n"
        "    def touch(self, values):\n"
        "        self.update(values)\n"
        "        self.save()\n",
    )

    assert _messages(tmp_path) == []


def test_cls_outside_a_model_home_is_not_a_model_receiver(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "class Report:\n"
        "    @classmethod\n"
        "    def build(cls):\n"
        "        return cls.where('a', 1).get()\n",
    )

    assert _messages(tmp_path) == []
