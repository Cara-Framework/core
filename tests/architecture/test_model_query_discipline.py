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


def test_the_allow_tag_opts_one_call_out(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n"
        "\n"
        "def run():\n"
        "    # allow-inline-orm: boot probe runs before the container exists\n"
        "    return Widget.where('status', 'active').get()\n",
    )

    assert _messages(tmp_path) == []


def test_an_untagged_allow_comment_does_not_opt_out(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        IMPORT + "\n\ndef run():\n    # allow-inline-orm\n"
        "    return Widget.where('status', 'active').get()\n",
    )

    # The tag must carry a reason; a bare marker documents nothing.
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
