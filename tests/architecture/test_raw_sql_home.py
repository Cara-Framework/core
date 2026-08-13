"""RawSqlHome: raw SQL only inside a declared repository home (DOCTRINE §5).

These cases pin the three exemptions that four product copies disagreed
about — docstrings, schema metadata, and the single documented query
compiler — plus the shrink-only ratchet a product uses to adopt the guard
over a tree it has not cleaned yet.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.scanners.RawSqlHome import RawSqlHome

from ._fixtures import make_manifest, write

QUERY = "SELECT id FROM widget WHERE status = ?"


def _messages(root: Path, **overrides) -> list[str]:
    return [str(finding) for finding in RawSqlHome.scan(make_manifest(root, **overrides))]


def test_facade_alias_resolved_manager_and_driver_cursor_are_all_caught(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from cara.foundation import resolve as get_service\n"
        "from cara.facades import DB as Database\n"
        "\n"
        'manager = get_service("DB")\n'
        "\n"
        "\n"
        "def run(connection, query):\n"
        "    Database.select(query)\n"
        "    manager.statement(query)\n"
        "    with connection.cursor() as cursor:\n"
        f'        cursor.execute("{QUERY}")\n',
    )

    messages = _messages(tmp_path)

    assert any(".select(...)" in message for message in messages)
    assert any(".statement(...)" in message for message in messages)
    assert any(".cursor(...)" in message for message in messages)
    assert any("SQL literal" in message for message in messages)


def test_constructor_injected_manager_attributes_are_caught(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "services" / "Report.py",
        "from cara.eloquent import DatabaseManager\n"
        "\n"
        "\n"
        "class Report:\n"
        "    def __init__(self, manager: DatabaseManager):\n"
        "        self.manager = manager\n"
        "\n"
        "    def run(self, query):\n"
        "        return self.manager.select(query)\n",
    )

    assert any(".select(...)" in message for message in _messages(tmp_path))


def test_composing_raw_sql_is_caught_even_without_a_database_receiver(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "Search.py",
        "def apply(query):\n    return query.where_raw('lower(name) = ?')\n",
    )

    assert [".where_raw(...) composes raw SQL" in m for m in _messages(tmp_path)] == [
        True
    ]


def test_a_repository_home_owns_raw_sql(tmp_path: Path) -> None:
    write(
        tmp_path / "app" / "repositories" / "catalog" / "WidgetRepository.py",
        f"from cara.facades import DB\n\n\ndef all_active():\n    return DB.select('{QUERY}')\n",
    )

    assert _messages(tmp_path) == []


def test_a_home_fragment_matches_a_whole_path_run_not_a_substring(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "app" / "services" / "repositories_report" / "Export.py",
        f"from cara.facades import DB\n\n\ndef run():\n    return DB.select('{QUERY}')\n",
    )

    # ``repositories_report`` merely STARTS with the home fragment; treating
    # that as a home would let any file opt out by prefixing its directory.
    assert _messages(tmp_path) != []


def test_docstrings_and_schema_metadata_are_not_queries(tmp_path: Path) -> None:
    write(
        tmp_path / "commons" / "models" / "Widget.py",
        '"""Widget.\n'
        "\n"
        f"    Reporting runs {QUERY} against this table.\n"
        '    """\n'
        "\n"
        "\n"
        "class Widget:\n"
        '    __indexes__ = ["CREATE INDEX widget_status ON widget (status)"]\n',
    )

    assert _messages(tmp_path) == []


def test_exactly_one_documented_query_compiler_is_legal(tmp_path: Path) -> None:
    compiler = (
        "class {name}:\n"
        '    """Doctrine §5 query compiler for a closed field vocabulary."""\n'
        "\n"
        "    def apply(self, query):\n"
        "        return query.where_raw(self.clause)\n"
    )
    write(tmp_path / "commons" / "shared" / "Rules.py", compiler.format(name="Rules"))

    assert _messages(tmp_path) == []

    write(tmp_path / "commons" / "shared" / "Filters.py", compiler.format(name="Filters"))
    messages = _messages(tmp_path)

    assert any("only ONE query-compiler class is legal" in m for m in messages)


def test_an_undocumented_class_gets_no_compiler_exemption(tmp_path: Path) -> None:
    write(
        tmp_path / "commons" / "shared" / "Rules.py",
        'class Rules:\n    """Builds a clause."""\n\n'
        "    def apply(self, query):\n        return query.where_raw(self.clause)\n",
    )

    assert _messages(tmp_path) != []


def test_a_pinned_file_reports_only_through_the_shrink_only_ratchet(
    tmp_path: Path,
) -> None:
    write(
        tmp_path / "commons" / "gates" / "Predicates.py",
        "def apply(query):\n    return query.where_raw('a = 1').order_by_raw('b desc')\n",
    )
    pin = {"raw_sql_home": {"commons/gates/Predicates.py": 2}}

    assert _messages(tmp_path, seam_allowlists=pin) == []

    grown = {"raw_sql_home": {"commons/gates/Predicates.py": 1}}
    assert any("debt grew" in m for m in _messages(tmp_path, seam_allowlists=grown))

    stale = {"raw_sql_home": {"commons/gates/Predicates.py": 3}}
    assert any("stale" in m for m in _messages(tmp_path, seam_allowlists=stale))


def test_a_pin_whose_violation_was_fixed_fails_as_stale(tmp_path: Path) -> None:
    write(
        tmp_path / "commons" / "gates" / "Predicates.py",
        "def apply(query):\n    return query\n",
    )
    pin = {"raw_sql_home": {"commons/gates/Predicates.py": 2}}

    assert any(
        "violation resolved" in m for m in _messages(tmp_path, seam_allowlists=pin)
    )
