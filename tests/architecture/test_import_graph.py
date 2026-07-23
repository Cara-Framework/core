"""ImportGraph: module-level graph, relative resolution, TYPE_CHECKING
exclusion, SCC/cycle truth-table, hoist-safety query."""

from __future__ import annotations

from cara.architecture.ImportGraph import ImportGraph

from ._fixtures import make_manifest, write


def _build(tmp_path):
    write(tmp_path / "app" / "Producer.py", "from app.Consumer import thing\n")
    write(tmp_path / "app" / "Consumer.py", "X = 1\n")
    write(tmp_path / "app" / "Independent.py", "Y = 1\n")
    write(tmp_path / "app" / "CycleA.py", "from app.CycleB import b_thing\n")
    write(tmp_path / "app" / "CycleB.py", "from app.CycleA import a_thing\n")
    write(tmp_path / "app" / "pkg" / "__init__.py", "")
    write(tmp_path / "app" / "pkg" / "A.py", "from .B import x\n")
    write(tmp_path / "app" / "pkg" / "B.py", "from .A import y\n")
    write(
        tmp_path / "app" / "TypeCheckOnly.py",
        "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    from app.Consumer import thing\n",
    )
    manifest = make_manifest(tmp_path)
    return ImportGraph.build(manifest)


def test_module_level_edge_is_recorded(tmp_path):
    graph = _build(tmp_path)
    assert "app.Consumer" in graph.adjacency["app.Producer"]


def test_type_checking_block_is_excluded(tmp_path):
    graph = _build(tmp_path)
    assert graph.adjacency["app.TypeCheckOnly"] == set()


def test_relative_import_resolves_within_a_package(tmp_path):
    graph = _build(tmp_path)
    assert "app.pkg.B" in graph.adjacency["app.pkg.A"]
    assert "app.pkg.A" in graph.adjacency["app.pkg.B"]


def test_sccs_truth_table(tmp_path):
    graph = _build(tmp_path)
    sccs = {frozenset(c) for c in graph.sccs()}
    assert frozenset({"app.CycleA", "app.CycleB"}) in sccs
    assert frozenset({"app.pkg.A", "app.pkg.B"}) in sccs
    # a plain directed edge with no return path is never reported as a cycle
    assert not any({"app.Producer", "app.Consumer"} <= c for c in sccs)
    # an isolated node with no self-loop is never reported
    assert not any(c == {"app.Independent"} for c in sccs)


def test_path_finds_a_route_through_the_graph(tmp_path):
    graph = _build(tmp_path)
    route = graph.path("app.CycleA", "app.CycleB")
    assert route == ["app.CycleA", "app.CycleB"]


def test_path_returns_none_when_unreachable(tmp_path):
    graph = _build(tmp_path)
    assert graph.path("app.Independent", "app.Producer") is None


def test_would_cycle_flags_an_unsafe_hoist(tmp_path):
    graph = _build(tmp_path)
    # Consumer already reaches back to Producer only via Producer -> Consumer;
    # hoisting a LOCAL Consumer -> Producer import to module level would close
    # a 2-cycle.
    assert graph.would_cycle("app.Consumer", "app.Producer") is True


def test_would_cycle_permits_a_safe_hoist(tmp_path):
    graph = _build(tmp_path)
    assert graph.would_cycle("app.Consumer", "app.Independent") is False


def test_would_cycle_self_edge_is_always_unsafe(tmp_path):
    graph = _build(tmp_path)
    assert graph.would_cycle("app.Independent", "app.Independent") is True
