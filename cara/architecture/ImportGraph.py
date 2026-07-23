"""ImportGraph: the module-level import graph (DOCTRINE §5.1).

"Hoisting is decided by the import graph, not by feel. A local import may
move to the top iff adding that edge to the module-level import graph
(barrels included as nodes) creates no cycle." This is that graph: every
Python module under the manifest's scan roots (``app``/``config``/
``routes``/``packages``) plus every dev-only kernel package is a node
(barrels — ``__init__.py`` files — are ordinary nodes, not special-cased);
an edge ``A -> B`` means "importing A, at module scope, requires B to have
finished executing" (relative imports resolved against the importing
module's own dotted path; a partially-initialised ANCESTOR package can
still satisfy a submodule import, so ancestor edges are added too, but
never back onto the importer's own ancestor chain). ``if TYPE_CHECKING:``
bodies are excluded — they never execute.

Two queries drive the Import Law tooling:

* :meth:`would_cycle` — hoist-safety: would adding the edge
  ``src -> dst`` (a function-local import promoted to module level)
  create a cycle? True iff ``dst`` can already reach ``src``.
* :meth:`sccs` — every strongly-connected component of size > 1 (a real
  import cycle), Tarjan's algorithm via Kosaraju's two-pass construction
  (iterative — no recursion-depth ceiling on a large tree).
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from cara.architecture._ast_utils import module_level_imports, parse
from cara.architecture.Manifest import Manifest


def _discover(manifest: Manifest) -> dict[str, Path]:
    roots: list[tuple[Path, str]] = []
    for path in (
        manifest.roots.app,
        manifest.roots.config,
        manifest.roots.routes,
        manifest.roots.packages,
    ):
        if path is not None:
            roots.append((path, path.name))
    for pkg_name, path in manifest.roots.kernel.items():
        roots.append((path, f"{manifest.roots.kernel_dev_root_name}.{pkg_name}"))

    nodes: dict[str, Path] = {}
    for base, prefix in roots:
        if not base.exists():
            continue
        for f in base.rglob("*.py"):
            if "__pycache__" in f.parts:
                continue
            rel = f.relative_to(base)
            parts = list(rel.parts)
            parts[-1] = parts[-1][:-3]
            if parts[-1] == "__init__":
                parts = parts[:-1]
            name = prefix + ("." + ".".join(parts) if parts else "")
            nodes[name] = f
    return nodes


def _ancestors(name: str) -> list[str]:
    parts = name.split(".")
    return [".".join(parts[:i]) for i in range(1, len(parts))]


def _resolve_from_module(node: ast.ImportFrom, src_mod: str, src_is_pkg: bool) -> str:
    if node.level == 0:
        return node.module or ""
    parts = src_mod.split(".")
    if not src_is_pkg:
        parts = parts[:-1]
    drop = node.level - 1
    if drop:
        parts = parts[:-drop] if drop <= len(parts) else []
    base = ".".join(parts)
    if node.module:
        return f"{base}.{node.module}" if base else node.module
    return base


def _edge_targets(
    node: ast.stmt, src_mod: str, src_is_pkg: bool, nodes: dict[str, Path]
) -> list[str]:
    """In-scope graph edges induced by executing this import in ``src_mod``."""
    raw_targets: list[str] = []
    hard_targets: list[str] = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            raw_targets.append(alias.name)
    else:
        mod = _resolve_from_module(node, src_mod, src_is_pkg)
        if not mod:
            return []
        raw_targets.append(mod)
        for alias in node.names:
            if alias.name == "*":
                hard_targets.append(mod)
                continue
            deep = f"{mod}.{alias.name}"
            if deep in nodes:
                raw_targets.append(deep)
            else:
                # ``from X import NAME`` where NAME is not itself a
                # submodule: X's module BODY must have executed past
                # NAME's binding — a hard (fully-initialised) dependency,
                # even when X is an ancestor package of the source.
                hard_targets.append(mod)

    src_ancestors = set(_ancestors(src_mod))
    out: set[str] = set()
    for target in raw_targets:
        for candidate in (target, *_ancestors(target)):
            if (
                candidate in nodes
                and candidate != src_mod
                and candidate not in src_ancestors
            ):
                out.add(candidate)
    for target in hard_targets:
        if target in nodes and target != src_mod:
            out.add(target)
    return sorted(out)


def _build_adjacency(
    nodes: dict[str, Path], trees: dict[Path, ast.Module]
) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = {name: set() for name in nodes}
    for name, path in nodes.items():
        tree = trees.get(path)
        if tree is None:
            continue
        is_pkg = path.name == "__init__.py"
        for imp in module_level_imports(tree):
            adjacency[name].update(_edge_targets(imp, name, is_pkg, nodes))
    return adjacency


@dataclass(slots=True)
class ImportGraph:
    """The module-level import graph for one manifest's scan scope."""

    nodes: dict[str, Path] = field(default_factory=dict)
    adjacency: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def build(cls, manifest: Manifest) -> ImportGraph:
        nodes = _discover(manifest)
        trees = {path: parse(path) for path in set(nodes.values())}
        adjacency = _build_adjacency(nodes, trees)
        return cls(nodes=nodes, adjacency=adjacency)

    def reachable_from(self, start: str) -> set[str]:
        """Every node transitively imported (module scope) by ``start``."""
        seen: set[str] = set()
        stack = [start]
        while stack:
            current = stack.pop()
            for nxt in self.adjacency.get(current, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    def path(self, start: str, goal: str) -> list[str] | None:
        """BFS ``start -> goal``; ``None`` if unreachable. ``[start]`` when
        ``start == goal``."""
        if start == goal:
            return [start]
        from collections import deque

        prev: dict[str, str | None] = {start: None}
        queue = deque([start])
        while queue:
            current = queue.popleft()
            for nxt in self.adjacency.get(current, ()):
                if nxt in prev:
                    continue
                prev[nxt] = current
                if nxt == goal:
                    trail = [nxt]
                    while prev[trail[-1]] is not None:
                        trail.append(prev[trail[-1]])
                    return list(reversed(trail))
                queue.append(nxt)
        return None

    def would_cycle(self, src: str, dst: str) -> bool:
        """Hoist-safety query: would the edge ``src -> dst`` (a function-
        local import promoted to module level) create a cycle? True iff
        ``dst`` can already reach ``src`` — the promoted edge would then
        close the loop ``src -> dst -> ... -> src``."""
        if src == dst:
            return True
        return src in self.reachable_from(dst)

    def sccs(self) -> list[list[str]]:
        """Every strongly-connected component of size > 1 (a real import
        cycle) or a single node with a self-loop. Kosaraju's algorithm,
        iterative (no recursion-depth ceiling)."""
        finish_order: list[str] = []
        visited: set[str] = set()
        for start in self.nodes:
            if start in visited:
                continue
            stack: list[tuple[str, Iterator[str]]] = [
                (start, iter(self.adjacency.get(start, ())))
            ]
            visited.add(start)
            while stack:
                node, it = stack[-1]
                advanced = False
                for nxt in it:
                    if nxt not in visited:
                        visited.add(nxt)
                        stack.append((nxt, iter(self.adjacency.get(nxt, ()))))
                        advanced = True
                        break
                if not advanced:
                    finish_order.append(node)
                    stack.pop()

        reverse: dict[str, set[str]] = {name: set() for name in self.nodes}
        for name, targets in self.adjacency.items():
            for target in targets:
                reverse.setdefault(target, set()).add(name)

        assigned: dict[str, int] = {}
        components: list[list[str]] = []
        for start in reversed(finish_order):
            if start in assigned:
                continue
            comp_id = len(components)
            component: list[str] = []
            stack = [start]
            assigned[start] = comp_id
            while stack:
                node = stack.pop()
                component.append(node)
                for nxt in reverse.get(node, ()):
                    if nxt not in assigned:
                        assigned[nxt] = comp_id
                        stack.append(nxt)
            components.append(component)

        return [
            comp
            for comp in components
            if len(comp) > 1
            or (len(comp) == 1 and comp[0] in self.adjacency.get(comp[0], ()))
        ]
