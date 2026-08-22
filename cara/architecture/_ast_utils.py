"""Shared pure-AST helpers for the Guard Pack scanners.

Leading underscore: an internal module, not part of the package's public
surface (mirrors ``cara/commands/_optional.py``). Every function here is
stdlib-only, side-effect free, and safe to call without booting any
application — the boot-free contract every scanner and craft command in
``cara/architecture/`` must uphold.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

STDLIB: frozenset[str] = frozenset(sys.stdlib_module_names) | {"__future__"}

_UPPER_RE_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def is_upper_const(name: str) -> bool:
    """True for an UPPER_SNAKE identifier (a constant, never a class/def)."""
    return (
        bool(name)
        and not name.startswith("_")
        and name == name.upper()
        and any(c.isalpha() for c in name)
        and set(name) <= _UPPER_RE_CHARS
    )


def python_files(base: Path) -> list[Path]:
    """Every ``*.py`` under ``base``, sorted, ``__pycache__`` excluded."""
    if not base.is_dir():
        return []
    return sorted(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)


def iter_modules(
    roots: Iterable[Path], deployable: Path
) -> Iterator[tuple[Path, str, ast.Module]]:
    """Yield ``(path, deployable-relative path, tree)`` for every non-barrel
    module under ``roots``.

    Barrels (``__init__.py``) are generated and carry no logic, so every
    source-law scanner skips them. A root may be listed twice (two scanners
    sharing a tree) or be a symlink into a sibling checkout; a file is
    yielded once, keyed by its resolved path, and its reported path stays
    the LOGICAL one so findings read the way a developer navigates.
    """
    seen: set[Path] = set()
    for root in roots:
        for path in python_files(root):
            if path.name == "__init__.py":
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            tree = parse(path)
            if tree is None:
                continue
            yield path, relpath(path, deployable), tree


def relpath(path: Path, root: Path) -> str:
    """POSIX-style path relative to ``root`` (falls back to the name)."""
    # Preserve the logical path through a deployable's symlinked dev kernel.
    # Resolving first turns ``api/commons/models/Foo.py`` into a sibling path
    # outside ``api/`` and collapses every finding to the basename.
    try:
        return path.absolute().relative_to(root.absolute()).as_posix()
    except ValueError:
        pass
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.name


def _path_has_fragment(relative: str, fragments: Iterable[str]) -> bool:
    """Whether a POSIX path contains one declared contiguous path fragment."""

    parts = relative.split("/")
    for fragment in fragments:
        segments = [segment for segment in fragment.split("/") if segment]
        if not segments:
            continue
        span = len(segments)
        if any(
            parts[index : index + span] == segments
            for index in range(len(parts) - span + 1)
        ):
            return True
    return False


def read_source(path: Path) -> str | None:
    """A file's text, or ``None`` when it cannot be read as UTF-8 source.

    Every scanner that needs raw lines goes through here rather than calling
    ``read_text`` itself. A guard pack walks a tree that is MID-CHANGE by
    definition: a file disappears between the glob and the read, a directory
    has no barrel yet, a vendored asset carries one non-UTF-8 byte. Any of
    those used to abort the whole pack with a traceback that named the
    scanner and not the file.
    """
    try:
        return path.read_text(encoding="utf-8")
    except OSError, UnicodeDecodeError:
        return None


#: Parsed trees, keyed by ``(path, the file's exact source text)``.
#:
#: A Guard Pack run parses the SAME file once per scanner whose roots contain
#: it. In cheapa services that is 29,899 parses of 3,195 distinct files — 89%
#: of them redundant, ~7.5s of a 20s run, and the reason
#: ``test_architecture_manifest_is_clean`` began tripping a 30s CI budget.
#:
#: The key is the source TEXT, not an mtime/size signature, so a stale hit is
#: impossible by construction: identical bytes always compile to an identical
#: tree, and a file rewritten mid-run (what the tmp_path scanner tests do)
#: re-parses because its text differs — no dependence on filesystem timestamp
#: granularity. Sharing one tree between scanners is safe because no scanner
#: mutates one: ``cara/architecture/`` contains no NodeTransformer and no
#: assignment to an AST node attribute.
_PARSE_CACHE: dict[tuple[str, str], ast.Module | None] = {}

#: Materialized ``ast.walk`` order per tree, keyed by ``id(tree)``. The tuple
#: keeps the TREE itself alive alongside its node list: an ``id()`` key is
#: only stable while its object is, and a collected tree could otherwise hand
#: its address to an unrelated one.
_NODES_CACHE: dict[int, tuple[ast.Module, list[ast.AST]]] = {}


def clear_parse_cache() -> None:
    """Drop every memoized tree AND its materialized node list.

    Together they roughly double the scan's peak RSS (189 MB -> 419 MB for
    cheapa services' 14 MB of Python). ``craft arch:check`` simply exits; a
    process that scans once and then does unrelated work — a pytest session
    with 11k tests after the guard test — reclaims that memory here.
    """
    _PARSE_CACHE.clear()
    _NODES_CACHE.clear()


def parse(path: Path) -> ast.Module | None:
    """Parse a file; ``None`` on a syntax error (a scanner's own concern —
    py_compile / the test suite catches genuine syntax breakage) or on a file
    that cannot be read at all (see ``read_source``).

    Memoized on the file's exact text (see ``_PARSE_CACHE``) — the tree the
    next scanner would build from the same bytes is the one already built.
    """
    source = read_source(path)
    if source is None:
        return None
    key = (str(path), source)
    if key in _PARSE_CACHE:
        return _PARSE_CACHE[key]
    try:
        tree: ast.Module | None = ast.parse(source, filename=str(path))
    except SyntaxError:
        tree = None
    _PARSE_CACHE[key] = tree
    return tree


def is_type_checking_if(node: ast.stmt) -> bool:
    if not isinstance(node, ast.If):
        return False
    test = node.test
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def module_level_imports(tree: ast.Module) -> list[ast.stmt]:
    """Runtime module-level ``Import``/``ImportFrom`` nodes.

    Recurses into module-level ``if``/``try``/``with`` bodies (they execute
    at import time); skips ``if TYPE_CHECKING:`` bodies (they never
    execute); never descends into a function or class BODY beyond a
    class's own top level (class bodies execute at class-definition time,
    i.e. still at import time, so those recurse too).
    """
    out: list[ast.stmt] = []

    def visit(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                out.append(node)
                continue
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if isinstance(node, ast.ClassDef):
                visit(node.body)
                continue
            if is_type_checking_if(node):
                visit(node.orelse or [])
                continue
            for attr in ("body", "orelse", "finalbody"):
                sub = getattr(node, attr, None)
                if sub:
                    visit(sub)
            for handler in getattr(node, "handlers", []) or []:
                visit(handler.body)
            for case in getattr(node, "cases", []) or []:
                visit(case.body)

    visit(tree.body)
    return out


def function_local_imports(tree: ast.Module) -> list[ast.stmt]:
    """``Import``/``ImportFrom`` nodes whose enclosing scope is a function."""
    found: list[ast.stmt] = []

    def visit(body: list[ast.stmt], in_function: bool) -> None:
        for node in body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if in_function:
                    found.append(node)
                continue
            entering = in_function or isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef)
            )
            for attr in ("body", "orelse", "finalbody"):
                visit(getattr(node, attr, []) or [], entering)
            for handler in getattr(node, "handlers", []) or []:
                visit(handler.body, entering)
            for case in getattr(node, "cases", []) or []:
                visit(case.body, entering)

    visit(tree.body, False)
    return found


def all_nodes(tree: ast.Module) -> list[ast.AST]:
    """Every node of ``tree`` in ``ast.walk`` order, walked at most once.

    ``ast.walk`` is a Python-level BFS: a deque round-trip plus
    ``iter_child_nodes``/``iter_fields``/``getattr`` per node. Scanners ask
    the same tree for its nodes over and over — ``RawSqlHome`` alone walked
    each file SEVEN times (bound names, compiler ids, call sites, docstring
    ids, schema metadata, literals), and across the pack cheapa services
    yielded 18.7M nodes from a 1.2M-node census, 15x.

    Walking once and re-reading the list is exactly equivalent: the list is
    in the same BFS order, holds the same node objects, and no scanner
    mutates a tree. Paired with the ``parse`` cache the list is shared
    ACROSS scanners too, because they are handed the same tree object.
    """
    cached = _NODES_CACHE.get(id(tree))
    if cached is not None:
        return cached[1]
    nodes = list(ast.walk(tree))
    _NODES_CACHE[id(tree)] = (tree, nodes)
    return nodes


def docstring_node_ids(tree: ast.Module) -> set[int]:
    """``id()`` of every module/class/function docstring Constant node."""
    ids: set[int] = set()
    for node in all_nodes(tree):
        if (
            not isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            )
            or not node.body
        ):
            continue
        first = node.body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            ids.add(id(first.value))
    return ids


def dunder_all(tree: ast.Module) -> list[str] | None:
    """The literal ``__all__`` list/tuple of string constants, if declared."""
    for node in tree.body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        elif isinstance(node, ast.AnnAssign):
            target = node.target
        if (
            target is not None
            and isinstance(target, ast.Name)
            and target.id == "__all__"
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            return [e.value for e in node.value.elts if isinstance(e, ast.Constant)]
    return None


def declares_dunder_all(tree: ast.Module) -> bool:
    """True when the module assigns ``__all__`` at all — literal or computed.

    ``dunder_all`` answers ``None`` for two very different modules: one that
    declares no ``__all__``, and one that declares a COMPUTED ``__all__``
    (``sorted({*_EXPORTS, ...})`` — the shape a hand-written lazy barrel
    uses). A checker that cannot tell them apart silently fails OPEN on the
    computed form, so the two questions are asked separately.
    """
    for node in tree.body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            target = node.target
        if isinstance(target, ast.Name) and target.id == "__all__":
            return True
    return False


def public_names(path: Path) -> list[str]:
    """Public surface of a module: ``__all__`` if declared, else top-level
    classes/functions and UPPER_SNAKE constants, ``_``-prefixed excluded."""
    tree = parse(path)
    if tree is None:
        return []
    declared = dunder_all(tree)
    if declared is not None:
        return sorted(set(declared))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and is_upper_const(t.id):
                    names.add(t.id)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and is_upper_const(node.target.id)
            and node.value is not None
        ):
            names.add(node.target.id)
    return sorted(names)


def declared_all(path: Path) -> list[str]:
    """Only the EXPLICIT literal ``__all__`` of a module file (never the
    derived-names fallback ``public_names`` uses) — the way to tell "not yet
    generated" apart from "generated with an empty surface"."""
    tree = parse(path) if path.exists() else None
    if tree is None:
        return []
    declared = dunder_all(tree)
    return list(declared) if declared is not None else []


def is_module_object(pkg_dir: Path, name: str) -> bool:
    """Does ``from . import <name>`` inside ``pkg_dir/__init__.py`` bind the
    SUBMODULE, rather than a same-named symbol re-exported from it?

    This is the module-object contract of §5.1, and it is a single question
    with a single answer, asked from both sides: the BarrelGenerator asks it
    to decide what to preserve and keep module-qualified, and
    ``BarrelCompleteness`` / ``ImportForm`` ask it to decide what to exempt
    from the barrel-superset and deep-import rules. It lived in two places
    that disagreed, and the disagreement was one-directional: the reader
    accepted ANY existing submodule, so the class-per-file case
    (``ChannelService.py`` defining ``class ChannelService``) was granted the
    exemption. That is name/submodule shadowing — the exact failure §5.1's
    "a public name missing from its barrel is a bug even before anyone
    imports it" sentence was written for — and the completeness guard was
    blind to it precisely because the writer, which got it right, was not the
    one being read (§5: read the SSOT, never restate it).

    A leaf ``X.py`` that exports a public ``X`` resolves to that symbol once
    any re-export runs, so it is NOT a module object. A subpackage is judged
    by its literal ``__all__`` for the same reason: a barrel that re-exports
    its own name shadows itself.
    """
    leaf = pkg_dir / f"{name}.py"
    if leaf.exists():
        return name not in public_names(leaf)
    sub_init = pkg_dir / name / "__init__.py"
    if sub_init.exists():
        return name not in declared_all(sub_init)
    return False


def module_object_names(pkg: Path) -> set[str]:
    """Submodule names this package's ``__init__`` binds as MODULE OBJECTS
    (``from . import X`` with no asname, X passing :func:`is_module_object`)
    — the module-object contract exemption (§5.1): X's own symbols stay
    module-qualified and are exempt from barrel-superset/deep-import checks.

    A SUBPACKAGE counts exactly like a leaf module. It has to: a theme kept
    module-qualified to break a boot-order cycle (``from . import catalog``)
    is a directory, and recognizing only ``X.py`` left the subpackage arm of
    ``BarrelCompleteness._expected_exports`` unreachable — the exemption the
    scanner's own docstring promises could never fire for the only shape
    that needs it.
    """
    init = pkg / "__init__.py"
    tree = parse(init) if init.exists() else None
    if tree is None:
        return set()
    return {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.level == 1 and not node.module
        for alias in node.names
        if alias.asname is None and is_module_object(pkg, alias.name)
    }
