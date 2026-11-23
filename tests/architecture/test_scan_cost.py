"""What one Guard Pack run costs — pinned as COUNTS, not as seconds.

The pack is twenty-two scanners over the same files. Each one is written as if
it were alone, so every shared cost multiplies by the number of scanners unless
something makes it shared: parsing a file, walking its tree, sieving that walk
for the handful of node types a rule cares about.

A timing assertion would rot with the runner it happens to execute on. These
assert the thing the time is made of — how often a file is compiled, how often
a tree is walked — and that the shared answers are the ones each scanner would
have computed by itself: same nodes, same order, never a stale tree.

Measured on synkronus api (5,422 files, 3.9M nodes) when these were written:
a cold run went from ~20s to ~10s, findings byte-identical with and without
the product's allowlists (838 raw findings).
"""

from __future__ import annotations

import ast
import itertools
from collections import Counter
from pathlib import Path

import pytest

from cara.architecture import _ast_utils
from cara.architecture._ast_utils import (
    IMPORT_NODE_TYPES,
    all_nodes,
    clear_parse_cache,
    nodes_of,
    parse,
    relpath,
)
from cara.architecture.scanners import REGISTRY
from cara.architecture.scanners.HttpInBusinessLogic import HttpInBusinessLogic

from ._fixtures import make_manifest, write

_ARCHITECTURE_SOURCE = Path(_ast_utils.__file__).parent


@pytest.fixture(autouse=True)
def _fresh_caches():
    """Counts are only meaningful from an empty cache, and no tree built here
    should ride along through the rest of the session."""
    clear_parse_cache()
    yield
    clear_parse_cache()


def _product_tree(root: Path):
    """A tree every scanner has something to read in: an edge, a service with
    a function-local import and a broad except, a repository, a port, a job,
    a model, a gate and a shared module."""
    write(
        root / "app" / "controllers" / "shop" / "OrderController.py",
        "from app.services.shop import OrderService\n"
        "\n"
        "\n"
        "class OrderController:\n"
        "    def __init__(self, order_service: OrderService):\n"
        "        self.order_service = order_service\n"
        "\n"
        "    async def show(self, request):\n"
        "        return self.order_service.find(request.id)\n",
    )
    write(
        root / "app" / "services" / "shop" / "OrderService.py",
        "from app.models import Order\n"
        "from app.shared import Text\n"
        "\n"
        "\n"
        "class OrderService:\n"
        "    def find(self, order_id):\n"
        "        import json  # local: heavy optional dep\n"
        "\n"
        "        try:\n"
        "            return json.dumps(Order.find(order_id))\n"
        "        except Exception as error:\n"
        "            raise RuntimeError(Text.clean(str(error))) from error\n",
    )
    write(
        root / "app" / "repositories" / "shop" / "OrderRepository.py",
        "from app.models import Order\n"
        "\n"
        "\n"
        "class OrderRepository:\n"
        "    def paid(self):\n"
        "        return Order.where('status', 'paid').get()\n",
    )
    write(
        root / "app" / "ports" / "PaymentContract.py",
        "# port: the card processor's HTTP API\nclass PaymentContract:\n    pass\n",
    )
    write(
        root / "app" / "jobs" / "SendReceiptJob.py",
        "class SendReceiptJob(BaseJob):\n    idempotency_params = ('order_id',)\n",
    )
    write(root / "config" / "shop.py", "import os\n\nCURRENCY = os.getenv('CURRENCY')\n")
    write(
        root / "commons" / "models" / "Order.py",
        "class Order:\n    __table__ = 'orders'\n",
    )
    write(
        root / "commons" / "gates" / "persistence" / "OrderGate.py",
        "def settle(order):\n    order.update({'status': 'paid'})\n",
    )
    write(
        root / "commons" / "shared" / "Text.py", "def clean(value):\n    return value\n"
    )
    return make_manifest(
        root,
        layers=("controllers", "services", "repositories", "ports", "jobs"),
        domain_layers=("controllers", "services", "repositories"),
        domains={"shop": "Orders and the money they move."},
        plugin_tokens=frozenset({"amazon"}),
        write_ownership={"orders": "api-owned"},
    )


def _run_the_pack(manifest) -> None:
    for name in sorted(REGISTRY):
        REGISTRY[name].scan(manifest)


# ── a file is compiled once, a tree is walked once ───────────────────


def test_a_full_pack_run_compiles_each_file_exactly_once(tmp_path, monkeypatch) -> None:
    manifest = _product_tree(tmp_path)
    compiled: Counter[str] = Counter()
    real_parse = ast.parse

    def counting_parse(source, filename="<unknown>", *args, **kwargs):
        compiled[str(filename)] += 1
        return real_parse(source, filename, *args, **kwargs)

    monkeypatch.setattr(ast, "parse", counting_parse)

    _run_the_pack(manifest)

    assert compiled, "the pack compiled nothing — the tree was never read"
    assert set(compiled.values()) == {1}, {
        path: count for path, count in compiled.items() if count != 1
    }


def test_no_scanner_walks_a_whole_module_by_itself(tmp_path, monkeypatch) -> None:
    """A scanner that calls ``ast.walk(tree)`` re-walks a tree the pack already
    holds a node list for. Fifteen of them did: 28M node visits for a 3.9M-node
    census, 43s of a 68s profile. A SUBTREE walk (one function, one handler) is
    a different question and stays legal — only ``ast.Module`` is counted."""
    manifest = _product_tree(tmp_path)
    module_walks: list[str] = []
    real_walk = ast.walk

    def counting_walk(node):
        if isinstance(node, ast.Module):
            module_walks.append(ast.dump(node)[:80])
        return real_walk(node)

    monkeypatch.setattr(ast, "walk", counting_walk)

    _run_the_pack(manifest)

    assert module_walks == []


def test_a_full_pack_run_walks_each_tree_exactly_once(tmp_path, monkeypatch) -> None:
    manifest = _product_tree(tmp_path)

    class CountingCache(dict):
        writes: Counter[int] = Counter()

        def __setitem__(self, key, value):
            self.writes[key] += 1
            super().__setitem__(key, value)

    cache = CountingCache()
    monkeypatch.setattr(_ast_utils, "_NODES_CACHE", cache)

    _run_the_pack(manifest)

    assert cache.writes, "no tree was walked — the scanners never reached a node"
    assert set(cache.writes.values()) == {1}


# ── the shared answers are the ones a scanner would compute alone ────


def _framework_trees() -> list[ast.Module]:
    trees = [
        parse(path)
        for path in sorted(_ARCHITECTURE_SOURCE.rglob("*.py"))
        if "__pycache__" not in path.parts
    ]
    assert len(trees) > 30 and all(tree is not None for tree in trees)
    return trees


def test_all_nodes_is_ast_walk_node_for_node() -> None:
    """Findings, and the order of the hits joined into one finding, follow walk
    order. ``all_nodes`` reimplements the BFS for speed, so its claim to BE
    ``ast.walk`` is checked on real source rather than taken on trust."""
    for tree in _framework_trees():
        walked = list(ast.walk(tree))
        cached = all_nodes(tree)

        assert len(cached) == len(walked)
        assert all(mine is theirs for mine, theirs in zip(cached, walked, strict=True))


def test_nodes_of_is_the_sieved_walk_in_walk_order() -> None:
    for tree in _framework_trees():
        for types in (IMPORT_NODE_TYPES, (ast.Call,), (ast.ClassDef,)):
            expected = [node for node in ast.walk(tree) if isinstance(node, types)]
            sieved = nodes_of(tree, types)

            assert len(sieved) == len(expected)
            assert all(a is b for a, b in zip(sieved, expected, strict=True))
            assert nodes_of(tree, types) is sieved, "the sieve ran twice"


def test_a_rewritten_file_is_never_answered_from_the_old_tree(tmp_path) -> None:
    """Same path, same LENGTH, same second: the rewrite every mtime/size cache
    key gets wrong. The cache is keyed on the text, and so is everything
    derived from the tree it returns."""
    module = tmp_path / "app" / "services" / "Report.py"
    write(module, "import cara.request\n")
    manifest = make_manifest(tmp_path)
    assert len(HttpInBusinessLogic.scan(manifest)) == 1

    write(module, "import cara.reQuest\n")

    assert HttpInBusinessLogic.scan(manifest) == []


def test_clearing_the_parse_cache_drops_every_derived_list(tmp_path) -> None:
    tree = parse(write(tmp_path / "module.py", "import os\n\nos.getcwd()\n"))
    assert tree is not None
    nodes_of(tree, IMPORT_NODE_TYPES)
    assert _ast_utils._NODES_CACHE and _ast_utils._TYPED_NODES_CACHE

    clear_parse_cache()

    assert _ast_utils._PARSE_CACHE == {}
    assert _ast_utils._NODES_CACHE == {}
    assert _ast_utils._TYPED_NODES_CACHE == {}


# ── relpath: the string fast path IS pathlib's answer ────────────────


def _pathlib_relpath(path: Path, root: Path) -> str:
    try:
        return path.absolute().relative_to(root.absolute()).as_posix()
    except ValueError:
        pass
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.name


_SHAPES = (
    "/",
    "/a",
    "/ab",
    "/a/b",
    "/a/b/",
    "/a/b/c.py",
    "/a/bc/c.py",
    "/a/b/../c.py",
    "/a//b/c.py",
    "/a/./b/c.py",
    "/a/b/c.py/d",
    "//a/b",
    "//a/b/c.py",
    "rel",
    "rel/x.py",
    "x.py",
    ".",
)


def test_relpath_matches_pathlib_for_every_shape() -> None:
    for path, root in itertools.product(_SHAPES, _SHAPES):
        expected = _pathlib_relpath(Path(path), Path(root))

        assert relpath(Path(path), Path(root)) == expected, (path, root)


def test_relpath_keeps_the_logical_path_through_a_symlinked_kernel(tmp_path) -> None:
    kernel = tmp_path / "kernel" / "models"
    write(kernel / "Order.py", "")
    deployable = tmp_path / "api"
    (deployable / "commons").mkdir(parents=True)
    (deployable / "commons" / "models").symlink_to(kernel, target_is_directory=True)

    logical = deployable / "commons" / "models" / "Order.py"

    assert relpath(logical, deployable) == "commons/models/Order.py"
    # Outside the deployable by its spelling, inside it once resolved.
    assert relpath(kernel / "Order.py", deployable / "commons" / "models") == "Order.py"
