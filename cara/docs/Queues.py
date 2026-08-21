"""The queue-topology reference page.

Split out of :mod:`cara.docs.Inventory` because reading it honestly is not a
one-function job: the binding table has to be loaded rather than parsed, found
by shape rather than by name, and the worker pools have to be read statically
because they are config and read ``env()``.
"""

from __future__ import annotations

import ast
import importlib.util
from contextlib import suppress
from pathlib import Path
from types import ModuleType

from cara.docs.DocsManifest import DocsManifest
from cara.docs.Support import Say, header, md_escape, read, write_if_changed


def load_contract(path: Path) -> ModuleType:
    """Boot-free load of a kernel CONTRACT module, by file location.

    The same contract :meth:`DocsManifest.load` keeps — executed by location,
    no package import, no ``sys.path`` mutation, no app boot. It is legal here
    and nowhere else in this engine because a queue topology is pure
    vocabulary: DOCTRINE §5 puts every ``env()`` read in ``config/``, so a
    contract module has no environment to read and nothing to connect to.

    An AST walk cannot replace it. A product whose binding table is COMPUTED
    (``TABLE = _lane_bindings()``) has no ``elts`` to literal-eval, so the walk
    silently yields nothing — and this page shipped a "could not parse"
    apology, in both products, for files that parse perfectly.
    """
    spec = importlib.util.spec_from_file_location("cara_docs_queue_topology", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the queue-topology contract: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def binding_tables(module: ModuleType) -> list[tuple[str, list[tuple[str, str]]]]:
    """Queue ↔ routing-key tables, found by SHAPE rather than by name.

    Products name this table differently — one calls it the topic-exchange
    binding list, another the base routing rules — and a name here would be
    one product's vocabulary compiled into the framework, the coupling
    ``DocsManifest`` exists to prevent. The SHAPE is DOCTRINE §8 and identical
    everywhere: a public module-level sequence of ``(queue, pattern)`` string
    pairs. Private names are skipped so a retired or legacy table cannot be
    mistaken for the live one.
    """
    tables = []
    for name in sorted(vars(module)):
        if name.startswith("_") or not name.isupper():
            continue
        value = vars(module)[name]
        if not isinstance(value, list | tuple) or not value:
            continue
        if all(
            isinstance(row, list | tuple)
            and len(row) == 2
            and all(isinstance(cell, str) for cell in row)
            for row in value
        ):
            tables.append((name, [(row[0], row[1]) for row in value]))
    return tables


# A field this engine cannot evaluate. It is spelled out rather than left
# blank because a blank cell reads as "no queues", which is the lie this page
# was already telling in prose.
_UNEVALUATED = "_resolved at boot_"


def _pool_row(key: ast.expr | None, value: ast.expr) -> tuple[str, str, str]:
    """One ``(pool, queues, backoff)`` row from a WORKER_POOLS entry.

    Worker pools are CONFIG, not contract: they read ``env()`` and cannot be
    executed here. What is still static is the pool SET and any literal field,
    so those are reported and the rest is named as boot-resolved.
    """
    name = (
        key.value if isinstance(key, ast.Constant) and isinstance(key.value, str) else "?"
    )
    queues, backoff = _UNEVALUATED, _UNEVALUATED
    if isinstance(value, ast.Dict):
        entries = {
            entry.value: pair
            for entry, pair in zip(value.keys, value.values, strict=True)
            if isinstance(entry, ast.Constant) and isinstance(entry.value, str)
        }
        with suppress(ValueError, SyntaxError, KeyError):
            queues = ", ".join(f"`{q}`" for q in ast.literal_eval(entries["queues"]))
        with suppress(ValueError, SyntaxError, KeyError):
            backoff = f"{ast.literal_eval(entries['timeout'])}s"
    return (name, queues, backoff)


def worker_pool_rows(src: str) -> list[tuple[str, str, str]]:
    """Every declared worker pool, including later ``WORKER_POOLS[x] = {...}``.

    The regex this replaced stopped at the first ``\n}``, so a pool appended
    after the literal — the local-convenience ``all`` pool both products
    declare — was invisible to the page.
    """
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    rows: list[tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        if node.value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "WORKER_POOLS":
                if isinstance(node.value, ast.Dict):
                    rows += [
                        _pool_row(entry, pool)
                        for entry, pool in zip(
                            node.value.keys, node.value.values, strict=True
                        )
                    ]
            elif (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "WORKER_POOLS"
            ):
                rows.append(_pool_row(target.slice, node.value))
    return rows


def gen_queues(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the queue-topology reference page."""
    root = manifest.root
    topology = root / "commons" / "contracts" / "QueueTopology.py"
    if not topology.is_file():
        # A product with no shared queue contract has no page to write; the
        # generators probe and no-op, they do not invent a subject.
        return
    pool_modules = [
        f"{label}/config/queue.py"
        for label in ("services", "api")
        if (root / label / "config" / "queue.py").is_file()
    ]
    body = header(
        manifest,
        "Queue topology",
        ["commons/contracts/QueueTopology.py", *pool_modules],
        now,
    )
    tables = binding_tables(load_contract(topology))
    if not tables:
        # HARD FAILURE, not a prose apology. An unrenderable topology means
        # either the contract moved or this reader is wrong; both are bugs to
        # fix, and a page that says so while staying green is how an empty
        # reference survived two products for a month.
        raise RuntimeError(
            f"{topology} declares no queue ↔ routing-key table: expected a "
            "public module-level sequence of (queue, pattern) string pairs "
            "(DOCTRINE §8). Fix the contract or this reader — the reference "
            "page may not ship empty."
        )
    for name, rows in tables:
        body += f"## Bindings (`{name}`)\n\n| Queue | Routing key pattern |\n|---|---|\n"
        for queue, key in rows:
            body += f"| `{md_escape(queue)}` | `{md_escape(key)}` |\n"
        body += f"\n_Total: {len(rows)} bindings._\n\n"
    # Pools live in TWO config modules, and BOTH must be named here. The worker
    # deployable drains the pipeline families; the HTTP deployable drains the
    # queues whose job classes resolve only against its own ``app.jobs``
    # package. Documenting one alone would imply those queues have no consumer
    # — the green-by-omission failure this reference exists to prevent.
    for relative in pool_modules:
        rows = worker_pool_rows(read(root / relative))
        if not rows:
            body += f"_No WORKER_POOLS block found in {relative}._\n\n"
            continue
        # "timeout" is the BROKER RECONNECT BACKOFF, not a job budget and not
        # an idle poll: consumers hold a long-lived basic_consume, so there is
        # no empty-queue sleep to document. The framework clamps it to [1, 10]
        # and only applies it after a disconnect.
        body += (
            f"## Worker pools ({relative})\n\n"
            "| Pool | Queues | Reconnect backoff |\n|---|---|---|\n"
        )
        for name, queues, backoff in rows:
            body += f"| {md_escape(name)} | {queues} | {backoff} |\n"
        body += (
            f"\n_{len(rows)} pool{'' if len(rows) == 1 else 's'}. Fields marked "
            f"\u201cresolved at boot\u201d are `env()`-derived in {relative}; "
            "the pool set itself is static._\n\n"
        )
    write_if_changed(manifest.reference / "queues.md", body, "reference/queues.md", say)


__all__ = [
    "binding_tables",
    "gen_queues",
    "load_contract",
    "worker_pool_rows",
]
