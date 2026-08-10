"""Reference pages extracted straight from code: routes, queues, models, jobs…

Every page here is derived by an AST/regex walk over the checkout — never hand
written, so it cannot go stale. Its failure mode is the opposite one:
producing NOTHING and reporting "unchanged" forever, which is exactly what the
routes page did from the day routes were sharded until the shard walk in
:func:`gen_routes` was added. When a generator here stops finding its subject,
say so in the page (:func:`gen_queues` does) rather than emit an empty table.

The directory layout the walkers probe — deployables beside a dev-only kernel,
routes under the HTTP deployable, plug-in packages under the worker — is
DOCTRINE layout, identical in every Cara product. Everything the layout cannot
tell you comes from the product's :class:`~cara.docs.Manifest.DocsManifest`.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from cara.docs.Manifest import DocsManifest
from cara.docs.Support import Say, header, md_escape, read, write_if_changed

# ---------------------------------------------------------------- routes

VERBS = {"get", "post", "put", "patch", "delete", "options", "any"}

ROUTE_MODULES = ("api.py", "web.py", "websocket.py", "broadcasting.py")


def _chain(call: ast.Call):
    """Lists the method chain outside-in: Route.prefix(x).mw(y).routes(...)"""
    out = []
    cur: ast.expr = call
    while isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
        out.append((cur.func.attr, cur))
        cur = cur.func.value
    return out


def _walk_routes(node: ast.expr, prefix: str, mw: list, out: list) -> None:
    if not isinstance(node, ast.Call):
        return
    chain = _chain(node)
    local_prefix = "".join(
        c.args[0].value
        for a, c in reversed(chain)
        if a == "prefix" and c.args and isinstance(c.args[0], ast.Constant)
    )
    local_mw = list(mw)
    for a, c in chain:
        if a == "middleware" and c.args:
            try:
                extra = ast.literal_eval(c.args[0])
                local_mw = local_mw + (extra if isinstance(extra, list) else [extra])
            except ValueError, SyntaxError:
                pass
    verb = next(((a, c) for a, c in chain if a in VERBS), None)
    if verb:
        v, c = verb
        path = c.args[0].value if c.args and isinstance(c.args[0], ast.Constant) else "?"
        handler = (
            c.args[1].value
            if len(c.args) > 1 and isinstance(c.args[1], ast.Constant)
            else "?"
        )
        name = ""
        for kw in c.keywords:
            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                name = kw.value.value
            if kw.arg == "middleware":
                try:
                    extra = ast.literal_eval(kw.value)
                    local_mw = local_mw + (extra if isinstance(extra, list) else [extra])
                except ValueError, SyntaxError:
                    pass
        out.append((v.upper(), prefix + local_prefix + path, handler, name, local_mw))
        return
    routes_call = next((c for a, c in chain if a == "routes"), None)
    if routes_call:
        for arg in routes_call.args:
            _walk_routes(arg, prefix + local_prefix, local_mw, out)


def parse_routes(path: Path, prefix: str = "") -> list:
    """Route tuples declared by every registrar function in one module."""
    src = read(path)
    if not src:
        return []
    out: list = []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    for fn in ast.walk(tree):
        # ``register*`` is the single-file shape; ``route_groups`` is what each
        # generated shard exports. Matching only the former is what left this
        # page reading "Total: 0 routes" from the day routes were sharded.
        if isinstance(fn, ast.FunctionDef) and fn.name.startswith(
            ("register", "route_groups")
        ):
            for st in ast.walk(fn):
                if isinstance(st, ast.Return) and st.value is not None:
                    nodes = (
                        st.value.elts
                        if isinstance(st.value, (ast.Tuple, ast.List))
                        else [st.value]
                    )
                    for node in nodes:
                        _walk_routes(node, prefix, [], out)
    return out


def _shard_prefix(path: Path) -> str:
    """Outer prefix the registrar wraps around starred shard groups.

    ``routes:generate`` emits ``Route.prefix("/api").routes(*groups)`` in the
    aggregator module with the actual chains living in bounded shards under
    ``routes/generated/``; a starred argument is invisible to the static
    walker, so the wrapper's prefix is recovered here and applied to every
    shard route instead.
    """
    src = read(path)
    if not src:
        return ""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return ""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        chain = _chain(node)
        routes_call = next((c for a, c in chain if a == "routes"), None)
        if routes_call and any(isinstance(arg, ast.Starred) for arg in routes_call.args):
            return "".join(
                c.args[0].value
                for a, c in reversed(chain)
                if a == "prefix" and c.args and isinstance(c.args[0], ast.Constant)
            )
    return ""


def collect_routes(root: Path) -> dict[str, list]:
    """Every route the HTTP deployable declares, per routes module.

    Shards are walked as well as the aggregator: a checkout without a
    ``generated/`` tree simply finds none, so the same pass serves both
    shapes. That symmetry is the point — the shard-blind version of this
    function reported zero routes for a sharded product and kept reporting it,
    because "no output" and "unchanged output" look identical downstream.
    """
    found: dict[str, list] = {}
    for module in ROUTE_MODULES:
        path = root / "api" / "routes" / module
        routes = parse_routes(path)
        shard_dir = root / "api" / "routes" / "generated" / module.removesuffix(".py")
        if shard_dir.is_dir():
            outer = _shard_prefix(path)
            for shard in sorted(shard_dir.glob("group_*.py")):
                routes += parse_routes(shard, outer)
        found[module] = routes
    return found


def gen_routes(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the route reference page."""
    body = header(
        manifest,
        "Routes",
        [f"api/routes/{module}" for module in ROUTE_MODULES],
        now,
    )
    total = 0
    for module, routes in collect_routes(manifest.root).items():
        if not routes:
            continue
        total += len(routes)
        body += f"## {module} ({len(routes)} routes)\n\n"
        body += "| Method | Path | Handler | Name | Middleware |\n|---|---|---|---|---|\n"
        for verb, path, handler, name, mw in routes:
            body += (
                f"| {verb} | `{md_escape(path)}` | `{md_escape(handler)}` | "
                f"{md_escape(name)} | {md_escape(', '.join(dict.fromkeys(mw)))} |\n"
            )
        body += "\n"
    body += f"_Total: {total} routes._\n"
    write_if_changed(manifest.reference / "routes.md", body, "reference/routes.md", say)


# ---------------------------------------------------------------- queues


def gen_queues(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the queue-topology reference page."""
    root = manifest.root
    topology = root / "commons" / "contracts" / "QueueTopology.py"
    body = header(
        manifest,
        "Queue topology",
        ["commons/contracts/QueueTopology.py", "services/config/queue.py"],
        now,
    )
    src = read(topology)
    bindings: list = []
    try:
        tree = ast.parse(src) if src else None
    except SyntaxError:
        tree = None
    if tree:
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                targets = [node.target.id]
            if "TOPIC_EXCHANGE_BINDINGS" in targets:
                val = node.value if isinstance(node, ast.Assign) else node.value
                for el in getattr(val, "elts", []):
                    try:
                        bindings.append(ast.literal_eval(el))
                    except ValueError, SyntaxError:
                        bindings.append(("<dynamic>", "<dynamic>"))
    if bindings:
        body += "| Queue | Routing key pattern |\n|---|---|\n"
        for queue, key in bindings:
            body += f"| `{queue}` | `{key}` |\n"
        body += (
            f"\n_Total: {len(bindings)} bindings. The exchange name comes from "
            "the `RABBIT_EXCHANGE` env var._\n\n"
        )
    else:
        body += "_Could not parse QueueTopology.py — check the file by hand._\n\n"
    # Pools live in TWO config modules, and BOTH must be named here. The worker
    # deployable drains the pipeline families; the HTTP deployable drains the
    # queues whose job classes resolve only against its own ``app.jobs``
    # package. Documenting one alone would imply those queues have no consumer
    # — the green-by-omission failure this reference exists to prevent.
    for label in ("services", "api"):
        pools_src = read(root / label / "config" / "queue.py")
        relative = f"{label}/config/queue.py"
        match = re.search(r"WORKER_POOLS[^=]*=\s*(\{.*?\n\})", pools_src, re.DOTALL)
        if not match:
            body += f"_No WORKER_POOLS block found in {relative}._\n\n"
            continue
        try:
            pools = ast.literal_eval(match.group(1))
        except ValueError, SyntaxError:
            # Expected when a product derives concurrency from env().
            body += (
                f"_WORKER_POOLS in {relative} is not a literal (env()-derived "
                "values) — read the file._\n\n"
            )
            continue
        # "timeout" is the BROKER RECONNECT BACKOFF, not a job budget and not
        # an idle poll: consumers hold a long-lived basic_consume, so there is
        # no empty-queue sleep to document. The framework clamps it to [1, 10]
        # and only applies it after a disconnect.
        body += (
            f"## Worker pools ({relative})\n\n"
            "| Pool | Queues | Reconnect backoff |\n|---|---|---|\n"
        )
        for name, cfg in pools.items():
            queues = (
                ", ".join(f"`{q}`" for q in cfg.get("queues", []))
                if isinstance(cfg, dict)
                else ""
            )
            timeout = cfg.get("timeout", "?") if isinstance(cfg, dict) else "?"
            body += f"| {name} | {queues} | {timeout}s |\n"
        body += "\n"
    write_if_changed(manifest.reference / "queues.md", body, "reference/queues.md", say)


# ---------------------------------------------------------------- models / jobs / commands


def scan_classes(dirs: list[Path], attr_patterns: dict[str, str], base: Path):
    """Collects class definitions + requested class attrs via regex.

    ``base`` is the root the reported file paths are relative to; it is a
    parameter (not always the product root) because the claim verifier reuses
    this to read a NEIGHBOURING checkout's command inventory.
    """
    rows = []
    for directory in dirs:
        if not directory.is_dir():
            continue
        for f in sorted(directory.rglob("*.py")):
            if f.name == "__init__.py" or "__pycache__" in f.parts:
                continue
            src = read(f)
            module_doc = ""
            dm = re.match(r'\s*(?:"""|\'\'\')\s*(.+?)[.\n]', src)
            if dm:
                module_doc = dm.group(1).strip()
            for cm in re.finditer(r"^class\s+(\w+)\s*\(([^)]*)\)\s*:", src, re.M):
                cls, bases = cm.group(1), cm.group(2)
                block = src[cm.end() : cm.end() + 2500]
                attrs = {}
                for key, pattern in attr_patterns.items():
                    am = re.search(pattern, block, re.M)
                    attrs[key] = am.group(1) if am else ""
                if "description" in attrs and not attrs["description"]:
                    cd = re.match(r'\s*(?:"""|\'\'\')\s*(.+?)[.\n]', block)
                    attrs["description"] = cd.group(1).strip() if cd else module_doc
                rows.append((cls, bases.strip(), f.relative_to(base), attrs))
    return rows


def gen_models(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the model reference page."""
    root = manifest.root
    dirs = [
        root / "commons" / "models",
        root / "api" / "app" / "models",
        root / "services" / "app" / "models",
    ]
    rows = scan_classes(
        dirs,
        {"table": r'^\s*__table__(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']'},
        base=root,
    )
    rows = [r for r in rows if "Model" in r[1] or r[3]["table"]]
    # Scoping is opt-in through a base class, so the class bases alone decide
    # it — no extra parsing. Publishing it here is what lets prose stop
    # hand-copying a count that rots hourly. A product that declares no scope,
    # or whose models carry none, gets the column omitted rather than printed
    # empty on every row. Both the header and the summary sentence come from
    # the manifest: WHAT the partition is called is the product's word, not
    # the engine's.
    scope = manifest.model_scope
    scoped = (
        {cls for cls, bases, _rel, _attrs in rows if scope.base in bases}
        if scope
        else set()
    )
    body = header(
        manifest, "Models", [str(d.relative_to(root)) for d in dirs if d.is_dir()], now
    )
    if scope and scoped:
        body += (
            f"\n_{len(scoped)} of {len(rows)} models are "
            f"{scope.label.lower()}_ — {scope.note}\n\n"
        )
    cols = (
        f"| Model | Table | {scope.label} | File |"
        if scope and scoped
        else "| Model | Table | File |"
    )
    body += cols + "\n" + "|---" * (4 if scoped else 3) + "|\n"
    for cls, _bases, relative, attrs in rows:
        mid = " ✅ |" if cls in scoped else " — |" if scoped else ""
        body += f"| {cls} | `{attrs['table'] or '—'}` |{mid} `{relative}` |\n"
    body += f"\n_Total: {len(rows)} models._\n"
    write_if_changed(manifest.reference / "models.md", body, "reference/models.md", say)


def gen_jobs(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the queue-job reference page."""
    root = manifest.root
    dirs = [
        root / "services" / "app" / "jobs",
        root / "api" / "app" / "jobs",
        root / "commons" / "contracts" / "envelopes",
    ]
    rows = scan_classes(
        dirs,
        {
            "queue_prefix": r'^\s*queue_prefix(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']',
            "queue": r'^\s*queue(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']',
            "routing": r'^\s*routing_prefix(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']',
            "timeout": r"^\s*timeout(?:\s*:[^=]+)?\s*=\s*(\d+)",
        },
        base=root,
    )
    rows = [r for r in rows if "Job" in r[0] or "Job" in r[1]]
    body = header(
        manifest, "Jobs", [str(d.relative_to(root)) for d in dirs if d.is_dir()], now
    )
    body += "| Job | Queue | Routing | Timeout | File |\n|---|---|---|---|---|\n"
    for cls, _bases, relative, attrs in rows:
        queue = attrs["queue_prefix"] or attrs["queue"] or "—"
        body += (
            f"| {cls} | `{queue}` | `{attrs['routing'] or '—'}` | "
            f"{attrs['timeout'] or '—'} | `{relative}` |\n"
        )
    body += f"\n_Total: {len(rows)} jobs._\n"
    write_if_changed(manifest.reference / "jobs.md", body, "reference/jobs.md", say)


def command_rows(root: Path):
    """(dirs, rows) for deployable and plug-in craft commands of ``root``.

    Shared by the reference page and the claim verifier — the verifier must
    judge a `craft x:y` claim against the SAME inventory the docs advertise,
    otherwise the two disagree and one of them is lying.
    """
    dirs = [
        root / "services" / "app" / "commands",
        root / "api" / "app" / "commands",
        *sorted((root / "services" / "packages").glob("*/commands")),
    ]
    rows = scan_classes(
        dirs,
        {
            "name": r'^\s*name(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']',
            "description": r'^\s*description(?:\s*:[^=]+)?\s*=\s*["\']([^"\']+)["\']',
        },
        base=root,
    )
    return dirs, [r for r in rows if r[3]["name"]]


def gen_commands(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the craft-command reference page."""
    root = manifest.root
    dirs, rows = command_rows(root)
    body = header(
        manifest,
        "Craft commands",
        [str(d.relative_to(root)) for d in dirs if d.is_dir()],
        now,
    )
    body += "| Command | Description | File |\n|---|---|---|\n"
    for _cls, _bases, relative, attrs in sorted(rows, key=lambda r: r[3]["name"]):
        body += (
            f"| `{attrs['name']}` | {md_escape(attrs['description'])} | `{relative}` |\n"
        )
    body += f"\n_Total: {len(rows)} commands._\n"
    write_if_changed(
        manifest.reference / "commands.md", body, "reference/commands.md", say
    )


# ---------------------------------------------------------------- permissions / env / packages


def gen_permissions(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the permission-catalog page, when the product declares one."""
    catalog = manifest.root / "commons" / "gates" / "Permissions.py"
    if not catalog.exists():
        return
    src = read(catalog)
    permissions = {}
    try:
        tree = ast.parse(src)
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                targets = [node.target.id]
            if "PERMISSIONS" in targets:
                permissions = ast.literal_eval(node.value)
    except ValueError, SyntaxError:
        pass
    body = header(manifest, "Permission catalog", ["commons/gates/Permissions.py"], now)
    body += "| Key | Group | Description |\n|---|---|---|\n"
    for key, (group, description) in permissions.items():
        body += f"| `{key}` | {group} | {md_escape(description)} |\n"
    body += (
        f"\n_Total: {len(permissions)} permissions. Projected to the DB with "
        "`craft permissions:sync`._\n"
    )
    write_if_changed(
        manifest.reference / "permissions.md", body, "reference/permissions.md", say
    )


def gen_env(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the environment-variable reference page."""
    root = manifest.root
    scan = [
        root / "api" / "config",
        root / "services" / "config",
        root / "commons" / "contracts",
        root / "commons" / "shared",
    ]
    seen: dict[str, tuple[str, str]] = {}
    for directory in scan:
        if not directory.is_dir():
            continue
        for f in sorted(directory.rglob("*.py")):
            source = read(f)
            tree = ast.parse(source, filename=str(f))
            calls = sorted(
                (
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "env"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                    and re.fullmatch(r"[A-Z0-9_]+", node.args[0].value)
                ),
                key=lambda node: (node.lineno, node.col_offset),
            )
            for call in calls:
                key = call.args[0].value
                default_node = (
                    call.args[1]
                    if len(call.args) > 1
                    else next(
                        (
                            keyword.value
                            for keyword in call.keywords
                            if keyword.arg == "default"
                        ),
                        None,
                    )
                )
                default = (
                    ast.get_source_segment(source, default_node).strip()
                    if default_node is not None
                    else ""
                )
                if key not in seen:
                    seen[key] = (default, str(f.relative_to(root)))
    body = header(
        manifest,
        "Environment variables",
        [str(d.relative_to(root)) for d in scan if d.is_dir()],
        now,
    )
    body += "| Key | Default | Read in |\n|---|---|---|\n"
    for key in sorted(seen):
        default, where = seen[key]
        body += f"| `{key}` | `{md_escape(default) or '—'}` | `{where}` |\n"
    body += (
        f"\n_Total: {len(seen)} env keys. (The first config file where each key "
        "is read is listed.)_\n"
    )
    write_if_changed(manifest.reference / "env.md", body, "reference/env.md", say)


def gen_connectors(manifest: DocsManifest, now: str, say: Say) -> None:
    """Write the plug-in package inventory, when the product ships packages."""
    packages = manifest.root / "services" / "packages"
    if not packages.is_dir():
        return
    body = header(manifest, manifest.packages_page_title, ["services/packages/*"], now)
    body += "| Package | Modules | Connector classes |\n|---|---|---|\n"
    dirs = sorted(
        p for p in packages.iterdir() if p.is_dir() and not p.name.startswith("__")
    )
    for directory in dirs:
        modules = [
            f.stem
            for f in directory.rglob("*.py")
            if f.name != "__init__.py" and "__pycache__" not in f.parts
        ]
        classes: list[str] = []
        for f in directory.rglob("*.py"):
            if "__pycache__" in f.parts:
                continue
            classes += re.findall(r"^class\s+(\w*Connector\w*)\s*\(", read(f), re.M)
        body += (
            f"| {directory.name} | {len(modules)} | "
            f"{', '.join(sorted(set(classes))) or '—'} |\n"
        )
    # Totalled like every other reference page, so prose can cite this line
    # instead of hand-copying a count that drifts on every new package.
    body += f"\n_Total: {len(dirs)} packages._\n"
    write_if_changed(
        manifest.reference / "connectors.md", body, "reference/connectors.md", say
    )


GENERATORS = (
    gen_routes,
    gen_queues,
    gen_models,
    gen_jobs,
    gen_commands,
    gen_permissions,
    gen_env,
    gen_connectors,
)


def generate_reference(manifest: DocsManifest, now: str, say: Say) -> None:
    """Rebuild every code-derived reference page."""
    for generator in GENERATORS:
        generator(manifest, now, say)


__all__ = [
    "GENERATORS",
    "ROUTE_MODULES",
    "VERBS",
    "collect_routes",
    "command_rows",
    "gen_commands",
    "gen_connectors",
    "gen_env",
    "gen_jobs",
    "gen_models",
    "gen_permissions",
    "gen_queues",
    "gen_routes",
    "generate_reference",
    "parse_routes",
    "scan_classes",
]
