"""EnvReadDiscipline: the process environment is read in ``config/`` only.

``env()`` / ``os.getenv`` / ``os.environ`` inside application code creates a
dependency with no injection seam: it cannot be overridden per environment,
cannot be defaulted in one place, and cannot be faked in a test. Configuration
is sourced once, in ``config/``, and injected.

The guard flags four shapes:

* importing anything from ``cara.environment`` (the ``env()`` helper's home),
  or the module itself (``import cara.environment``, ``from cara import
  environment``);
* ``os.getenv(...)`` and ``os.environ.<attr>(...)`` calls;
* ``os.environ[...]`` reads — both through whatever name the ``os`` module is
  bound to, so ``import os as _os`` changes nothing;
* any other reference to ``os.environ`` itself (``"X" in os.environ``,
  ``env = os.environ``, ``dict(os.environ)``, ``for k in os.environ``, passing
  it on): each hands the code the whole environment to read from, so the
  per-variable shapes above were trivially sidestepped;
* a bare reference to a name bound from one of those imports, so
  ``from os import getenv`` followed by ``getenv("X")`` cannot slip past.

``manifest.env_read_exempt_environ_attrs`` names ``os.environ`` methods that
are not reads of a specific variable (``copy()`` snapshots the whole mapping
for a subprocess — a legitimate composition act).

Scope is ``roots.scan_dirs("env_read_discipline")``. ``config/`` is exempt by
being outside every declared root, not by a special case.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import (
    IMPORT_NODE_TYPES,
    all_nodes,
    iter_modules,
    nodes_of,
)
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

ENVIRONMENT_MODULE = "cara.environment"
OS_ENV_NAMES = frozenset({"getenv", "environ"})

_ADVICE = "source it in config/ and inject the value"


def _is_os_environ(node: ast.AST, os_names: set[str]) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id in os_names
    )


def _is_environment_module(dotted: str) -> bool:
    return dotted == ENVIRONMENT_MODULE or dotted.startswith(ENVIRONMENT_MODULE + ".")


class EnvReadDiscipline:
    """Only ``config/`` reads the process environment (DOCTRINE §5)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        exempt = manifest.env_read_exempt_environ_attrs
        findings: list[Finding] = []
        for _path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("env_read_discipline"), manifest.roots.deployable
        ):
            # Names this module bound to an env-reading callable. Collected
            # first so a later bare reference to one of them is recognised
            # wherever it appears in the file.
            bound: set[str] = set()
            # Every name the ``os`` MODULE answers to here. The receiver check
            # compared against the literal ``os``, so ``import os as _os`` made
            # ``_os.getenv("X")`` and ``_os.environ["X"]`` invisible — the
            # module-level twin of the ``from os import getenv`` slip that
            # ``bound`` exists to close.
            os_names: set[str] = {"os"}
            for node in nodes_of(tree, IMPORT_NODE_TYPES):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "os" and alias.asname:
                            os_names.add(alias.asname)
                        elif _is_environment_module(alias.name):
                            findings.append(
                                Finding(
                                    rel,
                                    node.lineno,
                                    f"imports {alias.name} — {_ADVICE}",
                                )
                            )
                    continue
                if not node.module:
                    continue
                if _is_environment_module(node.module):
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            f"imports from {node.module} — {_ADVICE}",
                        )
                    )
                    bound.update(
                        alias.asname or alias.name
                        for alias in node.names
                        if alias.name == "env"
                    )
                elif node.module == "os":
                    bound.update(
                        alias.asname or alias.name
                        for alias in node.names
                        if alias.name in OS_ENV_NAMES
                    )
                elif not node.level and any(
                    _is_environment_module(f"{node.module}.{alias.name}")
                    for alias in node.names
                ):
                    # ``from cara import environment``: the helper's module,
                    # reached through its parent package.
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            f"imports from {ENVIRONMENT_MODULE} — {_ADVICE}",
                        )
                    )

            # ``os.environ`` nodes already judged by a sharper shape below (a
            # method call, a subscript read or write): never reported twice.
            judged: set[int] = set()
            for node in all_nodes(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if _is_os_environ(node.func.value, os_names):
                        judged.add(id(node.func.value))
                elif (
                    "copy" in exempt
                    and isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "dict"
                    and len(node.args) == 1
                    and _is_os_environ(node.args[0], os_names)
                ):
                    # ``dict(os.environ)`` IS ``os.environ.copy()``: the
                    # whole-mapping snapshot the exemption already allows.
                    judged.add(id(node.args[0]))
                elif isinstance(node, ast.Dict) and "copy" in exempt:
                    # ``{**os.environ, "X": "1"}``: the same snapshot, extended.
                    judged.update(
                        id(value)
                        for key, value in zip(node.keys, node.values, strict=True)
                        if key is None and _is_os_environ(value, os_names)
                    )
                elif isinstance(node, ast.Subscript) and _is_os_environ(
                    node.value, os_names
                ):
                    # A write (``os.environ["X"] = …``) or ``del`` is not a
                    # read; the read form is reported by its own shape.
                    judged.add(id(node.value))

            for node in all_nodes(tree):
                if (
                    _is_os_environ(node, os_names)
                    and id(node) not in judged
                    and isinstance(node.ctx, ast.Load)
                ):
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            f"os.environ referenced — {_ADVICE}",
                        )
                    )
                    continue
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    func = node.func
                    if (
                        func.attr == "getenv"
                        and isinstance(func.value, ast.Name)
                        and func.value.id in os_names
                    ):
                        findings.append(
                            Finding(rel, node.lineno, f"os.getenv(...) — {_ADVICE}")
                        )
                    elif _is_os_environ(func.value, os_names) and func.attr not in exempt:
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"os.environ.{func.attr}(...) — {_ADVICE}",
                            )
                        )
                elif (
                    isinstance(node, ast.Subscript)
                    and _is_os_environ(node.value, os_names)
                    and isinstance(node.ctx, ast.Load)
                ):
                    findings.append(
                        Finding(rel, node.lineno, f"os.environ[...] read — {_ADVICE}")
                    )
                elif (
                    isinstance(node, ast.Name)
                    and node.id in bound
                    and isinstance(node.ctx, ast.Load)
                ):
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            f"reads the environment via `{node.id}` — {_ADVICE}",
                        )
                    )
        return findings
