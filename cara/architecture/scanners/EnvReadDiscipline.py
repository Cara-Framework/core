"""EnvReadDiscipline: the process environment is read in ``config/`` only.

``env()`` / ``os.getenv`` / ``os.environ`` inside application code creates a
dependency with no injection seam: it cannot be overridden per environment,
cannot be defaulted in one place, and cannot be faked in a test. Configuration
is sourced once, in ``config/``, and injected.

The guard flags four shapes:

* importing anything from ``cara.environment`` (the ``env()`` helper's home);
* ``os.getenv(...)`` and ``os.environ.<attr>(...)`` calls;
* ``os.environ[...]`` reads;
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

from cara.architecture._ast_utils import iter_modules
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

ENVIRONMENT_MODULE = "cara.environment"
OS_ENV_NAMES = frozenset({"getenv", "environ"})

_ADVICE = "source it in config/ and inject the value"


def _is_os_environ(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


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
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if node.module == ENVIRONMENT_MODULE or node.module.startswith(
                    ENVIRONMENT_MODULE + "."
                ):
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

            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    func = node.func
                    if (
                        func.attr == "getenv"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "os"
                    ):
                        findings.append(
                            Finding(rel, node.lineno, f"os.getenv(...) — {_ADVICE}")
                        )
                    elif _is_os_environ(func.value) and func.attr not in exempt:
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"os.environ.{func.attr}(...) — {_ADVICE}",
                            )
                        )
                elif (
                    isinstance(node, ast.Subscript)
                    and _is_os_environ(node.value)
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
