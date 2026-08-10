"""HttpInBusinessLogic: transport types stop at the edge (DOCTRINE §5).

A service, repository, job, listener or port that imports ``cara.http`` /
``cara.request`` / ``cara.response`` — or calls ``abort()`` — has bound a
business rule to one delivery mechanism. It can no longer run from a queue
consumer, a CLI command or a test without an HTTP request object, and the
worker deployable does not even have that layer.

Business logic raises a DOMAIN exception; the edge translates it to a status
code. That translation is the controller's whole job.

Scope is ``roots.scan_dirs("http_in_business_logic")`` — a product declares
exactly which of its layers count as business logic, because that genuinely
differs (an api has ``controllers``, a worker's cross-cutting logic lives in
``support``). The RULE does not differ, which is why it lives here.
"""

from __future__ import annotations

import ast

from cara.architecture._ast_utils import iter_modules
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

#: Edge helper that raises an HTTP response. Never legal in business logic.
ABORT_CALL = "abort"


def _matches(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == p or module.startswith(p + ".") for p in prefixes)


class HttpInBusinessLogic:
    """Business layers import no HTTP transport type and never ``abort()``."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        prefixes = manifest.http_import_prefixes
        findings: list[Finding] = []
        for _path, rel, tree in iter_modules(
            manifest.roots.scan_dirs("http_in_business_logic"),
            manifest.roots.deployable,
        ):
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    if _matches(node.module, prefixes):
                        findings.append(
                            Finding(
                                rel,
                                node.lineno,
                                f"imports an HTTP type from {node.module} — raise a "
                                f"domain exception and let the edge translate it",
                            )
                        )
                elif isinstance(node, ast.Import):
                    findings.extend(
                        Finding(
                            rel,
                            node.lineno,
                            f"imports {alias.name} — HTTP transport stops at the edge",
                        )
                        for alias in node.names
                        if _matches(alias.name, prefixes)
                    )
                elif (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == ABORT_CALL
                ):
                    findings.append(
                        Finding(
                            rel,
                            node.lineno,
                            "calls abort() — raise a domain exception instead",
                        )
                    )
        return findings
