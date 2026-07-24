"""FlowLaw: transport edges never skip the use-case service (DOCTRINE §5).

Controllers and queued jobs are adapters. They may validate/decode input and
invoke a use-case service; they must not import repositories, models, kernel
gates or the DB facade, nor resolve repositories from the container.

Existing violations are counted per file in the exact, shrink-only
``seam_allowlists["flow_law"]`` census. New files, growth and stale counts all
fail.
"""

from __future__ import annotations

import ast
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

FLOW_KEY = "flow_law"
_FORBIDDEN_MODULE_PREFIXES = (
    "app.repositories",
    "app.models",
    "app.gates",
    "commons.models",
    "commons.gates",
)
_FORBIDDEN_BARREL_MEMBERS = {
    "app": frozenset({"gates", "models", "repositories"}),
    "commons": frozenset({"gates", "models"}),
}


def _is_edge_path(path: Path, edge_layers: frozenset[str]) -> bool:
    parts = path.parts
    return any(
        part in edge_layers
        and (
            index == 0
            or parts[index - 1] in {"app", "packages"}
            or "packages" in parts[:index]
        )
        for index, part in enumerate(parts)
    )


def _forbidden_import(module: str) -> bool:
    return any(
        module == prefix or module.startswith(f"{prefix}.")
        for prefix in _FORBIDDEN_MODULE_PREFIXES
    )


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _call_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _hits(tree: ast.Module) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if _forbidden_import(module):
                hits.append((node.lineno, f"imports forbidden edge dependency {module}"))
            elif module in _FORBIDDEN_BARREL_MEMBERS and any(
                alias.name in _FORBIDDEN_BARREL_MEMBERS[module]
                for alias in node.names
            ):
                hits.append(
                    (
                        node.lineno,
                        f"imports forbidden edge dependency through {module} barrel",
                    )
                )
            elif module in {"cara", "cara.facades"} and any(
                alias.name == "DB" for alias in node.names
            ):
                hits.append((node.lineno, "imports DB facade at transport edge"))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _forbidden_import(alias.name):
                    hits.append(
                        (
                            node.lineno,
                            f"imports forbidden edge dependency {alias.name}",
                        )
                    )
        elif isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name.endswith((".resolve", ".make")) and node.args:
                target = node.args[0]
                if (
                    isinstance(target, ast.Constant)
                    and isinstance(target.value, str)
                    and target.value.endswith("Repository")
                ) or (
                    isinstance(target, (ast.Name, ast.Attribute))
                    and _call_name(target).endswith("Repository")
                ):
                    hits.append(
                        (
                            node.lineno,
                            f"resolves repository from container via {name}",
                        )
                    )
    return hits


class FlowLaw:
    """Enforce controller/job → use-case service → repository direction."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        found: dict[str, list[tuple[int, str]]] = {}
        seen: set[Path] = set()
        for root in manifest.roots.scan_dirs("flow_law"):
            for path in python_files(root):
                resolved = path.resolve()
                if resolved in seen or path.name == "__init__.py":
                    continue
                seen.add(resolved)
                rel = relpath(path, manifest.roots.deployable)
                if not _is_edge_path(Path(rel), manifest.flow_edge_layers):
                    continue
                tree = parse(path)
                if tree is None:
                    continue
                violations = _hits(tree)
                if violations:
                    found[rel] = violations

        allowlist = manifest.seam_allowlists.get(FLOW_KEY, {})
        findings: list[Finding] = []
        for rel, hits in sorted(found.items()):
            count = len(hits)
            pinned = allowlist.get(rel)
            detail = "; ".join(f"line {line}: {message}" for line, message in hits)
            if pinned is None:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"{count} flow-law violation(s): {detail}",
                    )
                )
            elif count > pinned:
                findings.append(
                    Finding(rel, hits[0][0], f"flow-law debt grew {pinned} -> {count}")
                )
            elif count < pinned:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"stale flow-law pin ({pinned}) — only {count} remain",
                    )
                )
        for rel, pinned in sorted(allowlist.items()):
            if rel not in found:
                findings.append(
                    Finding(rel, 0, f"stale flow-law pin ({pinned}) — no hits remain")
                )
        return findings
