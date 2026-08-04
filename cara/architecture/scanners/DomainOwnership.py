"""DomainOwnership: each domain's repositories have one service door.

DOCTRINE §5 permits a service to call another domain's service, but never
another domain's repository.  Repository-to-repository reaches are forbidden
for the same reason: they bypass the owning domain's use-case boundary.

Existing violations are counted per file in the exact, shrink-only
``seam_allowlists["domain_ownership"]`` census.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

DOMAIN_OWNERSHIP_KEY = "domain_ownership"


def _repository_symbols(manifest: Manifest) -> dict[str, frozenset[str]]:
    """Map public repository symbols to the domain directories that own them."""
    owners: dict[str, set[str]] = defaultdict(set)
    root = manifest.roots.app / "repositories"
    if not root.is_dir():
        return {}
    for domain_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        for path in python_files(domain_dir):
            if path.name == "__init__.py":
                continue
            owners[path.stem].add(domain_dir.name)
            tree = parse(path)
            if tree is None:
                continue
            for node in tree.body:
                if isinstance(
                    node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
                ) and not node.name.startswith("_"):
                    owners[node.name].add(domain_dir.name)
    return {name: frozenset(domains) for name, domains in owners.items()}


def _source_domain(manifest: Manifest, path: Path) -> tuple[str, str] | None:
    try:
        parts = path.relative_to(manifest.roots.app).parts
    except ValueError:
        return None
    if len(parts) < 3 or parts[0] not in {"repositories", "services"}:
        return None
    return parts[0], parts[1]


def _absolute_module(rel: str, node: ast.ImportFrom) -> str:
    module = node.module or ""
    if not node.level:
        return module
    package = list(Path(rel).with_suffix("").parts[:-1])
    keep = max(0, len(package) - node.level + 1)
    suffix = module.split(".") if module else []
    return ".".join([*package[:keep], *suffix])


def _repository_target(
    module: str,
    symbol: str | None,
    symbols: dict[str, frozenset[str]],
) -> tuple[frozenset[str], str] | None:
    if module == "app.repositories":
        if symbol is None:
            return frozenset(), "opaque repository-barrel import"
        owners = symbols.get(symbol, frozenset())
        if not owners:
            return None
        return owners, symbol
    prefix = "app.repositories."
    if module.startswith(prefix):
        target = module[len(prefix) :].split(".", 1)[0]
        return frozenset({target}), symbol or module
    if module == "app" and symbol == "repositories":
        return frozenset(), "opaque repository-barrel import"
    return None


def _violations(
    tree: ast.Module,
    rel: str,
    own_domain: str,
    symbols: dict[str, frozenset[str]],
) -> list[tuple[int, str]]:
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = _absolute_module(rel, node)
            for alias in node.names:
                target = _repository_target(module, alias.name, symbols)
                if target is None:
                    continue
                owners, label = target
                if not owners:
                    violations.append(
                        (node.lineno, f"{label} hides the repository owner")
                    )
                elif owners != {own_domain}:
                    rendered = ", ".join(sorted(owners))
                    violations.append(
                        (
                            node.lineno,
                            f"{label} belongs to domain(s) {rendered}, not {own_domain}",
                        )
                    )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                target = _repository_target(alias.name, None, symbols)
                if target is None:
                    continue
                owners, label = target
                if not owners:
                    violations.append(
                        (node.lineno, f"{label} hides the repository owner")
                    )
                elif owners != {own_domain}:
                    rendered = ", ".join(sorted(owners))
                    violations.append(
                        (
                            node.lineno,
                            f"{alias.name} belongs to domain(s) {rendered}, "
                            f"not {own_domain}",
                        )
                    )
    return violations


class DomainOwnership:
    """Forbid service/repository access to another domain's repository."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        symbols = _repository_symbols(manifest)
        found: dict[str, list[tuple[int, str]]] = {}
        seen: set[Path] = set()
        for root in manifest.roots.scan_dirs(DOMAIN_OWNERSHIP_KEY):
            for path in python_files(root):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                source = _source_domain(manifest, path)
                if source is None or path.name == "__init__.py":
                    continue
                _, own_domain = source
                tree = parse(path)
                if tree is None:
                    continue
                rel = relpath(path, manifest.roots.deployable)
                hits = _violations(tree, rel, own_domain, symbols)
                if hits:
                    found[rel] = hits

        allowlist = manifest.seam_allowlists.get(DOMAIN_OWNERSHIP_KEY, {})
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
                        f"{count} domain-ownership violation(s): {detail}",
                    )
                )
            elif count > pinned:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"domain-ownership debt grew {pinned} -> {count}",
                    )
                )
            elif count < pinned:
                findings.append(
                    Finding(
                        rel,
                        hits[0][0],
                        f"stale domain-ownership pin ({pinned}) — only {count} remain",
                    )
                )
        for rel, pinned in sorted(allowlist.items()):
            if rel not in found:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"stale domain-ownership pin ({pinned}) — no hits remain",
                    )
                )
        return findings
