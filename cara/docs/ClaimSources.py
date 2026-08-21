"""Filesystem sources the documentation claim verifier resolves against."""

from __future__ import annotations

import os
import re
from contextlib import suppress
from pathlib import Path

PRUNE = {
    ".git",
    "node_modules",
    ".next",
    "__pycache__",
    "venv",
    ".venv",
    "dist",
    "build",
    ".turbo",
    ".pytest_cache",
    "storage",
    ".mypy_cache",
    "coverage",
}

CODE_EXT = {
    ".py",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".mjs",
    ".json",
    ".md",
    ".yml",
    ".yaml",
    ".sql",
    ".sh",
    ".toml",
    ".ini",
    ".txt",
    ".html",
    ".css",
    ".env",
}

_INDEX_CACHE: dict[Path, tuple[dict[str, list[str]], set[str]]] = {}


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def path_index(root: Path) -> tuple[dict[str, list[str]], set[str]]:
    """Return a basename index and the root's top-level names."""
    if root in _INDEX_CACHE:
        return _INDEX_CACHE[root]

    by_name: dict[str, list[str]] = {}
    top: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in PRUNE and not d.startswith(".")]
        here = Path(dirpath)
        if here == root:
            top = set(dirnames) | set(filenames)
        rel_dir = here.relative_to(root).as_posix()
        for name in [*dirnames, *filenames]:
            rel = f"{rel_dir}/{name}" if rel_dir != "." else name
            by_name.setdefault(name, []).append(rel)
    _INDEX_CACHE[root] = (by_name, top)
    return by_name, top


def forget_path_index() -> None:
    """Drop the basename index — for callers that mutate a tree mid-run."""
    _INDEX_CACHE.clear()


def sibling_roots(root: Path) -> list[Path]:
    """Return neighbouring product code roots that own documentation.

    Neighbours are found by the shape of the workspace rather than by name:
    each product lives in its own directory, and its checkout carries the same
    directory name as this one. Naming them would put one product's vocabulary
    inside the other's tooling — the exact coupling this engine exists without.
    """
    out: list[Path] = []
    with suppress(OSError):
        for product in sorted(root.parent.parent.iterdir()):
            candidate = product / root.name
            if candidate != root and (candidate / "docs" / "index.html").exists():
                out.append(candidate)
    return out


def strip_fences(text: str) -> list[str]:
    """Blank fenced-code lines while preserving source line numbers."""
    out: list[str] = []
    inside = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            inside = not inside
            out.append("")
            continue
        out.append("" if inside else line)
    return out


def _resolve_path(token: str, roots: list[Path]) -> bool:
    token = token.strip("/")
    basename = token.rsplit("/", 1)[-1]
    return any(
        relative == token or relative.endswith("/" + token)
        for root in roots
        for relative in path_index(root)[0].get(basename, ())
    )


def check_path_claim(token: str, roots: list[Path]) -> tuple[str, str] | None:
    """Classify one backticked token as ok, broken, unverifiable, or prose."""
    token = token.strip().strip("\"'")
    token = re.sub(r"\(\.\.\.\)$|\(\)$", "", token)
    token = token.split("::", 1)[0]
    token = token.rstrip(".,;:)").strip()
    if "/" not in token or "://" in token or token.startswith(("/", "~", "-", "#", "$")):
        return None
    # A dependency-tree path is real on a machine that has installed, and
    # absent on one that has not — and the path index deliberately skips
    # `node_modules`, so its contents can never be PROVEN either way. Calling
    # that "broken" would fail the suite on a correct sentence, and tooling
    # that rewrites a doc on every run can reintroduce one at will.
    if "node_modules/" in token:
        return ("unverifiable", "inside node_modules; not indexed")
    first = token.split("/", 1)[0]
    if "." in first or not first or any(char in token for char in "<>{}\"'"):
        return None
    last = token.rsplit("/", 1)[-1]
    extension = "." + last.rsplit(".", 1)[-1] if "." in last else ""
    if extension and extension not in CODE_EXT:
        return None
    top_names = set().union(*(path_index(root)[1] for root in roots))
    claims_repo = first in top_names
    if not (claims_repo or extension in CODE_EXT or token.endswith("/")):
        return None
    if any(char in token for char in "*?["):
        hit = any(any(root.glob(token)) for root in roots)
        return (
            ("ok", "")
            if hit
            else ("unverifiable", "glob matched nothing (may be rooted elsewhere)")
        )
    if _resolve_path(token, roots):
        return ("ok", "")
    if not extension and not token.endswith("/"):
        return (
            "unverifiable",
            "extension-less; write a trailing / to assert a directory",
        )
    if claims_repo or extension in CODE_EXT:
        return ("broken", "path does not exist")
    return ("unverifiable", "cannot resolve")


def declared_ports(root: Path, source_dirs: tuple[str, ...]) -> set[str]:
    """Return ports declared by product configuration and infrastructure."""
    files: list[Path] = []
    for subdir in source_dirs:
        files += list((root / subdir).glob(".env.example"))
        files += list((root / subdir / "config").glob("*.py"))
    infrastructure = root / "infrastructure"
    if infrastructure.is_dir():
        files += [
            file
            for file in infrastructure.rglob("*.y*ml")
            if "__pycache__" not in file.parts
        ]
    return {port for file in files for port in re.findall(r"\b(\d{4})\b", _read(file))}


def _owned_markdowns(
    root: Path,
    docs: Path,
    reference: Path,
    product: str,
) -> list[Path]:
    """Return hand-maintained markdown this product is answerable for.

    A neighbouring checkout's atlas is included when it names this product:
    one atlas may govern several checkouts, and the rules it states about this
    one are this one's to keep true.
    """
    doc_files = [
        file for file in sorted(docs.rglob("*.md")) if reference not in file.parents
    ]
    root_files = sorted(root.glob("*.md"))
    tree_files = [
        file
        for directory in sorted(root.iterdir())
        if directory.is_dir()
        and directory.name not in PRUNE
        and not directory.name.startswith(".")
        for file in sorted(directory.glob("*.md"))
    ]
    shared: list[Path] = []
    for sibling in sibling_roots(root):
        atlas = sibling / "CLAUDE.md"
        if atlas.exists() and product in _read(atlas):
            shared.append(atlas)
    # ONE physical file, ONE verdict. These three lists overlap by design: the
    # atlas is a tracked file inside a child repository exposed at the root by
    # symlink, and ``docs/README.md`` is both a docs page and a subtree page.
    # Reporting the same file twice under two labels doubles every finding it
    # makes and invites the reader to fix one copy. The ROOT spelling wins,
    # because that is the position the atlas occupies and the label
    # ``atlas_bans`` already reports.
    seen: set[Path] = set()
    ordered: list[Path] = []
    for file in root_files + doc_files + tree_files + shared:
        resolved = file.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(file)
    return ordered


__all__ = [
    "CODE_EXT",
    "PRUNE",
    "check_path_claim",
    "declared_ports",
    "forget_path_index",
    "path_index",
    "sibling_roots",
    "strip_fences",
]
