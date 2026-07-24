"""VerticalSliceSeams: plugin tokens confined to the Four Legal Seams
(DOCTRINE §4).

A plugin's name (``manifest.plugin_tokens`` — marketplace/vendor slugs)
may appear outside ``packages/<plugin>/`` in exactly FOUR places:

1. data vocabulary — slug constants under ``manifest.seam_locations
   .data_vocabulary_prefixes`` (a kernel models package);
2. composition roots — ``manifest.seam_locations.composition_roots``;
3. generic ingress — parameterized route strings never touch an
   identifier or the four literal positions this scanner checks, so no
   location is needed for it;
4. manifest data — ``manifest.seam_locations.manifest_files``.

Non-marketplace provider capabilities are not marketplace plug-ins. A product
may declare an owned integration lane (for example
``discovery/google_shopping``) and the exact token vocabulary that lane owns.
The lane remains scanned; only its declared tokens are legal there. Core code
still reaches the lane through a generic registry/port rather than importing a
provider implementation.

What counts as a token appearance:

* **identifier surfaces** — module-path parts, class/function/async-def
  names, import module paths and imported/aliased names, assignment
  target names at module/class level;
* **closing a known evasion — string literals in four syntactic
  positions**: a Compare operand (``if slug == "ebay":`` dodges the
  identifier scan the same branch on ``Channel.MARKETPLACE_EBAY`` would
  not), a function/lambda default value, a dict key, and a call argument
  (positional or keyword).

Prose, comments, docstrings and STRING LITERALS OUTSIDE those four
positions are exempt (a bare assignment of a brand slug to its own
UPPER_SNAKE constant — the data-vocabulary seam itself — is a plain
``Assign``, none of the four scanned positions, so it never needs a
special case).

Violations are counted per file against a dated, shrink-only sunset debt
(``manifest.seam_allowlists["vertical_slice_seams"]``): a NEW file or a
GROWN count is a seam leak; a SHRUNK count or a vanished file is a stale
pin to ratchet down in the same change.

Scanned trees: ``app``, ``config``, ``routes`` and every dev-only kernel
package. ``packages/`` (the plugins' own home) is out of scope by design.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from cara.architecture._ast_utils import docstring_node_ids, parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

SEAM_KEY = "vertical_slice_seams"
_UPPER_SNAKE = re.compile(r"^[A-Z0-9_]+$")


def _token_re(manifest: Manifest) -> re.Pattern[str]:
    tokens = sorted(manifest.plugin_tokens, key=len, reverse=True)
    return re.compile("(" + "|".join(re.escape(t) for t in tokens) + ")", re.IGNORECASE)


def _identifier_hits(rel: str, tree: ast.Module, token_re: re.Pattern[str]) -> list[str]:
    hits: list[str] = []
    for part in Path(rel).parts:
        if token_re.search(part):
            hits.append(f"module-path {part}")
            break

    def scan_assigns(body: list[ast.stmt]) -> None:
        for stmt in body:
            if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                for target in targets:
                    if isinstance(target, ast.Name) and token_re.search(target.id):
                        hits.append(f"assign {target.id} (line {stmt.lineno})")
            elif isinstance(stmt, ast.ClassDef):
                scan_assigns(stmt.body)

    scan_assigns(tree.body)
    for node in ast.walk(tree):
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if token_re.search(node.name):
                hits.append(f"def {node.name} (line {node.lineno})")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if token_re.search(alias.name) or (
                    alias.asname and token_re.search(alias.asname)
                ):
                    hits.append(f"import {alias.name} (line {node.lineno})")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                dotted = f"{module}.{alias.name}" if module else alias.name
                if token_re.search(dotted) or (
                    alias.asname and token_re.search(alias.asname)
                ):
                    hits.append(f"import {dotted} (line {node.lineno})")
    return hits


def _string_literal_hits(tree: ast.Module, token_re: re.Pattern[str]) -> list[str]:
    """Closing a known evasion: brand tokens smuggled as bare string
    literals in the four positions a branch/lookup can hide one — never
    prose, docstrings or comments (structurally out of AST reach here)."""
    doc_ids = docstring_node_ids(tree)

    def literal(node: ast.AST) -> str | None:
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in doc_ids
        ):
            return node.value
        return None

    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for operand in (node.left, *node.comparators):
                text = literal(operand)
                if text and token_re.search(text):
                    hits.append(f"compare-literal {text!r} (line {operand.lineno})")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            for default in (*node.args.defaults, *node.args.kw_defaults):
                if default is None:
                    continue
                text = literal(default)
                if text and token_re.search(text):
                    hits.append(f"default-literal {text!r} (line {default.lineno})")
        elif isinstance(node, ast.Dict):
            for key in node.keys:
                if key is None:
                    continue
                text = literal(key)
                if text and token_re.search(text):
                    hits.append(f"dict-key-literal {text!r} (line {key.lineno})")
        elif isinstance(node, ast.Call):
            for arg in (*node.args, *(kw.value for kw in node.keywords)):
                text = literal(arg)
                if text and token_re.search(text):
                    hits.append(f"call-arg-literal {text!r} (line {arg.lineno})")
    return hits


def _seam_filter(
    manifest: Manifest,
    rel: str,
    hits: list[str],
    token_re: re.Pattern[str],
) -> list[str]:
    seams = manifest.seam_locations
    # The architecture manifest is itself machine-readable manifest data. Its
    # token inventory and counted sunset paths must not become self-findings
    # when literal scanning is enabled.
    if rel == "app/architecture_manifest.py":
        return []
    if rel in seams.composition_roots:
        return []
    if rel in seams.manifest_files:
        return []
    for prefix, owned_tokens in seams.owned_integration_prefixes.items():
        normalized = prefix.rstrip("/")
        if rel != normalized and not rel.startswith(f"{normalized}/"):
            continue
        allowed = {token.casefold() for token in owned_tokens}
        remaining: list[str] = []
        for hit in hits:
            present = {
                match.group(0).casefold() for match in token_re.finditer(hit)
            }
            if present and present <= allowed:
                continue
            remaining.append(hit)
        return remaining
    if any(rel.startswith(prefix) for prefix in seams.data_vocabulary_prefixes):
        remaining = []
        for hit in hits:
            if hit.startswith("assign "):
                name = hit.split(" ", 2)[1]
                if _UPPER_SNAKE.match(name):
                    continue
            remaining.append(hit)
        return remaining
    return hits


def _scan(manifest: Manifest) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    token_re = _token_re(manifest)
    scan_bases = list(manifest.roots.scan_dirs("vertical_slice_seams"))
    scan_bases.extend(
        pkg_dir
        for pkg, pkg_dir in manifest.roots.kernel.items()
        if pkg in manifest.seam_kernel_packages
    )
    for base in scan_bases:
        for path in python_files(base):
            tree = parse(path)
            if tree is None:
                continue
            rel = relpath(path, manifest.roots.deployable)
            hits = _identifier_hits(rel, tree, token_re)
            if manifest.scan_plugin_string_literals:
                hits += _string_literal_hits(tree, token_re)
            hits = _seam_filter(manifest, rel, hits, token_re)
            if hits:
                found[rel] = hits
    return found


class VerticalSliceSeams:
    """Plugin brand tokens appear ONLY at the Four Legal Seams (§4)."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        if not manifest.plugin_tokens:
            return []
        if not manifest.scan_plugin_string_literals:
            return [
                Finding(
                    "app/architecture_manifest.py",
                    0,
                    "scan_plugin_string_literals must be true when plugin_tokens "
                    "are declared — identifier-only scanning leaves branch and "
                    "lookup coupling invisible",
                )
            ]
        found = _scan(manifest)
        allowlist = manifest.seam_allowlists.get(SEAM_KEY, {})
        findings: list[Finding] = []
        for rel, hits in sorted(found.items()):
            pinned = allowlist.get(rel)
            if pinned is None:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"{len(hits)} plugin token(s) outside the Four Legal Seams: "
                        + "; ".join(hits),
                    )
                )
            elif len(hits) > pinned:
                findings.append(
                    Finding(
                        rel, 0, f"token count grew {pinned} -> {len(hits)} (shrink-only)"
                    )
                )
            elif len(hits) < pinned:
                findings.append(
                    Finding(
                        rel,
                        0,
                        f"stale allowlist pin ({pinned}) — only {len(hits)} remain",
                    )
                )
        for rel, pinned in sorted(allowlist.items()):
            if rel not in found:
                findings.append(
                    Finding(rel, 0, f"stale allowlist pin ({pinned}) — no hits remain")
                )
        return findings
