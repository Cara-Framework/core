from __future__ import annotations
"""Warm planner context — build a compact codebase snapshot at plan time.

Goal: when Opus plans a new objective against a project it has never seen,
we burn turns letting it run `find . -type f`, `ls`, and a dozen `cat` calls
just to orient itself. Warm context pre-computes a project map + likely
relevant files locally and injects them into the first planning prompt, so
Opus can skip most of that exploration.

The warm snapshot has three parts:

1. **Project map** — A single `tree`-style listing of source files with
   byte sizes. Skips vendor/build/cache directories and common junk.
2. **Entry points** — Contents of well-known entry files
   (pyproject.toml, package.json, main.py, app.py, index.ts, etc.).
3. **Keyword matches** — Files whose contents match nouns extracted from
   the objective text.

Everything is size-capped so the prompt stays sane.
"""

import re
from pathlib import Path
from typing import Iterable

# Directories we never descend into.
SKIP_DIRS = {
    ".git", ".hg", ".svn",
    "node_modules", "bower_components",
    ".venv", "venv", "env", "__pycache__", ".mypy_cache",
    ".pytest_cache", ".tox", ".eggs", "*.egg-info",
    "dist", "build", "target", "out", ".next", ".nuxt",
    ".turbo", ".cache", ".parcel-cache", ".svelte-kit",
    ".idea", ".vscode", ".vs", ".DS_Store",
    "coverage", ".coverage", "htmlcov",
    "fixtures", "snapshots", "migrations", "seeds",
    "vendor",
    ".usta",  # our own bookkeeping — never leak tasks/logs into the plan
}

# Extensions excluded from keyword matching (still listed in the map).
# SQL dumps, minified JS, snapshot blobs — matching keywords in these
# almost always yields false positives that crowd out real code hits.
NO_KEYWORD_EXTS = {".sql", ".min.js", ".min.css", ".map"}

# Extensions we consider "source code" for mapping purposes.
SOURCE_EXTS = {
    ".py", ".pyi",
    ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs",
    ".php", ".rb", ".go", ".rs", ".java", ".kt", ".scala",
    ".c", ".cc", ".cpp", ".h", ".hpp",
    ".swift", ".m", ".mm",
    ".sh", ".bash", ".zsh",
    ".sql",
    ".html", ".css", ".scss", ".sass", ".less",
    ".vue", ".svelte",
}

# Known entry-point / config files.
ENTRY_POINTS = (
    # Python
    "pyproject.toml", "setup.py", "setup.cfg", "requirements.txt", "Pipfile",
    "manage.py", "main.py", "app.py", "__main__.py", "wsgi.py", "asgi.py",
    # Node / TS
    "package.json", "tsconfig.json", "next.config.js", "next.config.ts",
    "vite.config.ts", "vite.config.js", "webpack.config.js",
    "index.ts", "index.js", "server.ts", "server.js",
    # Other
    "Cargo.toml", "go.mod", "Gemfile", "composer.json",
    "Makefile", "Dockerfile", "docker-compose.yml", "docker-compose.yaml",
    "README.md", "README.rst",
)

# Byte caps — keep the warm prompt bounded.
MAX_MAP_FILES = 600
MAX_ENTRY_POINT_BYTES = 4000     # per entry file
MAX_KEYWORD_FILE_BYTES = 4000    # per keyword-matched file
MAX_KEYWORD_FILES = 6            # top N matches
MAX_TOTAL_BYTES = 60_000         # hard cap on the whole warm context
MIN_KEYWORD_DISTINCT = 2         # require ≥ this many distinct keyword hits


def _should_skip_dir(name: str) -> bool:
    if name in SKIP_DIRS:
        return True
    if name.endswith(".egg-info"):
        return True
    if name.startswith(".") and name not in {".github", ".claude"}:
        return True
    return False


def _iter_source_files(project_dir: Path) -> Iterable[Path]:
    """Yield source files under *project_dir*, skipping junk directories."""
    stack: list[Path] = [project_dir]
    while stack:
        current = stack.pop()
        try:
            entries = sorted(current.iterdir())
        except (OSError, PermissionError):
            continue
        for entry in entries:
            if entry.is_dir():
                if _should_skip_dir(entry.name):
                    continue
                stack.append(entry)
            elif entry.is_file():
                if entry.suffix in SOURCE_EXTS or entry.name in ENTRY_POINTS:
                    yield entry


def build_project_map(project_dir: Path, max_files: int = MAX_MAP_FILES) -> str:
    """Return a compact plain-text listing of source files with byte sizes.

    Format: ``<relative/path>  <bytes>B`` one per line. Sorted alphabetically
    by relative path, capped at *max_files* entries (truncation noted at the
    bottom).
    """
    rows: list[tuple[str, int]] = []
    for f in _iter_source_files(project_dir):
        try:
            size = f.stat().st_size
        except OSError:
            continue
        try:
            rel = str(f.relative_to(project_dir))
        except ValueError:
            continue
        rows.append((rel, size))

    rows.sort(key=lambda r: r[0])
    total_count = len(rows)
    truncated = total_count > max_files
    rows = rows[:max_files]

    lines = [f"{rel}  {size}B" for rel, size in rows]
    if truncated:
        lines.append(f"... (+{total_count - max_files} more files truncated)")
    return "\n".join(lines)


def collect_entry_points(
    project_dir: Path, per_file_cap: int = MAX_ENTRY_POINT_BYTES
) -> list[tuple[str, str]]:
    """Return ``[(relative_path, content), ...]`` for known entry files.

    Each file is truncated to *per_file_cap* bytes. Non-existent entries
    are skipped. We deliberately look only at the project root — entry
    points nested deep inside package dirs are picked up by the keyword
    scanner instead.
    """
    out: list[tuple[str, str]] = []
    for name in ENTRY_POINTS:
        candidate = project_dir / name
        if not candidate.is_file():
            continue
        try:
            text = candidate.read_text(errors="replace")
        except OSError:
            continue
        if len(text) > per_file_cap:
            text = text[:per_file_cap] + "\n... (truncated)"
        out.append((name, text))
    return out


# Words we drop when pulling keywords from the objective — they match
# everything and would add noise to the grep.
_STOPWORDS = frozenset(
    """
    a an the and or but if then else for while of in on at to from by with
    without is are was were be been being do does did not no yes this that
    these those it its they them their we our you your he she his her him
    i me my mine ours yours theirs which who whom whose what where when why
    how as so than up down out over under again further once here there
    all any both each few more most other some such only own same too very
    can will just should now add make create build update fix change rename
    move remove delete replace implement new use using used file files
    function class module import export helper helpers
    support supports format formats handle handles process processes
    value values need needs allow allows enable enables
    """.split()
)

_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{3,}")


def _keywords(objective: str) -> list[str]:
    """Pick out plausibly-meaningful nouns from *objective*.

    Naive but effective: split on non-word chars, drop stopwords, drop words
    shorter than 4 chars, dedupe preserving order, take the first 10.
    """
    seen: set[str] = set()
    keywords: list[str] = []
    for match in _WORD_RE.finditer(objective):
        word = match.group().lower()
        if word in _STOPWORDS or word in seen:
            continue
        seen.add(word)
        keywords.append(word)
        if len(keywords) >= 10:
            break
    return keywords


def _extract_path_hints(objective: str) -> list[str]:
    """Find path-like tokens in the objective (e.g. ``app/helpers``)."""
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*(?:/[A-Za-z_][A-Za-z0-9_]*)+",
                      objective)


def find_keyword_matches(
    objective: str,
    project_dir: Path,
    limit: int = MAX_KEYWORD_FILES,
    per_file_cap: int = MAX_KEYWORD_FILE_BYTES,
) -> list[tuple[str, str]]:
    """Return ``[(rel_path, content), ...]`` for files most relevant to the objective.

    Ranks each source file by how many DISTINCT objective keywords it
    contains (both path hits and content hits, path weighted heavier), and
    returns the top *limit* files that meet ``MIN_KEYWORD_DISTINCT``. Each
    file is truncated to *per_file_cap* bytes. Returns an empty list if no
    keywords could be extracted or nothing cleared the threshold.
    """
    keywords = _keywords(objective)
    path_hints = [p.lower() for p in _extract_path_hints(objective)]
    if not keywords and not path_hints:
        return []

    scores: list[tuple[int, int, str, Path]] = []
    for f in _iter_source_files(project_dir):
        if f.suffix in NO_KEYWORD_EXTS:
            continue
        try:
            text = f.read_text(errors="replace")
        except OSError:
            continue
        lower = text.lower()
        try:
            rel = str(f.relative_to(project_dir))
        except ValueError:
            continue
        rel_lower = rel.lower()
        score = 0
        distinct = 0
        for kw in keywords:
            hit = False
            if kw in rel_lower:
                score += 3  # filename hit is strong evidence
                hit = True
            if kw in lower:
                score += 1
                hit = True
            if hit:
                distinct += 1
        # Path hints ("app/helpers") match the full relative path.
        for hint in path_hints:
            if hint in rel_lower:
                score += 5
                distinct += 1
        if distinct >= MIN_KEYWORD_DISTINCT or any(h in rel_lower for h in path_hints):
            scores.append((score, distinct, rel, f))

    # Highest score first, tie-break by distinct count, then shortest path.
    scores.sort(key=lambda s: (-s[0], -s[1], len(s[2]), s[2]))

    out: list[tuple[str, str]] = []
    for _, _, rel, path in scores[:limit]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        if len(text) > per_file_cap:
            text = text[:per_file_cap] + "\n... (truncated)"
        out.append((rel, text))
    return out


def build_warm_context(
    objective: str,
    project_dir: Path,
    total_cap: int = MAX_TOTAL_BYTES,
) -> str:
    """Build the full warm-context block to prepend to the planning prompt.

    Returns a markdown-formatted string with three sections (project map,
    entry points, keyword-matched files). If nothing interesting is found
    (e.g. empty project), returns an empty string and the caller should
    fall back to Opus exploring normally.

    The returned string is guaranteed to be at most *total_cap* bytes.
    """
    parts: list[str] = []
    used = 0

    project_map = build_project_map(project_dir)
    if project_map:
        header = "## Project map (pre-computed, source files only)\n```\n"
        footer = "\n```\n"
        block = header + project_map + footer
        if used + len(block) <= total_cap:
            parts.append(block)
            used += len(block)

    entries = collect_entry_points(project_dir)
    if entries:
        parts.append("## Entry points\n")
        used += len("## Entry points\n")
        for rel, content in entries:
            block = f"### {rel}\n```\n{content}\n```\n"
            if used + len(block) > total_cap:
                break
            parts.append(block)
            used += len(block)

    matches = find_keyword_matches(objective, project_dir)
    if matches:
        parts.append("## Likely relevant files (keyword-matched)\n")
        used += len("## Likely relevant files (keyword-matched)\n")
        for rel, content in matches:
            block = f"### {rel}\n```\n{content}\n```\n"
            if used + len(block) > total_cap:
                break
            parts.append(block)
            used += len(block)

    if not parts:
        return ""

    preamble = (
        "# Warm context (pre-computed)\n"
        "The project map and likely-relevant files below were gathered "
        "locally before this turn. **Use them first** — only run "
        "`find`/`ls`/`cat` if something you need is missing.\n\n"
    )
    return preamble + "".join(parts)
