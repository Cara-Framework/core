from __future__ import annotations
"""Runner — executes tasks via Qwen3-Coder (default) or aider (opt-in).

Default executor: Qwen3-Coder over OpenRouter chat completions. We call the
API directly, ask for whole-file blocks, parse, syntax-validate, and write.
No tool-calling — Qwen3-Coder corrupts write_file args in tool-call format,
but is clean in plain chat mode.

Opt-in executor: aider (``cfg.executor == "aider"``). Spawns the aider CLI
subprocess. Good for large files where a targeted search/replace beats a
full rewrite. Watchdogged so it can't hang forever.

No fallback chain. The chosen executor is the only executor for the run.
"""

import ast
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from .config import Cfg
from .tasks import Task, today_dir


# ── Wire format ─────────────────────────────────────────────

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

QWEN_SYSTEM_PROMPT = """You are a precise code editor. You will receive:
1. A task description (markdown)
2. The current contents of files to modify (empty means the file does not exist yet)
3. Optional read-only <<<REF: ...>>> blocks — sibling files that demonstrate
   the local conventions. Study them to match import paths, base classes,
   method names, and naming style. NEVER emit output for REF files.

Return the COMPLETE final version of every file you create or modify.
Use this EXACT output format, nothing else:

<<<FILE: relative/path/to/file.ext>>>
<complete file content here, exactly as it should be on disk>
<<<END>>>

If you need to delete a file, emit on its own line:
<<<DELETE: relative/path/to/file.ext>>>

For LARGE existing files (when the user prompt marks a file as eligible for
edit-block mode), you may instead emit targeted search/replace patches:

<<<EDIT: relative/path/to/file.ext>>>
<<<SEARCH>>>
exact lines to find (byte-for-byte, including leading whitespace)
<<<REPLACE>>>
new lines to put there
<<<END>>>

Edit blocks only change the matched region; all other lines are left
untouched. SEARCH text must be unique within the target file — include
2–3 lines of surrounding context to guarantee uniqueness. Multiple edit
blocks for the same file are allowed and are applied in the order given.
For brand-new files and small files, keep using <<<FILE: ...>>> whole-file
blocks.

Rules:
- Emit ONLY file blocks. No prose, no markdown fences, no JSON wrapping, no explanations.
- Each <<<FILE: ...>>> block must contain the COMPLETE final file content, not a diff.
- TESTING: never use `assert x is True` or `assert x is False` for functions that return
  truthy/falsy values. Use `assert x` / `assert not x` / `assert bool(x) is True` instead.
  Many Python methods return the value itself (a string, dict, etc.), not a literal bool.
- Preserve existing style, imports, and docstrings unless the task says otherwise.
- If a file is listed but does not need changes, do NOT emit a block for it.
- Never include markdown code fences (```) anywhere in your output.
- Never emit Python data literals (dict/list) as the file content — always emit real code.
- NEVER emit output for <<<REF: ...>>> files. They are read-only convention references.
- When rewriting an existing file, copy unchanged lines EXACTLY. Do not drop imports,
  __all__ entries, class headers, or decorators. A silent-deletion guard will catch
  drops and reject your output.
- CRITICAL: when you DELETE a file (<<<DELETE: ...>>>), you MUST also update every file
  that imports from the deleted file. At minimum update __init__.py, __all__, and any
  direct callers. Leaving stale imports causes ImportError at runtime.
- When a file imports from a package __init__.py, check the existing __init__.py exports
  and update them if you add/remove/rename a module.
"""

FILE_BLOCK_RE = re.compile(
    r"<<<FILE:\s*(.+?)>>>\s*\n(.*?)\n?<<<END>>>",
    re.DOTALL,
)
DELETE_BLOCK_RE = re.compile(r"<<<DELETE:\s*(.+?)>>>")

# FIX 1b: targeted search/replace edits for existing large files.
# Format:
#   <<<EDIT: path/to/file.py>>>
#   <<<SEARCH>>>
#   exact lines to find
#   <<<REPLACE>>>
#   new lines to put there
#   <<<END>>>
#
# Qwen sometimes falls back to aider-style conflict markers
# (``<<<<<<< SEARCH`` / ``=======`` / ``>>>>>>> REPLACE``) instead of the
# clean ``<<<SEARCH>>>`` / ``<<<REPLACE>>>`` markers. We accept BOTH
# variants in the regex so we don't silently drop edits. The parser
# below also verifies that the number of parsed edits matches the
# number of ``<<<EDIT:`` tags in the raw response — if not, retry is
# forced with a format-reminder message so Qwen can re-emit the dropped
# blocks in the correct shape.
EDIT_BLOCK_RE = re.compile(
    r"<<<EDIT:\s*(.+?)>>>\s*\n"
    r"(?:<<<SEARCH>>>|<{3,}\s*SEARCH)\s*\n(.*?)\n?"
    r"(?:<<<REPLACE>>>|={3,})\s*\n(.*?)\n?"
    r"(?:<<<END>>>|>{3,}\s*REPLACE(?:\s*>>>)?\s*\n?<<<END>>>|>{3,}\s*REPLACE\s*>>>|>{3,}\s*REPLACE)",
    re.DOTALL,
)

# How many times ``<<<EDIT:`` appears in the raw text. Used to detect
# parse drift (Qwen emitted an edit block but our regex couldn't match
# its exact marker flavor).
_EDIT_TAG_RE = re.compile(r"<<<EDIT:\s*.+?>>>")

# Files bigger than this threshold (lines) get the edit-block hint in the
# prompt when they already exist on disk. Small files stay on whole-file
# rewrites because search/replace is overkill for a 20-line helper.
EDIT_BLOCK_LINE_THRESHOLD = 40

# Edit-block mode is always on. Qwen can still emit <<<FILE: ...>>>
# whole-file blocks when it prefers — the format is additive.
def _edit_blocks_enabled() -> bool:
    return True


# ── Task-file resolution ────────────────────────────────────

def _find_task(task: Task, cfg: Cfg) -> Optional[Path]:
    d = today_dir(cfg) / task.filename
    if d.exists():
        return d
    if cfg.tasks_dir.exists():
        for dd in sorted(cfg.tasks_dir.iterdir(), reverse=True):
            c = dd / task.filename
            if c.exists():
                return c
    return None


# ── Prompt + parsing ────────────────────────────────────────

def _pick_convention_refs(
    project_dir: Path, target_rel: str, exclude: set[str], limit: int = 2
) -> list[str]:
    """Pick up to *limit* sibling files of the same type as *target_rel*
    for use as read-only convention references in the applier prompt.

    Heuristic: look in the same directory as the target, sort by size
    (smallest first so we don't blow the context window), exclude the
    target itself, ``__init__.py``, and anything already listed in
    *exclude*. Only returns files within 30KB so we don't inject a
    monster.
    """
    target_path = project_dir / target_rel
    parent = target_path.parent
    if not parent.exists() or not parent.is_dir():
        return []
    suffix = target_path.suffix
    if not suffix:
        return []
    target_name = target_path.name
    candidates: list[tuple[int, Path]] = []
    try:
        for sibling in parent.iterdir():
            if not sibling.is_file():
                continue
            if sibling.suffix != suffix:
                continue
            if sibling.name in ("__init__.py", target_name):
                continue
            rel = str(sibling.relative_to(project_dir))
            if rel in exclude:
                continue
            try:
                size = sibling.stat().st_size
            except OSError:
                continue
            if size > 30_000:
                continue
            candidates.append((size, sibling))
    except OSError:
        return []
    candidates.sort(key=lambda pair: pair[0])
    out: list[str] = []
    for _, path in candidates[:limit]:
        out.append(str(path.relative_to(project_dir)))
    return out


MAX_CONVENTIONS_CHARS = 4000  # hard cap to avoid context blowup


def _load_conventions(project_dir: Path) -> str:
    """Load project conventions from .usta/CONVENTIONS.md or CODING_RULES.md.

    Caps at MAX_CONVENTIONS_CHARS. If the file is bigger, keeps the first
    chunk and appends a truncation note — the top of these files is almost
    always the most important (general rules, architecture pattern, naming).
    """
    for name in (".usta/CONVENTIONS.md", "CONVENTIONS.md", "CODING_RULES.md"):
        p = project_dir / name
        if p.exists() and p.is_file():
            try:
                text = p.read_text(errors="replace").strip()
                if not text:
                    continue
                if len(text) > MAX_CONVENTIONS_CHARS:
                    text = text[:MAX_CONVENTIONS_CHARS] + "\n\n… (truncated)"
                return text
            except OSError:
                continue
    return ""


def _build_prompt(task_md: str, project_dir: Path, files: list[str]) -> str:
    parts = [f"# TASK\n\n{task_md.strip()}\n"]
    # Inject project conventions so Qwen follows local coding rules.
    conventions = _load_conventions(project_dir)
    if conventions:
        parts.append(
            f"# PROJECT RULES (follow strictly, never deviate)\n\n{conventions}\n"
        )
    large_existing: list[str] = []  # files eligible for edit-block mode
    if files:
        parts.append("# CURRENT FILE CONTENTS\n")
        for rel in files:
            p = project_dir / rel
            if p.exists() and p.is_file():
                try:
                    content = p.read_text(errors="replace")
                except OSError as e:
                    content = f"<read error: {e}>"
                parts.append(f"\n<<<FILE: {rel}>>>\n{content}\n<<<END>>>\n")
                # FIX 1b: tag large existing files for edit-block mode.
                if (
                    _edit_blocks_enabled()
                    and content.count("\n") + 1 >= EDIT_BLOCK_LINE_THRESHOLD
                ):
                    large_existing.append(rel)
            else:
                parts.append(
                    f"\n<<<FILE: {rel}>>>\n<file does not exist — create it from scratch>\n<<<END>>>\n"
                )

        # FIX 8: convention reference injection.
        # For each target file, pick up to 2 existing sibling files of the
        # same type and include them as READ-ONLY references. This anchors
        # Qwen to the local conventions (imports, naming, method names)
        # without relying on the planner to enumerate them.
        exclude = set(files)
        ref_blocks: list[str] = []
        for rel in files:
            refs = _pick_convention_refs(project_dir, rel, exclude, limit=2)
            for ref in refs:
                if ref in exclude:
                    continue
                exclude.add(ref)
                rp = project_dir / ref
                try:
                    ref_content = rp.read_text(errors="replace")
                except OSError:
                    continue
                # Hard cap per reference at 6000 chars.
                if len(ref_content) > 6000:
                    ref_content = ref_content[:6000] + "\n# … truncated\n"
                ref_blocks.append(
                    f"\n<<<REF: {ref}>>>\n{ref_content}\n<<<END>>>\n"
                )
        if ref_blocks:
            parts.append(
                "\n# CONVENTION REFERENCES (read-only)\n\n"
                "Existing sibling files of the same type. **Match their "
                "conventions exactly** — if they import `X` from "
                "`package.module`, you import `X` from `package.module`, "
                "not from a relative path. If they implement method "
                "`default_message()`, you implement `default_message()`, "
                "not `message()`. If they use `snake_case`, you use "
                "`snake_case`. These files are NOT in your output scope — "
                "do NOT emit <<<FILE: ...>>> blocks for them.\n"
            )
            parts.extend(ref_blocks)
    else:
        parts.append(
            "# NOTE\n\nNo explicit file list provided — infer the files to "
            "modify from the task description above. You may create, edit, or "
            "delete any files the task describes, within the project root.\n"
        )
    parts.append(
        "\n# INSTRUCTIONS\n\n"
        "Emit the final version of every file you change, using the <<<FILE: ...>>> "
        "block format specified in the system message. Do not include any prose. "
        "When copying unchanged parts of an existing file, copy them EXACTLY — "
        "do not drop imports, `__all__` entries, class headers, or decorators. "
        "Silent deletions will be detected and the output will be rejected."
    )

    # FIX 1b: if any target file is large and already exists, strongly
    # encourage targeted edit blocks over whole-file rewrites. Whole-file
    # rewrites are where Qwen drops imports; SEARCH/REPLACE edits are
    # mechanically safe because we leave unmatched lines alone.
    if large_existing:
        pretty = ", ".join(f"`{p}`" for p in large_existing)
        parts.append(
            "\n# PREFERRED FORMAT FOR LARGE EXISTING FILES\n\n"
            f"The following files are non-trivial and already exist: {pretty}.\n\n"
            "**Use targeted edit blocks for these files instead of rewriting "
            "the whole file.** Format:\n\n"
            "```\n"
            "<<<EDIT: path/to/file.py>>>\n"
            "<<<SEARCH>>>\n"
            "exact lines to find (must match byte-for-byte, including "
            "indentation)\n"
            "<<<REPLACE>>>\n"
            "new lines to put there\n"
            "<<<END>>>\n"
            "```\n\n"
            "Rules for edit blocks:\n"
            "- SEARCH text must match exactly once in the current file. "
            "Include enough surrounding context (2–3 lines above and below "
            "the change) to make the match unique.\n"
            "- All other lines in the file are left untouched — this is "
            "safer than whole-file rewrites and avoids silent deletion of "
            "imports.\n"
            "- Emit one <<<EDIT: ...>>> block per independent change. "
            "Multiple edits to the same file are allowed and are applied "
            "top-to-bottom in the order you emit them.\n"
            "- To APPEND to a file, put the last existing line in SEARCH "
            "and repeat it in REPLACE followed by the new lines.\n"
            "- New files and small files (< 40 lines) should still use "
            "<<<FILE: ...>>> whole-file blocks — edit blocks are only for "
            "the large existing files listed above.\n"
        )
    return "\n".join(parts)


def _parse_blocks(
    text: str,
) -> tuple[
    list[tuple[str, str]],
    list[str],
    list[tuple[str, str, str]],
    int,
]:
    """Parse Qwen output into (files, deletes, edits, edit_tag_mismatch).

    - files: ``[(rel_path, full_content)]`` — whole-file rewrites
    - deletes: ``[rel_path]``
    - edits: ``[(rel_path, search_text, replace_text)]`` — targeted
      search/replace patches (FIX 1b). Multiple edits for the same
      file are allowed and applied in order.
    - edit_tag_mismatch: number of ``<<<EDIT:`` tags in the raw text
      that we could NOT parse into a complete (search, replace) pair.
      Non-zero values are treated as a fatal format error in the retry
      loop and Qwen is asked to re-emit the dropped blocks.
    """
    files = FILE_BLOCK_RE.findall(text)
    deletes = [d.strip() for d in DELETE_BLOCK_RE.findall(text)]
    edits = [
        (p.strip(), s, r) for (p, s, r) in EDIT_BLOCK_RE.findall(text)
    ]
    files = [(p.strip(), c) for p, c in files if p.strip() not in deletes]
    # Tag vs parsed-edit count. Anything left over is a silent drop.
    edit_tag_count = len(_EDIT_TAG_RE.findall(text))
    edit_tag_mismatch = max(0, edit_tag_count - len(edits))
    return files, deletes, edits, edit_tag_mismatch


# ── Validation + apply ──────────────────────────────────────

def _validate(path: str, content: str) -> tuple[bool, str]:
    """Guard rail: reject obviously corrupt output before touching disk."""
    stripped = content.lstrip()
    if path.endswith((".py", ".pyi")):
        if stripped and stripped[0] in "[{" and not stripped.startswith("{%"):
            return False, "rejected: content starts with [ or { (corruption pattern)"
        try:
            ast.parse(content)
        except SyntaxError as e:
            return False, f"rejected: python syntax error — {e}"
    if path.endswith(".json"):
        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            return False, f"rejected: invalid JSON — {e}"
    return True, ""


# Lines that, if silently dropped during a whole-file rewrite, are almost
# always a hallucination rather than an intentional deletion. Used by
# ``_detect_silent_deletions`` as the trip-wire for FIX 1a.
_SILENT_DROP_PATTERNS = (
    "import ",
    "from ",
    "__all__",
    "class ",
    "def ",
    "@",
)


def _rel_to_module(rel: str) -> str | None:
    """Map a relative .py path to a dotted module name for importing."""
    if not rel.endswith(".py"):
        return None
    parts = list(Path(rel).with_suffix("").parts)
    if not parts:
        return None
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if not parts:
        return None
    return ".".join(parts)


def _smoke_import(project_dir: Path, rel: str, timeout: int = 10) -> tuple[bool, str]:
    """Try to import *rel* as a module in a subprocess.

    Runs ``python3 -c "import <module>"`` with ``cwd=project_dir`` so the
    project root is on sys.path. Uses a subprocess so any ImportError or
    SyntaxError bubbles up as a return code rather than crashing the runner.

    Returns (ok, error_message). Non-Python files and files we can't map
    to a module name auto-pass.
    """
    mod = _rel_to_module(rel)
    if not mod:
        return True, ""
    try:
        r = subprocess.run(
            ["python3", "-c", f"import {mod}"],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, f"smoke-import timeout/error: {e}"
    if r.returncode != 0:
        # Last line of stderr is usually the actionable one.
        stderr = (r.stderr or "").strip().splitlines()
        msg = stderr[-1] if stderr else f"exit {r.returncode}"
        # ModuleNotFoundError for an unrelated dep is noise, not a bug
        # in the file we just wrote — pass those through.
        if "ModuleNotFoundError" in msg and mod.split(".")[0] not in msg:
            return True, ""
        return False, msg
    return True, ""


def _detect_silent_deletions(
    before: str, after: str, task_md: str
) -> list[str]:
    """Return ``before`` lines that vanished in ``after`` without the task
    asking for them.

    Focuses on high-signal lines: imports, ``__all__`` entries, class/def
    headers, and decorators. These are the lines Qwen drops while
    "faithfully copying" a whole-file rewrite.

    If the task markdown explicitly mentions a dropped line (by substring
    or by key symbol), we assume the deletion was intentional and do not
    flag it.
    """
    before_lines = [l.rstrip() for l in before.splitlines()]
    after_set = set(l.rstrip() for l in after.splitlines())
    task_lower = task_md.lower()
    dropped: list[str] = []
    for line in before_lines:
        stripped = line.lstrip()
        if not stripped:
            continue
        if not any(stripped.startswith(p) for p in _SILENT_DROP_PATTERNS):
            continue
        if line.rstrip() in after_set:
            continue
        # Line no longer exists in the new file. Check if the task
        # asked for this removal using multiple strategies.
        hint = stripped.split("#")[0].strip()
        if hint and hint in task_md:
            continue
        # Strategy 2: extract key symbols from the dropped line and
        # check if ANY appear in the task text near removal keywords.
        # This catches pyflakes-style "sys imported but unused" when
        # the dropped line is "import sys".
        if _deletion_mentioned_in_task(stripped, task_lower):
            continue
        dropped.append(line.rstrip())
    return dropped


def _deletion_mentioned_in_task(stripped_line: str, task_lower: str) -> bool:
    """Check whether *stripped_line* (a high-signal line) is plausibly
    mentioned in the task text as something to remove/fix.

    Handles cases like:
    - dropped: ``import sys``   task: ``'sys' imported but unused``
    - dropped: ``from foo import bar``   task: ``remove unused import bar``
    - dropped: ``def old_func(...)``   task: ``rename old_func to new_func``
    """
    if not task_lower:
        return False
    # For import lines, extract the imported module/symbol names
    if stripped_line.startswith(("import ", "from ")):
        # "import sys" → ["sys"]
        # "from os import path, sep" → ["path", "sep"]
        # "from os.path import join" → ["join"]
        parts = stripped_line.split()
        symbols: list[str] = []
        if parts[0] == "import":
            # import foo, bar, baz  OR  import foo.bar
            raw = " ".join(parts[1:])
            for chunk in raw.split(","):
                name = chunk.strip().split(" as ")[0].strip()
                # Use leaf name: "os.path" → "path"
                symbols.append(name.split(".")[-1])
        elif "import" in parts:
            idx = parts.index("import")
            raw = " ".join(parts[idx + 1:])
            for chunk in raw.split(","):
                name = chunk.strip().split(" as ")[0].strip()
                symbols.append(name.split(".")[-1])
        # Check if any symbol is mentioned in the task alongside
        # removal/unused language.
        removal_hints = ("unused", "remove", "delete", "drop", "clean",
                         "not used", "imported but", "never used",
                         "unnecessary", "unneeded", "redundant")
        for sym in symbols:
            if not sym or len(sym) < 2:
                continue
            sym_lower = sym.lower()
            if sym_lower in task_lower:
                # Symbol is mentioned — check if task also hints at removal
                if any(h in task_lower for h in removal_hints):
                    return True
    # For def/class lines, extract the name
    elif stripped_line.startswith(("def ", "class ")):
        # "def old_func(..." → "old_func"
        name = stripped_line.split("(")[0].split()[-1].rstrip(":")
        if name and len(name) >= 2 and name.lower() in task_lower:
            removal_hints = ("rename", "remove", "delete", "replace",
                             "refactor", "drop", "move")
            if any(h in task_lower for h in removal_hints):
                return True
    return False


def _safe_target(project_dir: Path, rel: str) -> Path | None:
    """Join *rel* under *project_dir* without escaping it.

    Uses logical path normalization (``os.path.normpath``) rather than
    ``Path.resolve()`` so that symlinked directories inside the project
    (e.g. ``api/cara -> ../commons/cara/cara``) are still treated as
    inside the project. Returns None if the path is absolute or uses
    ``..`` to escape upward.
    """
    if os.path.isabs(rel):
        return None
    rel_norm = os.path.normpath(rel)
    if rel_norm.startswith("..") or rel_norm == "..":
        return None
    if rel_norm.startswith(os.sep):
        return None
    return project_dir / rel_norm


# FIX 10: suspicion heuristic for orphan paths.
# When the applier is about to CREATE a new file, it's usually because
# the task asked for it. But sometimes a task file references a bad
# path (e.g. ``cara/cara/validation/rules/URLRule.py`` when ``api/cara``
# is already a symlink into ``commons/cara/cara``), and we end up
# creating ``.../cara/cara/cara/validation/rules/URLRule.py`` — an
# orphan nobody asked for. The heuristic: if the first-level parent of
# the target does NOT exist, AND there are no existing sibling files
# of the same extension anywhere in the deepest existing ancestor, the
# path is suspicious. Only applied to NEW files; explicitly-listed
# targets in ``task.files`` pass through unchallenged.
def _looks_like_orphan_path(
    project_dir: Path, rel: str, declared_files: set[str]
) -> bool:
    """Return True iff *rel* looks like a bad path that slipped through.

    - If the path was explicitly declared in the task's Files section,
      we trust it (returns False).
    - If every path component already exists on disk, we trust it
      (returns False) — Qwen is editing an existing file.
    - Otherwise, walk upward to find the deepest existing ancestor and
      check whether any sibling of the same extension exists under that
      ancestor. No siblings → very likely an orphan.
    """
    if rel in declared_files:
        return False
    target = project_dir / rel
    if target.exists():
        return False
    parts = Path(rel).parts
    # Walk up to the first ancestor that exists.
    ancestor = project_dir
    for i, part in enumerate(parts[:-1]):
        probe = ancestor / part
        if not probe.exists():
            # Everything below this point is being created fresh.
            break
        ancestor = probe
    else:
        # Whole directory chain exists — trust the path.
        return False
    # Does *any* file with the same suffix exist anywhere under ancestor?
    suffix = Path(rel).suffix
    if not suffix:
        return False
    try:
        for sibling in ancestor.rglob(f"*{suffix}"):
            if sibling.is_file():
                return False
    except OSError:
        return False
    return True


def _format_reminder_prompt(dropped_count: int) -> str:
    """Retry message when EDIT blocks were emitted but not parsed.

    Qwen occasionally falls back to aider-style conflict markers
    (``<<<<<<< SEARCH`` / ``=======`` / ``>>>>>>> REPLACE``) or omits
    required delimiters entirely. The parser handles both flavors, but
    when there's a mismatch we send this reminder so Qwen re-emits the
    dropped blocks in the canonical shape.
    """
    return (
        f"Your previous response contained {dropped_count} "
        f"<<<EDIT: ...>>> tag(s) that COULD NOT BE PARSED and were "
        f"silently dropped. This is a fatal protocol error — the file "
        f"changes were NOT applied.\n\n"
        "The ONLY accepted edit-block format is:\n\n"
        "<<<EDIT: path/to/file.py>>>\n"
        "<<<SEARCH>>>\n"
        "exact lines to find (byte-for-byte, including leading whitespace)\n"
        "<<<REPLACE>>>\n"
        "new lines to put there\n"
        "<<<END>>>\n\n"
        "Do NOT use aider-style conflict markers like `<<<<<<< SEARCH`, "
        "`=======`, or `>>>>>>> REPLACE`. Do NOT wrap blocks in markdown "
        "fences. Do NOT add prose between blocks. Each edit block must "
        "open with `<<<EDIT: <path>>>>`, contain exactly one `<<<SEARCH>>>` "
        "line followed by the literal search text, exactly one "
        "`<<<REPLACE>>>` line followed by the replacement text, and close "
        "with `<<<END>>>` on its own line.\n\n"
        "Re-emit the COMPLETE set of edits you intended — include the "
        "ones that parsed successfully AND the ones that were dropped. "
        "Do not assume any previous partial changes are still in place; "
        "each round starts from the current on-disk state, so your SEARCH "
        "text must match the file AS IT IS NOW."
    )


def _fuzzy_search_replace(
    content: str, search: str, replace: str
) -> Optional[str]:
    """Attempt a whitespace-tolerant SEARCH/REPLACE.

    When the exact SEARCH text doesn't match, we try two progressively
    looser strategies:
    1. Strip trailing whitespace from each line of both sides.
    2. Collapse all runs of whitespace to a single space (ignoring
       indentation structure) and find the matching region by line
       sequence.

    Returns the patched content on success, or None if no unique match.
    """
    # Strategy 1: strip trailing whitespace per line.
    def _rstrip_lines(text: str) -> str:
        return "\n".join(line.rstrip() for line in text.split("\n"))

    search_rs = _rstrip_lines(search)
    content_rs = _rstrip_lines(content)
    if content_rs.count(search_rs) == 1:
        # Find the exact byte range in the original content by locating the
        # match in the rstripped version and mapping line numbers back.
        idx = content_rs.index(search_rs)
        # Count newlines before the match to find the start line.
        start_line = content_rs[:idx].count("\n")
        search_line_count = search.count("\n") + 1
        lines = content.split("\n")
        before = "\n".join(lines[:start_line])
        after = "\n".join(lines[start_line + search_line_count:])
        parts = []
        if before:
            parts.append(before)
        parts.append(replace)
        if after:
            parts.append(after)
        return "\n".join(parts)

    # Strategy 2: match by stripped-and-collapsed lines. This handles the
    # case where Qwen changes 4-space indent to 2-space or vice versa.
    def _collapse(text: str) -> list[str]:
        return [" ".join(line.split()) for line in text.split("\n")]

    search_collapsed = _collapse(search)
    content_lines = content.split("\n")
    content_collapsed = _collapse(content)

    # Slide a window of len(search_collapsed) across content_collapsed.
    needle_len = len(search_collapsed)
    matches = []
    for i in range(len(content_collapsed) - needle_len + 1):
        if content_collapsed[i : i + needle_len] == search_collapsed:
            matches.append(i)
    if len(matches) == 1:
        i = matches[0]
        before = "\n".join(content_lines[:i])
        after = "\n".join(content_lines[i + needle_len:])
        parts = []
        if before:
            parts.append(before)
        parts.append(replace)
        if after:
            parts.append(after)
        return "\n".join(parts)

    return None


def _apply_edit_blocks(
    project_dir: Path,
    edits: list[tuple[str, str, str]],
) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """Apply SEARCH/REPLACE edit blocks (FIX 1b) in memory.

    Walks *edits* in order, reading each target file once and accumulating
    subsequent edits against the running in-memory content so multiple
    edits to the same file compose correctly.

    Returns ``(patched_contents, rejections)`` where ``patched_contents``
    maps ``rel_path -> new_content`` for every file that received at
    least one successful edit. Caller is responsible for feeding those
    contents into the same validation/silent-deletion/smoke-import
    pipeline as whole-file blocks.

    Rejection reasons:
      - "edit escapes project_dir"
      - "edit target missing: <rel>" (can't edit a file that doesn't exist)
      - "edit search text not found in <rel>"
      - "edit search text matched N times in <rel> — must be unique"
    """
    patched: dict[str, str] = {}
    rejections: list[tuple[str, str]] = []
    for rel, search, replace in edits:
        target = _safe_target(project_dir, rel)
        if target is None:
            rejections.append((rel, "edit escapes project_dir"))
            continue
        if rel in patched:
            current = patched[rel]
        else:
            if not (target.exists() and target.is_file()):
                rejections.append((rel, f"edit target missing: {rel}"))
                continue
            try:
                current = target.read_text(errors="replace")
            except OSError as e:
                rejections.append((rel, f"edit read error: {e}"))
                continue
        # Count matches to enforce uniqueness of SEARCH.
        count = current.count(search)
        if count == 0:
            # Fuzzy fallback: try whitespace-normalized matching.
            # Qwen often emits slightly different indentation or trailing
            # spaces. Normalize both sides and re-attempt.
            found_fuzzy = _fuzzy_search_replace(current, search, replace)
            if found_fuzzy is not None:
                patched[rel] = found_fuzzy
                continue
            rejections.append(
                (rel, f"edit search text not found in {rel}")
            )
            continue
        if count > 1:
            rejections.append(
                (
                    rel,
                    f"edit search text matched {count} times in {rel} — "
                    f"must be unique; add more context to SEARCH",
                )
            )
            continue
        patched[rel] = current.replace(search, replace, 1)
    return patched, rejections


def _apply_blocks(
    project_dir: Path,
    blocks: list[tuple[str, str]],
    deletes: list[str],
    task_md: str = "",
    edits: list[tuple[str, str, str]] | None = None,
    declared_files: set[str] | None = None,
) -> dict:
    """Apply whole-file blocks to disk.

    Returns a dict with:
        - written: list of relative paths successfully written
        - deleted: list of relative paths deleted
        - rejected: list of (path, reason) tuples — these become retry
          candidates in the runner's main loop
        - silent_drops: list of (path, list[str]) for files that passed
          validation but lost high-signal lines (imports, __all__, class
          headers) without the task asking. These are ALSO treated as
          rejections so the retry loop can re-prompt Qwen with the
          exact missing lines.
    """
    result: dict = {
        "written": [], "deleted": [], "rejected": [], "silent_drops": []
    }
    declared_set: set[str] = set(declared_files or ())

    # FIX 1b: apply SEARCH/REPLACE edits first, in memory, then fold the
    # resulting patched contents into the blocks list so they flow through
    # the same validation, silent-deletion guard, and smoke-import gates
    # as whole-file rewrites. Edits that couldn't match are surfaced as
    # rejections for the retry loop.
    if edits:
        patched, edit_rejections = _apply_edit_blocks(project_dir, edits)
        result["rejected"].extend(edit_rejections)
        # Merge patched contents into the blocks list. If an edit target
        # was ALSO sent as a whole-file block in the same round, the
        # whole-file block wins (Qwen presumably meant it); otherwise
        # the edit-patched content is added.
        existing_paths = {p for p, _ in blocks}
        for rel, content in patched.items():
            if rel not in existing_paths:
                blocks = blocks + [(rel, content)]

    for rel, content in blocks:
        target = _safe_target(project_dir, rel)
        if target is None:
            result["rejected"].append((rel, "escapes project_dir"))
            continue
        ok, reason = _validate(rel, content)
        if not ok:
            result["rejected"].append((rel, reason))
            continue

        # FIX 10: orphan-path guard. Only applied to NEW files (not in
        # declared task.files, not already on disk) that land in a
        # directory with zero siblings of the same type. Prevents the
        # cara/cara/cara/... double-nesting disaster.
        if _looks_like_orphan_path(project_dir, rel, declared_set):
            result["rejected"].append((
                rel,
                "orphan-path guard: target is not in task.Files and no "
                "sibling files of the same type exist near it — "
                "check the path; it may be double-nested or wrong",
            ))
            continue

        # Capture pre-edit content for silent-deletion detection.
        before = ""
        if target.exists() and target.is_file():
            try:
                before = target.read_text(errors="replace")
            except OSError:
                before = ""

        target.parent.mkdir(parents=True, exist_ok=True)
        if not content.endswith("\n"):
            content += "\n"

        # FIX 1a: silent-deletion guard — check BEFORE writing so we can
        # refuse to touch disk when Qwen is dropping lines.
        if before:
            dropped = _detect_silent_deletions(before, content, task_md)
            if dropped:
                result["silent_drops"].append((rel, dropped))
                reason = (
                    "silent-deletion guard: file lost high-signal lines "
                    "that the task did not ask to remove — "
                    + "; ".join(f"`{l.strip()}`" for l in dropped[:5])
                )
                result["rejected"].append((rel, reason))
                # Do NOT write the corrupted content. Leave `before` on disk.
                continue

        target.write_text(content)
        result["written"].append(rel)

        # FIX 2: smoke-import test for .py files.
        # Skip __init__.py to avoid cascading package import failures —
        # individual module imports will catch their own breakage, and
        # __init__.py often pulls in optional deps not present in the env.
        if rel.endswith(".py") and not rel.endswith("__init__.py"):
            ok, err = _smoke_import(project_dir, rel)
            if not ok:
                # Roll back: restore pre-edit content so retry round
                # sees the known-good baseline.
                if before:
                    target.write_text(before)
                else:
                    try:
                        target.unlink()
                    except OSError:
                        pass
                # Remove from written list
                if rel in result["written"]:
                    result["written"].remove(rel)
                result["rejected"].append(
                    (rel, f"smoke-import failed: {err[:200]}")
                )

    for rel in deletes:
        target = _safe_target(project_dir, rel)
        if target is None:
            result["rejected"].append((rel, "delete escapes project_dir"))
            continue
        if target.exists():
            target.unlink()
            result["deleted"].append(rel)
    return result


# ── OpenRouter HTTP call ────────────────────────────────────

def _cacheable_system_message(text: str) -> dict:
    """Build a system message whose content is marked cache-eligible.

    OpenRouter honours Anthropic-style ``cache_control: {type: ephemeral}``
    markers on content parts for providers that support prompt caching
    (Anthropic, DeepInfra for some Qwen checkpoints, Together's Qwen for
    others). Providers that don't support it silently ignore the field.
    Structuring the system message as a list-of-parts is the only safe
    way to attach the marker without breaking providers that expect a
    plain string — OpenRouter converts multi-part system content back to
    a string for the underlying API when caching is unsupported.
    """
    return {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": text,
                "cache_control": {"type": "ephemeral"},
            }
        ],
    }


def _call_openrouter(
    messages: list[dict],
    model: str,
    api_key: str,
    timeout: int = 600,
    max_tokens: int = 16384,
    temperature: float = 0.0,
) -> dict:
    """POST an OpenAI-style chat completion request to OpenRouter.

    *messages* is a full message list (system + user + any prior assistant
    turns), so the same helper serves first-shot and retry calls.

    FIX 10: asks OpenRouter to return detailed cache/cost usage via
    ``usage: {include: true}`` so the runner can log cache hits and real
    provider cost instead of the coarse defaults. Stable parts of the
    message list (the system prompt, REF blocks) should be marked with
    ``cache_control: {type: ephemeral}`` by the caller — see
    ``_cacheable_system_message``.
    """
    body = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        # Ask the provider to return detailed usage (cache hits, real cost).
        # OpenRouter drops unknown fields per-provider, so this is safe
        # even when the underlying provider doesn't support caching.
        "usage": {"include": True},
    }
    req = urllib.request.Request(
        OPENROUTER_URL,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/cfkarakulak/usta",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# ── Active subprocess tracking (for aider path only) ───────

_active_proc: Optional[subprocess.Popen] = None


def kill_active():
    """Kill the currently running aider subprocess (if any).

    No-op when the qwen-direct executor is active — that path has no
    subprocess, and in-flight urllib requests complete naturally on Ctrl+C.
    """
    global _active_proc
    if _active_proc is None:
        return
    if _active_proc.poll() is not None:
        _active_proc = None
        return
    try:
        _active_proc.terminate()
        _active_proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        try:
            _active_proc.kill()
        except OSError:
            pass
    _active_proc = None


# ── Public dispatcher ───────────────────────────────────────

def run_task(task: Task, cfg: Cfg) -> Task:
    """Dispatch to the configured executor. Updates *task* in place."""
    executor = (cfg.executor or "qwen").lower()
    if executor == "aider":
        return _run_aider(task, cfg)
    return _run_qwen(task, cfg)


# ── Qwen-direct executor (default) ──────────────────────────

# FIX 9: was 2 (one-shot + one retry). Bumped to 4 so that the applier
# can iterate through a realistic correction loop:
#   round 1: initial emit
#   round 2: validation/silent-deletion/smoke-import retry
#   round 3: parse-drift retry (Qwen used wrong marker flavor)
#   round 4: last-resort correction
# Override with USTA_QWEN_MAX_ROUNDS env var.
QWEN_MAX_ROUNDS = int(os.environ.get("USTA_QWEN_MAX_ROUNDS", "4"))


def _run_qwen(task: Task, cfg: Cfg) -> Task:
    """Run one task using Qwen3-Coder via OpenRouter chat completions.

    Multi-round verify loop:
    1. Send task + current file contents, get whole-file blocks.
    2. Parse, validate (AST/JSON), apply the clean ones.
    3. If any block was rejected AND we have rounds left, reprompt with the
       rejection reasons in the same conversation and apply the corrected
       blocks on top of the previously-written ones.
    4. After the final round, mark the task ``done`` if at least one file was
       written or deleted; otherwise ``fail``.

    Updates *task* in place and returns it.
    """
    task_path = _find_task(task, cfg)
    if not task_path:
        task.status = "fail"
        task.error = f"File not found: {task.filename}"
        return task

    applier = cfg.applier_cfg
    if applier.get("provider") != "openrouter":
        task.status = "fail"
        task.error = (
            f"applier must be openrouter, got {applier.get('provider')!r} — "
            f"set --model 'local/opus:openrouter/qwen/qwen3-coder'"
        )
        return task

    model = applier["model"]
    api_key = cfg.openrouter_api_key
    if not api_key:
        task.status = "fail"
        task.error = "OPENROUTER_API_KEY not set — export it in .zshrc or .env"
        return task

    model_short = model.split("/")[-1]
    task.status = "running"
    task.live_line = f"[{model_short}] building prompt..."
    t0 = time.time()

    log_dir = cfg.project_dir / ".usta" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    live_log = log_dir / f"{task.id}_{task.filename.replace('.md', '.log')}"
    log_lines: list[str] = []

    def _log(msg: str):
        log_lines.append(msg)
        try:
            live_log.write_text("\n".join(log_lines) + "\n")
        except OSError:
            pass

    try:
        task_md = task_path.read_text(errors="replace")
        prompt = _build_prompt(task_md, cfg.project_dir, task.files)
        _log("=== qwen-direct executor ===")
        _log(f"model: {model}")
        _log(f"task: {task.filename}")
        _log(f"files: {task.files}")
        _log(f"prompt_len: {len(prompt)}")

        messages: list[dict] = [
            _cacheable_system_message(QWEN_SYSTEM_PROMPT),
            {"role": "user", "content": prompt},
        ]

        total_in = 0
        total_out = 0
        total_cost = 0.0
        total_cached_in = 0  # FIX 10: prompt-cache hit bytes
        rounds_used = 0
        all_written: set[str] = set()
        all_deleted: set[str] = set()
        last_rejected: list[tuple[str, str]] = []

        for round_num in range(1, QWEN_MAX_ROUNDS + 1):
            rounds_used = round_num
            task.live_line = (
                f"[{model_short}] round {round_num}/{QWEN_MAX_ROUNDS}"
                f" — calling OpenRouter..."
            )
            _log(f"--- round {round_num}/{QWEN_MAX_ROUNDS} ---")
            # Temperature escalation: bump temp on retries to break
            # out of degenerate outputs (prose instead of file blocks).
            temp = 0.0 if round_num == 1 else min(0.4, 0.2 * (round_num - 1))
            resp = _call_openrouter(messages, model, api_key, temperature=temp)

            if "error" in resp:
                err = resp["error"]
                msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                _log(f"API error: {msg}")
                task.status = "fail"
                task.error = f"OpenRouter error: {msg[:200]}"
                task.duration = time.time() - t0
                return task

            choices = resp.get("choices") or []
            if not choices:
                _log(f"no choices in response: {json.dumps(resp)[:400]}")
                task.status = "fail"
                task.error = "no choices in API response"
                task.duration = time.time() - t0
                return task

            text = choices[0].get("message", {}).get("content", "") or ""
            finish = choices[0].get("finish_reason", "")
            usage = resp.get("usage", {}) or {}
            total_in += usage.get("prompt_tokens", 0)
            total_out += usage.get("completion_tokens", 0)
            total_cost += float(usage.get("cost", 0.0) or 0.0)

            # FIX 10: OpenRouter surfaces prompt-cache hits under one of
            # several fields depending on the underlying provider. Try
            # every known name and fall back to 0 so the log line always
            # has a value.
            prompt_details = usage.get("prompt_tokens_details") or {}
            cached_in = int(
                prompt_details.get("cached_tokens", 0)
                or usage.get("cache_read_input_tokens", 0)
                or usage.get("prompt_cache_hit_tokens", 0)
                or 0
            )
            total_cached_in += cached_in

            _log(f"round {round_num} response_len: {len(text)} finish={finish}")
            _log(
                f"round {round_num} usage: "
                f"in={usage.get('prompt_tokens',0)} "
                f"out={usage.get('completion_tokens',0)} "
                f"cached_in={cached_in} "
                f"cost=${float(usage.get('cost',0) or 0):.4f}"
            )
            _log("--- RAW RESPONSE ---")
            _log(text)
            _log("--- END RAW ---")

            messages.append({"role": "assistant", "content": text})

            if finish == "length":
                _log(f"round {round_num}: response truncated (max_tokens)")
                # Detect pathological repetition loops — Qwen sometimes
                # gets stuck emitting the same short marker forever
                # (e.g. ">>> REPLACE\n" 500 times) and burns the whole
                # token budget. If the last 2k chars of the response
                # contain fewer than 20 unique non-empty lines, retry
                # with a pointed reminder instead of failing outright.
                tail = text[-2000:]
                tail_lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
                unique_lines = set(tail_lines)
                repetition_loop = len(tail_lines) >= 30 and len(unique_lines) <= 5
                if repetition_loop and round_num < QWEN_MAX_ROUNDS:
                    _log(
                        f"round {round_num}: repetition loop detected "
                        f"({len(tail_lines)} tail lines, {len(unique_lines)} unique) — "
                        f"queuing reset-and-retry"
                    )
                    # Strip the garbage assistant turn so we don't feed
                    # 16k tokens of `>>> REPLACE` back into the next
                    # round.
                    if messages and messages[-1].get("role") == "assistant":
                        messages.pop()
                    messages.append({
                        "role": "user",
                        "content": (
                            "Your previous response got stuck in a repetition loop "
                            "(same marker emitted many times). Start over. "
                            "Produce a SINGLE clean block per file change using either "
                            "`<<<FILE: path>>>...<<<END>>>` for whole-file rewrites "
                            "or `<<<EDIT: path>>> <<<SEARCH>>>...<<<REPLACE>>>...<<<END>>>` "
                            "for surgical edits. Do NOT nest or repeat markers."
                        ),
                    })
                    continue
                task.status = "fail"
                task.error = "response truncated (max_tokens reached) — task too large"
                task.duration = time.time() - t0
                task.tokens = total_in + total_out
                task.cost = total_cost
                task.messages = rounds_used
                return task

            task.live_line = f"[{model_short}] parsing blocks..."
            blocks, deletes, edits, edit_mismatch = _parse_blocks(text)
            _log(
                f"round {round_num}: {len(blocks)} file blocks, "
                f"{len(deletes)} deletes, {len(edits)} edits, "
                f"edit_tag_mismatch={edit_mismatch}"
            )

            if not blocks and not deletes and not edits:
                # Case 1: unparseable EDIT tags — retry with format reminder.
                if edit_mismatch > 0 and round_num < QWEN_MAX_ROUNDS:
                    _log(
                        f"round {round_num}: {edit_mismatch} EDIT tag(s) "
                        f"present but none parsed — forcing format retry"
                    )
                    messages.append({
                        "role": "user",
                        "content": _format_reminder_prompt(edit_mismatch),
                    })
                    continue
                # Case 2: model said "no changes needed" — treat as success.
                _no_change_phrases = (
                    "already been implemented",
                    "already been created",
                    "already exists",
                    "no further modifications",
                    "no changes are needed",
                    "no changes needed",
                    "no modifications needed",
                    "already implemented",
                    "already in place",
                    "nothing to change",
                    "all the required changes have already",
                )
                lower_text = text.lower()
                if any(p in lower_text for p in _no_change_phrases):
                    _log(f"round {round_num}: model says no changes needed — treating as noop pass")
                    task.status = "pass"
                    task.error = None
                    task.duration = time.time() - t0
                    task.tokens = total_in + total_out
                    task.cost = total_cost
                    task.messages = rounds_used
                    return task
                # Case 3: model ignored format entirely — retry once with
                # a hard nudge before giving up.
                if round_num < QWEN_MAX_ROUNDS:
                    _log(
                        f"round {round_num}: no blocks at all — "
                        f"retrying with format enforcement"
                    )
                    messages.append({
                        "role": "user",
                        "content": (
                            "Your response contained NO <<<FILE: ...>>> or "
                            "<<<EDIT: ...>>> blocks. The orchestrator could "
                            "not apply any changes.\n\n"
                            "You MUST respond with file blocks — nothing "
                            "else. Re-read the task and emit the changes "
                            "using the exact format from the system prompt. "
                            "Do NOT add prose or explanations."
                        ),
                    })
                    continue
                task.status = "fail"
                task.error = "no file blocks in response — model ignored format"
                task.duration = time.time() - t0
                task.tokens = total_in + total_out
                task.cost = total_cost
                task.messages = rounds_used
                return task

            task.live_line = (
                f"[{model_short}] applying {len(blocks)} file(s)"
                + (f" + {len(edits)} edit(s)" if edits else "")
                + "..."
            )
            result = _apply_blocks(
                cfg.project_dir, blocks, deletes,
                task_md=task_md, edits=edits,
                declared_files=set(task.files or ()),
            )
            _log(
                f"round {round_num} applied: written={result['written']} "
                f"deleted={result['deleted']} rejected={result['rejected']}"
            )
            if result["silent_drops"]:
                _log(f"round {round_num} silent_drops: {result['silent_drops']}")
            for p in result["written"]:
                all_written.add(p)
            for p in result["deleted"]:
                all_deleted.add(p)
            last_rejected = list(result["rejected"])
            silent_drops = list(result["silent_drops"])

            # FIX 9: parse-drift detection. If Qwen emitted N EDIT tags
            # but only M < N parsed, the dropped ones are a silent
            # protocol failure. Force a retry with a format reminder
            # even if everything that parsed applied cleanly.
            if edit_mismatch > 0 and round_num < QWEN_MAX_ROUNDS:
                _log(
                    f"round {round_num}: {edit_mismatch} EDIT tag(s) "
                    f"dropped by parser — queuing format-reminder retry"
                )
                messages.append({
                    "role": "user",
                    "content": _format_reminder_prompt(edit_mismatch),
                })
                continue

            if not last_rejected:
                break  # success — all emitted blocks applied

            if round_num >= QWEN_MAX_ROUNDS:
                break  # out of retries; take what we have

            # Build a retry prompt asking Qwen to fix ONLY the rejected files.
            reject_lines = "\n".join(
                f"- `{p}`: {reason}" for p, reason in last_rejected
            )

            # FIX 1a: if the rejection came from the silent-deletion guard,
            # include the exact dropped lines in the retry prompt so Qwen
            # knows what to put back. Also re-load the current (already-
            # written) file contents so Qwen can see what it just did wrong.
            drop_note = ""
            if silent_drops:
                drop_blocks = []
                for rel, lines in silent_drops:
                    joined = "\n".join(f"    {l}" for l in lines)
                    drop_blocks.append(
                        f"In `{rel}`, your rewrite SILENTLY REMOVED these "
                        f"lines that were present before:\n{joined}"
                    )
                drop_note = (
                    "\n\n**CRITICAL — silent-deletion detected.** "
                    "You are rewriting existing files and losing lines "
                    "that the task did not ask you to remove. "
                    "This is a HALLUCINATION — do not let it happen. "
                    "When copying unchanged parts of a file, copy EVERY "
                    "import, EVERY `__all__` entry, EVERY class header, "
                    "EVERY decorator. Do not paraphrase. Do not shorten.\n\n"
                    + "\n\n".join(drop_blocks)
                    + "\n\nRe-emit the COMPLETE corrected file with every "
                    "line restored. Do not drop anything again."
                )

            retry_user = (
                "Your previous output contained invalid content for these "
                "files and they were REJECTED:\n\n"
                f"{reject_lines}\n"
                f"{drop_note}\n\n"
                "Re-emit ONLY corrected <<<FILE: ...>>> blocks for the "
                "rejected files above. Do not touch any other file. Do not "
                "wrap the output in markdown fences or add prose. Ensure "
                ".py files parse as valid Python and .json files parse as "
                "valid JSON."
            )
            messages.append({"role": "user", "content": retry_user})
            _log("queued retry prompt for rejected files")

        task.duration = time.time() - t0
        task.tokens = total_in + total_out
        task.cost = total_cost
        task.messages = rounds_used

        if not all_written and not all_deleted:
            reasons = "; ".join(f"{r}" for _, r in last_rejected) or "no files applied"
            task.status = "fail"
            task.error = f"all blocks rejected after {rounds_used} round(s): {reasons[:300]}"
            return task

        task.status = "done"
        if last_rejected:
            task.error = (
                f"warning: {len(last_rejected)} block(s) still rejected "
                f"after {rounds_used} round(s), but {len(all_written)} "
                f"file(s) written"
            )

    except urllib.error.HTTPError as e:
        task.duration = time.time() - t0
        body_preview = ""
        try:
            body_preview = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        _log(f"HTTPError {e.code}: {body_preview}")
        task.status = "fail"
        task.error = f"HTTP {e.code}: {body_preview[:200]}"
    except urllib.error.URLError as e:
        task.duration = time.time() - t0
        _log(f"URLError: {e}")
        task.status = "fail"
        task.error = f"network: {e}"
    except Exception as e:
        task.duration = time.time() - t0
        _log(f"unexpected: {type(e).__name__}: {e}")
        task.status = "fail"
        task.error = f"{type(e).__name__}: {e}"

    return task


# ── Aider executor (opt-in: --executor aider) ───────────────
#
# Watchdog constants. These are the ONLY retry/kill heuristics in runner.py.
# They exist solely to stop the aider subprocess from hanging forever when
# the underlying model gets stuck in edit-format loops or streams junk.

AIDER_STALE_TIMEOUT = 600           # kill if no stdout for 10 min
AIDER_API_SILENCE = 900             # kill if no completed API round-trip for 15 min
AIDER_MAX_DURATION_BASE = 600       # 10 min minimum
AIDER_MAX_DURATION_PER_FILE = 300   # +5 min per listed file
AIDER_MAX_DURATION_CAP = 1800       # 30 min absolute max
AIDER_MAX_FORMAT_FAILURES = 3       # give up after N "did not conform to edit format"
AIDER_MAX_ERROR_LOOPS = 5           # give up after N identical errors in a row


def _parse_tok(s: str) -> int:
    """Parse '12.3k' or '12,345' into an int token count."""
    s = s.replace(",", "").lower()
    if s.endswith("k"):
        try:
            return int(float(s[:-1]) * 1000)
        except ValueError:
            return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _run_aider(task: Task, cfg: Cfg) -> Task:
    """Run one task via the aider CLI.

    Aider writes search/replace patches against the listed files. We spawn it,
    watchdog the subprocess, parse the cost/token summary from its output, and
    return the updated task. No fallback — if aider fails, the task is failed.
    """
    global _active_proc

    task_path = _find_task(task, cfg)
    if not task_path:
        task.status = "fail"
        task.error = f"File not found: {task.filename}"
        return task

    if not cfg.aider_bin:
        task.status = "fail"
        task.error = "aider binary not found — install with `pip install aider-chat`"
        return task

    cmd = [
        cfg.aider_bin,
        "--read", str(task_path),
        "--message-file", str(task_path),
    ]
    for f in task.files:
        if (cfg.project_dir / f).exists():
            cmd.append(f)

    # Model. Required; aider would otherwise pick its default (expensive).
    cmd.extend(["--model", cfg.aider_model])

    # Always accept prompts.
    cmd.append("--yes")

    # Small repo map so DeepSeek-class models don't drown in context.
    cmd.extend(["--map-tokens", "1024"])

    # Quiet everything: no browser, no warnings, no analytics, no prompts.
    cmd.extend([
        "--no-show-model-warnings",
        "--no-check-update",
        "--no-analytics",
        "--no-detect-urls",
        "--no-suggest-shell-commands",
        "--no-pretty",
    ])

    env = os.environ.copy()
    if "OPENROUTER_API_KEY" not in env:
        env["OPENROUTER_API_KEY"] = cfg.openrouter_api_key or ""
    env["BROWSER"] = ""
    env["AIDER_ANALYTICS"] = "false"
    env["AIDER_CHECK_UPDATE"] = "false"
    # Pin aider's state dir under .usta/ so we don't pollute ~/.aider
    env["AIDER_HOME"] = str(cfg.project_dir / ".usta")

    model_short = cfg.aider_model.split("/")[-1]
    task.status = "running"
    task.live_line = f"[aider:{model_short}] starting..."
    t0 = time.time()

    # Dynamic max duration based on file count.
    n_files = max(len(task.files), 1)
    max_duration = min(
        AIDER_MAX_DURATION_BASE + AIDER_MAX_DURATION_PER_FILE * n_files,
        AIDER_MAX_DURATION_CAP,
    )

    log_dir = cfg.project_dir / ".usta" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    live_log = log_dir / f"{task.id}_{task.filename.replace('.md', '.log')}"

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(cfg.project_dir),
            env=env,
            stdin=subprocess.DEVNULL,
        )
        _active_proc = proc

        lines: list[str] = []
        last_output_time = time.time()
        last_api_time = time.time()
        kill_reason = ""

        def _watchdog():
            nonlocal kill_reason
            while proc.poll() is None:
                now = time.time()
                if now - last_output_time > AIDER_STALE_TIMEOUT:
                    kill_reason = f"no output for {AIDER_STALE_TIMEOUT}s"
                    break
                if now - t0 > max_duration:
                    kill_reason = f"exceeded max duration {max_duration:.0f}s"
                    break
                if now - last_api_time > AIDER_API_SILENCE:
                    kill_reason = f"no API response for {AIDER_API_SILENCE}s"
                    break
                time.sleep(5)
            if kill_reason:
                try:
                    proc.kill()
                except OSError:
                    pass

        threading.Thread(target=_watchdog, daemon=True).start()

        live_fh = open(live_log, "w", errors="replace")
        recent_errors: list[str] = []
        format_failures = 0

        for line in proc.stdout:
            last_output_time = time.time()
            lines.append(line)
            live_fh.write(line)
            live_fh.flush()

            stripped = line.strip()
            if stripped and not stripped.startswith("─"):
                task.live_line = stripped[:80]

            if stripped.startswith("Tokens:"):
                last_api_time = time.time()

            if (
                "did not conform to the edit format" in stripped
                or "LLM did not conform" in stripped
            ):
                format_failures += 1
                task.live_line = f"Format fail #{format_failures}/{AIDER_MAX_FORMAT_FAILURES}"
                if format_failures >= AIDER_MAX_FORMAT_FAILURES:
                    kill_reason = (
                        f"LLM failed edit format {format_failures}x — "
                        f"model can't handle this task"
                    )
                    try:
                        proc.kill()
                    except OSError:
                        pass
                    break

            low = stripped.lower()
            if "error" in low or "can't find" in low or "no such file" in low:
                recent_errors.append(stripped[:100])
                if len(recent_errors) > 20:
                    recent_errors = recent_errors[-20:]
                if len(recent_errors) >= AIDER_MAX_ERROR_LOOPS:
                    last_n = recent_errors[-AIDER_MAX_ERROR_LOOPS:]
                    if len(set(last_n)) == 1:
                        kill_reason = f"stuck in error loop: {last_n[0][:60]}"
                        try:
                            proc.kill()
                        except OSError:
                            pass
                        break

        proc.wait()
        live_fh.close()
        task.duration = time.time() - t0
        full = "".join(lines)

        if kill_reason:
            task.status = "fail"
            task.error = f"Killed — {kill_reason}"
            try:
                with open(live_log, "a") as f:
                    f.write(f"\n\n--- KILLED: {kill_reason} ---\n")
            except OSError:
                pass
            _active_proc = None
            return task

        # Parse cost/tokens from aider's summary lines.
        cost_matches = re.findall(r"Cost:.*?\$([0-9.]+)\s+session", full)
        if cost_matches:
            task.cost = float(cost_matches[-1])
        else:
            m = re.search(r"Cost:\s*\$([0-9.]+)", full)
            if m:
                task.cost = float(m.group(1))
        tok_matches = re.findall(r"(\d[\d,.]*k?)\s+sent", full)
        if tok_matches:
            task.tokens = _parse_tok(tok_matches[-1])
        task.messages = len(re.findall(r"Tokens:", full))

        task.status = "done" if proc.returncode == 0 else "fail"
        if proc.returncode != 0:
            task.error = f"exit {proc.returncode}"

    except FileNotFoundError:
        task.duration = time.time() - t0
        task.status = "fail"
        task.error = f"aider not found: {cfg.aider_bin}"
    except Exception as e:
        task.duration = time.time() - t0
        task.status = "fail"
        task.error = f"{type(e).__name__}: {e}"
    finally:
        _active_proc = None

    return task
