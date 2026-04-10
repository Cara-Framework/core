from __future__ import annotations
"""Planner — Opus generates task MDs from a high-level objective.

Session-aware: first plan explores the codebase, subsequent plans
resume the same Claude session (zero re-exploration, cached context).
"""

import json
import re
from pathlib import Path
from typing import Optional, Callable

from .config import Cfg
from .claude import ask as ask_claude, ClaudeResult
from .tasks import Task, next_id, _bump_id
from .warm import build_warm_context

def _ask(prompt, cfg, **kwargs):
    """Dispatch to claude CLI or OpenRouter based on planner provider."""
    if cfg.planner_cfg.get("provider") == "openrouter":
        from .openrouter import ask_openrouter
        model = cfg.planner_cfg.get("model")
        return ask_openrouter(prompt, cfg, model=model,
                              system_prompt=kwargs.get("system_prompt"),
                              on_event=kwargs.get("on_event"))
    return ask_claude(prompt, cfg, **kwargs)


# Max retries when Opus explores but doesn't output JSON
MAX_CONTINUE_RETRIES = 2

# ── System prompts ────────────────────────────────────────

EXECUTOR_HINT_QWEN = (
    "The downstream applier is Qwen3-Coder via OpenRouter chat completions. "
    "It writes WHOLE-FILE blocks, so every file you list in ## Files will be "
    "fully regenerated. Name only files that actually change."
)
EXECUTOR_HINT_AIDER = (
    "The downstream applier is aider (search/replace patches). It edits files "
    "in place, so list every file the change should touch — aider will make "
    "surgical diffs, not rewrites. Keep tasks focused on one concern."
)


SYSTEM_FIRST = """You are an expert software architect planning tasks for a downstream code-applier (Qwen3-Coder via OpenRouter chat completions).

This is your FIRST look at this project. A **warm context block** has
already been prepared locally and will appear in the user message below —
it contains:
- a full project map (source files + byte sizes, vendor dirs excluded),
- contents of key entry-point/config files (pyproject.toml, package.json,
  main.py, etc.),
- a shortlist of files whose contents matched keywords from the objective.

**Read the warm context first.** Only run `find`/`ls`/`cat` if something
you genuinely need is still missing — in most cases you will not need to
explore at all and can go straight to the JSON output.

CRITICAL: Do NOT over-explore. You have limited turns. Prefer the warm
context. Read only the small number of additional files you truly need,
then IMMEDIATELY generate the JSON task array. Save turns for the output.
After exploring (if needed), generate task files for the given objective.

Task file rules:
- Each task = one Markdown file. One concern per task.
- Include ## Goal, ## Steps (detailed), ## Files (to modify), ## Reference Files (read-only context)
- Number IDs sequentially. Order by dependency.
- Bundle related small changes into one task. Group by file or concern.
- You ARE the architect — write explicit, unambiguous specs. The applier writes whole-file blocks, so every file you list will be regenerated in full.
- Create as many or as few tasks as the objective truly needs. Do NOT pad to a round number. A 3-file fix might need 1 task. A full refactor might need 15. Be precise.
- ABSOLUTE HARD LIMIT: Each task MUST have at most 3-4 files in ## Files. NEVER exceed 4. If a change touches 8 files, you MUST split into 2-3 separate tasks of 3-4 files each. Large file counts per task reduce quality sharply. This is non-negotiable.
- Prefer 2-3 files per task. 4 is the absolute max. 1-2 is ideal.

**NEVER invent concrete output values in spec examples, doctests, or
"expected" assertions.** You will get the arithmetic wrong and the
downstream applier will implement the correct math, then the reviewer
will (correctly) flag the spec as a bug. Instead, describe expected
behavior in prose: "returns a string of the form 'Nd Nh Nm Ns' with
zero components omitted, ordered from largest to smallest unit." Let
Qwen compute the actual numbers when it writes the tests. If you
absolutely must include a concrete example, use inputs whose output
you can verify trivially by eye (e.g. 60 → '1m', 3600 → '1h', never
arbitrary six-digit numbers).

IMPORTANT: You MUST end your response with a valid JSON array. No markdown fences. No text after the JSON.
[{"id":"01","title":"Short title","filename":"01_short_name.md","content":"full markdown content","files":["path/to/File.py"],"depends_on":[]}]
"""
SYSTEM_CONTINUE = """You already know this project from our previous conversation. Do NOT re-explore the codebase.

Generate task files for the new objective below. Use your existing knowledge of the project structure and code patterns.

If you need to check a specific file for the new objective, that's fine (1-2 reads max). But do NOT do a full project scan again.

Task file rules:
- Each task = one Markdown file. One concern per task.
- Include ## Goal, ## Steps (detailed), ## Files (to modify), ## Reference Files (read-only context)
- Number IDs sequentially. Order by dependency.
- Bundle related small changes into one task.
- You ARE the architect — write explicit, unambiguous specs. The applier writes whole-file blocks.
- Create as many or as few tasks as the objective truly needs. Do NOT pad to a round number.
- ABSOLUTE HARD LIMIT: Each task MUST have at most 3-4 files in ## Files. NEVER exceed 4. Split larger changes into multiple tasks. Prefer 2-3 files per task.

**NEVER invent concrete output values in spec examples, doctests, or
"expected" assertions.** Describe expected behavior in prose. If you
must include a concrete example, use trivially-verifiable inputs
(60 → '1m', 3600 → '1h'). The applier computes the real math.

IMPORTANT: You MUST end your response with a valid JSON array. No markdown fences. No text after the JSON.
[{"id":"01","title":"Short title","filename":"01_short_name.md","content":"full markdown content","files":["path/to/File.py"],"depends_on":[]}]
"""
# Continuation prompt — when Opus explored but ran out of turns before JSON
CONTINUE_PROMPT = (
    "You explored the codebase but did not output the JSON task array yet. "
    "Now output ONLY the JSON array. No explanation, no markdown fences, just the raw JSON.\n"
    '[{"id":"01","title":"...","filename":"01_xxx.md","content":"...","files":[...],"depends_on":[]}]'
)

# ── File detection ────────────────────────────────────────

_REF_PATTERNS = re.compile(
    r'\b([A-Z_]+\.(?:md|txt|json|yaml|yml|toml))\b', re.IGNORECASE)

_SOURCE_PATH_PATTERN = re.compile(
    r'[`*]*([a-zA-Z_][\w/]*\.(?:py|js|ts|php|rb|go|rs|java))[`*]*')


def _auto_detect_files(objective: str, project_dir: Path) -> list[Path]:
    """Find doc files referenced in the objective."""
    found = []
    for match in _REF_PATTERNS.finditer(objective):
        fname = match.group(1)
        for candidate in [project_dir / fname, project_dir / ".usta" / fname]:
            if candidate.exists() and candidate not in found:
                found.append(candidate)
    return found


def _extract_source_paths(text: str, project_dir: Path) -> list[Path]:
    """Extract source file paths mentioned in a doc file."""
    seen = set()
    found = []
    for match in _SOURCE_PATH_PATTERN.finditer(text):
        fpath = match.group(1)
        if fpath in seen:
            continue
        seen.add(fpath)
        candidate = project_dir / fpath
        if candidate.exists() and candidate not in found:
            found.append(candidate)
    return found

# ── Plan ──────────────────────────────────────────────────

def plan(
    objective: str,
    cfg: Cfg,
    context_files: Optional[list[Path]] = None,
    on_event: Optional[Callable] = None,
) -> tuple[list[Task], Optional[ClaudeResult], bool]:
    """Plan tasks. Returns (tasks, claude_result, is_new_session).

    Session logic:
    - If no saved session → new session, full codebase exploration
    - If saved session exists → resume, skip exploration

    Auto-continue: if Opus explores but doesn't output JSON,
    we automatically resume the session asking for just the JSON.
    """
    existing_session = cfg.get_session_id()
    is_new = existing_session is None

    # ── Build prompt ──────────────────────────────────────
    parts = [f"## Objective\n{objective}\n"]

    # Inject project conventions so the planner generates convention-aware tasks.
    for conv_name in (".usta/CONVENTIONS.md", "CONVENTIONS.md", "CODING_RULES.md"):
        conv_path = cfg.project_dir / conv_name
        if conv_path.exists() and conv_path.is_file():
            try:
                conv_text = conv_path.read_text(errors="replace").strip()
                if conv_text:
                    parts.append(
                        f"## Project Rules (follow strictly)\n{conv_text}\n"
                    )
                    break
            except OSError:
                continue

    # FIX 5: inject persistent lessons from previous review cycles so
    # the planner stops repeating the same class of mistakes. Zero-cost
    # when .usta/lessons.jsonl is empty.
    try:
        from .lessons import load_lessons, format_for_prompt
        _lessons = load_lessons(cfg.project_dir)
        _lesson_block = format_for_prompt(_lessons)
        if _lesson_block:
            parts.append(_lesson_block)
    except Exception:
        pass

    if is_new:
        # Warm context: local codebase snapshot injected BEFORE Opus explores.
        try:
            warm = build_warm_context(objective, cfg.project_dir)
        except Exception:
            warm = ""
        if warm:
            parts.append(warm)

        # Auto-detect and include referenced files
        auto_files = _auto_detect_files(objective, cfg.project_dir)
        all_context = list(auto_files)
        if context_files:
            for f in context_files:
                if f not in all_context:
                    all_context.append(f)

        source_files = []
        for f in all_context:
            if f.exists():
                content = f.read_text(errors='replace')
                parts.append(f"## {f.name}\n```\n{content[:12000]}\n```\n")
                source_files.extend(_extract_source_paths(content, cfg.project_dir))

        # Include source files mentioned in docs (saves Opus from reading them)
        seen = set()
        for sf in source_files:
            if sf in seen:
                continue
            seen.add(sf)
            try:
                src = sf.read_text(errors='replace')
                rel = sf.relative_to(cfg.project_dir)
                parts.append(f"## {rel}\n```python\n{src[:6000]}\n```\n")
            except (ValueError, OSError):
                pass

        hint = EXECUTOR_HINT_AIDER if cfg.executor == "aider" else EXECUTOR_HINT_QWEN
        parts.append(
            f"\n## Applier characteristics\n{hint}\n\n"
            "The warm context above already shows the project layout and "
            "the most likely-relevant files. Use it first, explore only "
            "if necessary, then generate tasks for the objective. "
            "Return ONLY the JSON array."
        )
        system = SYSTEM_FIRST
    else:
        # Continuing: minimal prompt, just the objective + any referenced files
        auto_files = _auto_detect_files(objective, cfg.project_dir)
        if context_files:
            auto_files.extend(f for f in context_files if f not in auto_files)

        for f in auto_files:
            if f.exists():
                content = f.read_text(errors='replace')
                parts.append(f"## {f.name}\n```\n{content[:12000]}\n```\n")

        hint = EXECUTOR_HINT_AIDER if cfg.executor == "aider" else EXECUTOR_HINT_QWEN
        parts.append(
            f"\n## Applier characteristics\n{hint}\n\n"
            "You already know this project. Generate tasks immediately. "
            "Return ONLY the JSON array."
        )
        system = SYSTEM_CONTINUE

    # ── Call Claude ────────────────────────────────────────
    if is_new:
        session_id = cfg.new_session_id()
        resp = _ask(
            "\n".join(parts), cfg,
            system_prompt=system,
            on_event=on_event,
            max_turns=30,
            session_id=session_id,
            resume=False,
        )
    else:
        resp = _ask(
            "\n".join(parts), cfg,
            system_prompt=system,
            on_event=on_event,
            max_turns=10,
            session_id=existing_session,
            resume=True,
        )
    if not resp.ok:
        # If resume failed (session expired etc), retry with new session
        if not is_new and ("session" in (resp.error or "").lower() or "resume" in (resp.error or "").lower()):
            cfg.clear_session()
            return plan(objective, cfg, context_files, on_event)
        return [], resp, is_new

    # Save session for next time (in case it was assigned a new one)
    if resp.session_id:
        cfg.save_session_id(resp.session_id)

    raw_tasks = _parse(resp.text)

    # ── Auto-continue: if Opus explored but didn't output JSON ──
    if not raw_tasks and resp.text and resp.session_id:
        for attempt in range(MAX_CONTINUE_RETRIES):
            if on_event:
                on_event("text", {"text": f"No JSON found, continuing session (attempt {attempt + 1}/{MAX_CONTINUE_RETRIES})..."})
            resp2 = _ask(
                CONTINUE_PROMPT, cfg,
                on_event=on_event,
                max_turns=5,
                session_id=resp.session_id,
                resume=True,
            )
            if resp2.ok and resp2.text:
                raw_tasks = _parse(resp2.text)
                if raw_tasks:
                    # Update usage from continuation
                    resp = resp2
                    break
    if not raw_tasks:
        return [], resp, is_new

    # FIX 9: verify any concrete examples the planner put in the spec
    # BEFORE the applier starts. Doctests and arrow samples get
    # executed in a sandboxed namespace and mismatches are surfaced to
    # the caller via on_event so the CLI can show them. A single
    # auto-retry re-prompts Opus to fix the spec when we find bugs.
    raw_tasks = _verify_and_retry_specs(
        raw_tasks, cfg, resp, on_event, is_new,
    )

    # Deduplicate tasks with identical or near-identical scope
    raw_tasks = _dedup_tasks(raw_tasks)

    # Renumber with globally incremental IDs
    start = next_id(cfg)
    tasks = []
    for i, t in enumerate(raw_tasks):
        new_id = f"{start + i:02d}"
        old_name_parts = t.filename.split("_", 1)
        name_suffix = old_name_parts[1] if len(old_name_parts) > 1 else f"task_{i+1}.md"
        new_filename = f"{new_id}_{name_suffix}"

        tasks.append(Task(
            id=new_id, title=t.title,
            filename=new_filename,
            content=t.content, files=t.files,
            depends_on=t.depends_on,
        ))
    _bump_id(cfg, start + len(raw_tasks) - 1)
    return tasks, resp, is_new


# ── Task deduplication ────────────────────────────────────

def _dedup_tasks(tasks: list[Task]) -> list[Task]:
    """Remove duplicate tasks based on title similarity and file overlap.

    Two tasks are considered duplicates when:
    - Their titles are identical after normalisation, OR
    - They share ≥80% of their files AND their titles share ≥2 keywords

    Keeps the first occurrence (which tends to be the more detailed spec).
    """
    if len(tasks) <= 1:
        return tasks

    def _norm_title(t: str) -> str:
        return " ".join(t.lower().split())

    def _title_keywords(t: str) -> set[str]:
        stop = {"a", "an", "the", "and", "or", "for", "to", "in", "of",
                "add", "create", "implement", "update", "fix", "with"}
        return {w for w in t.lower().split() if w not in stop and len(w) > 1}

    keep: list[Task] = []
    seen_titles: set[str] = set()

    for t in tasks:
        norm = _norm_title(t.title)

        # Exact title match
        if norm in seen_titles:
            continue

        # Check against already-kept tasks for near-duplicate
        is_dup = False
        t_files = set(t.files or [])
        t_kw = _title_keywords(t.title)
        for kept in keep:
            k_files = set(kept.files or [])
            # File overlap check
            if t_files and k_files:
                overlap = len(t_files & k_files)
                total = max(len(t_files), len(k_files))
                if total > 0 and overlap / total >= 0.8:
                    # High file overlap — check title keyword overlap
                    k_kw = _title_keywords(kept.title)
                    if len(t_kw & k_kw) >= 2:
                        is_dup = True
                        break

        if not is_dup:
            keep.append(t)
            seen_titles.add(norm)

    return keep


# ── FIX 9: spec self-verification ─────────────────────────

_SPEC_FIX_PROMPT = (
    "The tasks you just emitted contain concrete examples that are "
    "arithmetically wrong. Re-emit the ENTIRE JSON array exactly once "
    "with the buggy examples fixed — either correct the numbers or "
    "replace them with prose descriptions of the expected behaviour. "
    "Do not add new tasks, do not drop tasks, keep all IDs and files "
    "identical. Return ONLY the JSON array, no markdown fences.\n\n"
    "Here are the problems the spec checker found:\n"
)


def _verify_and_retry_specs(
    raw_tasks: list[Task],
    cfg: Cfg,
    resp,
    on_event,
    is_new: bool,
) -> list[Task]:
    """Run :func:`spec_check.check_spec` on each task and, if any issues
    turn up, try exactly one Opus retry to regenerate the affected
    tasks. Returns the (possibly updated) list.
    """
    try:
        from .spec_check import check_spec
    except Exception:
        return raw_tasks

    problems: list[tuple[int, str]] = []
    for idx, t in enumerate(raw_tasks):
        try:
            res = check_spec(t.content or "")
        except Exception:
            continue
        if res.issues:
            bullets = "\n".join(
                f"    * [{iss.kind}] {iss.excerpt}"
                + (f" — got {iss.got}, spec says {iss.expected}"
                   if iss.got or iss.expected else "")
                + (f" ({iss.reason})" if iss.reason else "")
                for iss in res.issues
            )
            problems.append((idx, f"- Task {t.id} ({t.title}):\n{bullets}"))
            if on_event:
                on_event("text", {
                    "text": f"  spec-check: {t.id} — {res.summary()}",
                })

    if not problems or not resp or not resp.session_id:
        return raw_tasks

    retry_prompt = _SPEC_FIX_PROMPT + "\n".join(p[1] for p in problems)

    if on_event:
        on_event("text", {
            "text": (
                f"  planner: re-prompting Opus to fix "
                f"{len(problems)} buggy spec(s)"
            ),
        })
    try:
        resp2 = _ask(
            retry_prompt, cfg,
            on_event=on_event,
            max_turns=3,
            session_id=resp.session_id,
            resume=True,
        )
    except Exception:
        return raw_tasks
    if not (resp2 and resp2.ok and resp2.text):
        return raw_tasks

    new_raw = _parse(resp2.text)
    if not new_raw:
        return raw_tasks

    # Only replace tasks whose original IDs were problematic; keep the
    # rest from the first pass so we never shuffle things the retry
    # didn't mention.
    bad_ids = {raw_tasks[idx].id for idx, _ in problems}
    id_to_new = {t.id: t for t in new_raw}
    out: list[Task] = []
    for t in raw_tasks:
        if t.id in bad_ids and t.id in id_to_new:
            out.append(id_to_new[t.id])
        else:
            out.append(t)
    return out

def _parse(text: str) -> list[Task]:
    m = re.search(r'\[.*\]', text, re.DOTALL)
    if not m:
        return []
    try:
        data = json.loads(m.group())
    except json.JSONDecodeError:
        cleaned = re.sub(r',\s*([}\]])', r'\1', m.group())
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            return []

    if not isinstance(data, list):
        return []

    return [
        Task(
            id=d.get("id", "00"), title=d.get("title", "?"),
            filename=d.get("filename", f"{d.get('id','00')}_task.md"),
            content=d.get("content", ""), files=d.get("files", []),
            depends_on=d.get("depends_on", []),
        )
        for d in data
        if isinstance(d, dict)
    ]