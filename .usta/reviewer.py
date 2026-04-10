from __future__ import annotations
"""Reviewer — Opus reviews applier diffs in a single batch call."""

import json
import re
import subprocess
from pathlib import Path
from typing import Optional

from .config import Cfg
from .claude import ask as ask_claude, ClaudeResult
from .tasks import Task


def _ask(prompt, cfg, **kwargs):
    """Dispatch to claude CLI or OpenRouter based on planner provider."""
    if cfg.planner_cfg.get("provider") == "openrouter":
        from .openrouter import ask_openrouter
        model = cfg.planner_cfg.get("model")
        # OpenRouter chat completions don't support the reviewer's
        # Read/Grep allow-list (FIX 12); drop the kwarg silently.
        return ask_openrouter(prompt, cfg, model=model,
                              system_prompt=kwargs.get("system_prompt"))
    # Filter any unknown kwargs the claude wrapper doesn't accept.
    supported = {
        "model", "system_prompt", "cwd", "timeout", "max_turns",
        "on_event", "session_id", "resume", "mcp_config", "allowed_tools",
    }
    clean = {k: v for k, v in kwargs.items() if k in supported}
    return ask_claude(prompt, cfg, **clean)

SYSTEM = """You are a senior code reviewer. You will receive diffs for one or more tasks.

CRITICAL RULES:
- You have a tiny tool budget: AT MOST 3 Read calls and AT MOST 3 Grep calls across the entire review, combined. Use them ONLY to verify a specific claim you cannot prove from the diff alone — e.g. "does this imported symbol actually exist in the module it claims?" or "is this function's old signature still referenced somewhere else?". Do NOT run Bash, do NOT browse directories, do NOT open unrelated files. If a diff looks fine from the diff, DO NOT open anything — go straight to the JSON verdict.
- If you genuinely cannot decide without running code (e.g. to check test behaviour), prefer verdict "warn" with a specific fix_instructions line over speculative tool use — the CLI has a separate test-runner pass that handles that case.
- Return ONLY a valid JSON array — no markdown fences, no prose before or after.

Each element must be:
{"task_id":"01","verdict":"pass|warn|fail","issues":[],"summary":"one line","fix_instructions":null}

Rules:
- verdict "pass": code is correct, no action needed
- verdict "warn": minor issues that SHOULD be fixed — you MUST provide fix_instructions
- verdict "fail": bugs or broken logic — you MUST provide fix_instructions
- issues: [{"severity":"error"|"warning"|"info","file":"path","line":0,"msg":"description"}]
- fix_instructions: concrete step-by-step instructions for an AI coder to fix the issues. Be VERY specific: name the exact file, the exact line or symbol, and what to change it to. Example: "In app/controllers/FooController.py, remove `import sys` on line 3 (unused). In app/controllers/__init__.py, remove `FooController` from the __all__ list." The more precise, the faster the auto-fix. null ONLY if verdict is "pass"

MANDATORY SANITY CHECKS — apply these to EVERY diff before giving a verdict:

1. MENTALLY EXECUTE TESTS. For any added/modified test, trace the implementation with the test's exact inputs and confirm the expected output matches. If a test asserts `camel_case("  foo__bar  ") == "fooBar"`, actually walk through the function with that input. If the real output would not match the assertion, that is a FAIL with severity "error" — either the test is wrong or the implementation is wrong, and you must say which.

2. CHECK FOR LEFTOVER OLD CODE. When a task replaces or renames a method/function/constant, verify the OLD version was actually REMOVED from the diff, not just that a new version was added alongside it. Two methods with conflicting names/behavior in the same class is a FAIL. Example: if the task is "rename message() to default_message()", the diff MUST show `-    def message(self` somewhere. If you only see `+    def default_message`, the old one is still there — fail it.

3. CHECK FOR DUPLICATE BLOCKS. If the same constant definition, method, import, or assignment appears twice in the same class/module in the new code, that's a FAIL (applier sometimes emits blocks twice).

4. VERIFY SIGNATURE/CALLER CONSISTENCY. If a function's signature changed (new/removed/renamed params), scan the diff for callers and confirm they were updated. A caller still passing the old args is a FAIL.

5. VERIFY IMPORT/EXPORT CONSISTENCY. If a new symbol is added to a module, and the task says to export it from `__init__.py` / package index, confirm the export line exists in the diff.

Be pragmatic on STYLE — ignore whitespace/naming nitpicks. Be STRICT on the five sanity checks above. Focus on bugs, logic errors, test/impl inconsistencies, leftover dead code, and duplicate blocks."""


def review_batch(tasks: list[Task], cfg: Cfg) -> tuple[dict[str, dict], Optional[ClaudeResult]]:
    """Review multiple tasks in a single Opus call.

    Returns (dict of task_id -> review_dict, claude_result).
    """
    # Collect diffs for each task
    task_diffs = {}
    for t in tasks:
        diff = _git_diff_last_commit(cfg.project_dir, t.files)
        if not diff:
            diff = _git_diff_unstaged(cfg.project_dir, t.files)
        if diff:
            task_diffs[t.id] = (t, diff)

    if not task_diffs:
        return {t.id: {"verdict": "pass", "issues": [], "summary": "No changes detected"}
                for t in tasks}, None

    # Build one prompt with all diffs
    sections = []

    # Inject project conventions so Opus reviews against local rules.
    for conv_name in (".usta/CONVENTIONS.md", "CONVENTIONS.md", "CODING_RULES.md"):
        conv_path = cfg.project_dir / conv_name
        if conv_path.exists() and conv_path.is_file():
            try:
                conv_text = conv_path.read_text(errors="replace").strip()
                if conv_text:
                    sections.append(
                        f"## Project Rules (check diffs against these)\n{conv_text}"
                    )
                    break
            except OSError:
                continue

    for tid, (t, diff) in task_diffs.items():
        # Cap each diff — Opus handles 200k context; be generous.
        max_per = 40000 // len(task_diffs) if len(task_diffs) > 1 else 30000
        sections.append(f"## Task {tid}: {t.title}\n```diff\n{diff[:max_per]}\n```")

    prompt = "\n\n".join(sections)
    prompt += "\n\nReturn ONLY the JSON array with one object per task."

    # FIX 12: the reviewer gets a tiny Read/Grep allow-list so it can
    # spot-check symbol existence without blowing up its own prompt.
    # Bash/Write/Edit remain forbidden — the reviewer MUST NOT mutate
    # the tree, and the separate verifier.py pass is responsible for
    # actually running tests. max_turns=8: 1 for JSON output + headroom
    # for at most 6 tool calls as described in the system prompt.
    resp = _ask(
        prompt, cfg, model=cfg.claude_model, system_prompt=SYSTEM,
        max_turns=8, allowed_tools=["Read", "Grep"],
    )
    if not resp.ok:
        # Return error for all tasks
        err = {"verdict": "error", "issues": [], "summary": f"Review failed: {resp.error}"}
        return {t.id: err for t in tasks}, resp

    reviews = _parse_batch(resp.text, [t.id for t in tasks])

    # Tasks with no diff get auto-pass
    for t in tasks:
        if t.id not in task_diffs and t.id not in reviews:
            reviews[t.id] = {"verdict": "pass", "issues": [], "summary": "No changes detected"}

    return reviews, resp


def review_task(task: Task, cfg: Cfg) -> tuple[dict, Optional[ClaudeResult]]:
    """Review a single task. Convenience wrapper around review_batch."""
    reviews, resp = review_batch([task], cfg)
    return reviews.get(task.id, {"verdict": "error", "issues": [], "summary": "Not found"}), resp


def _git_root_of(path: Path) -> Optional[Path]:
    """Walk up from *path* looking for a .git directory/file.

    Returns the first ancestor (inclusive) that contains ``.git``, or None
    if we hit the filesystem root. Used by FIX 5 to locate the real git
    repo behind a symlink that points out of the current project.
    """
    p = path.resolve() if path.exists() else path
    for ancestor in [p, *p.parents]:
        if (ancestor / ".git").exists():
            return ancestor
    return None


def _split_files_by_repo(
    project_dir: Path, files: list[str]
) -> dict[Path, list[str]]:
    """Group *files* by the git repo they actually live in.

    Handles the cheapa/api situation where ``api/cara`` is a symlink to
    ``commons/cara/cara`` — a completely separate git repo. ``git diff``
    from the api repo will show nothing for those paths because the
    underlying blobs live in a different repo.

    Returns ``{repo_root: [path_within_repo, ...]}``. The *project_dir*
    repo (resolved) is always the first group for files that live
    directly inside it. Caller can distinguish the "home" repo by
    comparing against ``project_dir.resolve()``.
    """
    try:
        project_resolved = project_dir.resolve()
    except OSError:
        project_resolved = project_dir
    project_repo = _git_root_of(project_resolved) or project_resolved

    groups: dict[Path, list[str]] = {}
    for rel in files:
        full = project_dir / rel
        try:
            resolved = full.resolve()
        except OSError:
            resolved = full
        repo = _git_root_of(resolved)
        if repo is None:
            # File not in any git repo — fall back to the project repo.
            groups.setdefault(project_repo, []).append(rel)
            continue
        try:
            path_in_repo = str(resolved.relative_to(repo))
        except ValueError:
            path_in_repo = rel
        groups.setdefault(repo, []).append(path_in_repo)
    return groups


def _git_diff_unstaged(project_dir: Path, files: list[str]) -> str:
    """Return the working-tree diff for *files* (staged + unstaged vs HEAD).

    FIX 5: if any of *files* live inside a symlinked git submodule whose
    actual repo is elsewhere (e.g. ``api/cara`` -> ``commons/cara``), we
    run ``git diff`` against each underlying repo and concatenate the
    results so the reviewer sees every change regardless of mount point.
    """
    if not files:
        return ""
    try:
        home = project_dir.resolve()
    except OSError:
        home = project_dir
    home_repo = _git_root_of(home) or home
    groups = _split_files_by_repo(project_dir, files)
    chunks: list[str] = []
    for repo, repo_files in groups.items():
        try:
            cmd = ["git", "diff", "HEAD", "--"] + repo_files
            r = subprocess.run(
                cmd, capture_output=True, text=True, cwd=str(repo)
            )
            out = r.stdout.strip()
            if out:
                if repo != home_repo:
                    chunks.append(
                        f"# --- diff from separate repo at {repo} "
                        f"(reachable via symlink from {project_dir}) ---"
                    )
                chunks.append(out)
        except Exception:
            continue
    return "\n\n".join(chunks)


def _git_last_touching_sha(repo_dir: Path, file: str, max_back: int = 10) -> str:
    """Return the SHA of the most recent commit in *repo_dir* that modified *file*.

    Returns an empty string if no such commit exists (file is new/uncommitted
    or path is outside the repo).
    """
    try:
        cmd = ["git", "log", f"-{max_back}", "--format=%H", "--", file]
        r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(repo_dir))
        lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
        return lines[0] if lines else ""
    except Exception:
        return ""


def _git_diff_last_commit(project_dir: Path, files: list[str]) -> str:
    """Per-file diff lookup: find the last commit that *actually* touched
    each file and return its diff scoped to that file. The per-file diffs
    are concatenated so a single task's review sees only its own changes,
    even when later commits modified other files (or the same file for a
    different task).

    FIX 5: groups files by their actual git repo first so symlinked
    submodules (e.g. ``api/cara`` -> ``commons/cara``) still get their
    diffs surfaced to the reviewer.
    """
    if not files:
        return ""
    try:
        home = project_dir.resolve()
    except OSError:
        home = project_dir
    home_repo = _git_root_of(home) or home
    groups = _split_files_by_repo(project_dir, files)
    chunks: list[str] = []
    # sha -> set(files already covered) — scoped per repo so two repos
    # with coincidentally identical SHAs don't clobber each other.
    seen: dict[tuple[Path, str], set[str]] = {}
    for repo, repo_files in groups.items():
        repo_chunks: list[str] = []
        for f in repo_files:
            sha = _git_last_touching_sha(repo, f)
            if not sha:
                continue
            key = (repo, sha)
            if f in seen.get(key, set()):
                continue
            try:
                cmd = ["git", "show", sha, "--", f]
                r = subprocess.run(
                    cmd, capture_output=True, text=True, cwd=str(repo)
                )
                body = r.stdout.strip()
                if body:
                    repo_chunks.append(body)
                    seen.setdefault(key, set()).add(f)
            except Exception:
                pass
        if repo_chunks:
            if repo != home_repo:
                chunks.append(
                    f"# --- diff from separate repo at {repo} "
                    f"(reachable via symlink from {project_dir}) ---"
                )
            chunks.extend(repo_chunks)
    return "\n\n".join(chunks)


def _parse_batch(text: str, task_ids: list[str]) -> dict[str, dict]:
    """Parse batch review JSON array into dict of task_id -> review.

    Handles: clean JSON, markdown-fenced JSON, truncated JSON (missing ] at end).
    """
    text = text.strip()
    # Strip all markdown fences (```json ... ```)
    text = re.sub(r'```(?:json)?\s*\n?', '', text)
    text = text.strip()

    results = {}

    # 1) Try direct parse
    try:
        arr = json.loads(text)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict) and "task_id" in item:
                    results[str(item["task_id"])] = item
            if results:
                return results
        if isinstance(arr, dict):
            tid = str(arr.get("task_id", task_ids[0] if task_ids else "?"))
            results[tid] = arr
            return results
    except json.JSONDecodeError:
        pass

    # 2) Try to extract JSON array
    m = re.search(r'\[.*\]', text, re.DOTALL)
    if m:
        try:
            arr = json.loads(m.group())
            for item in arr:
                if isinstance(item, dict) and "task_id" in item:
                    results[str(item["task_id"])] = item
            if results:
                return results
        except json.JSONDecodeError:
            pass

    # 3) Truncated array — Opus hit output limit, missing closing ] or }
    #    Fix by finding all complete JSON objects individually
    depth = 0
    start = -1
    for i, c in enumerate(text):
        if c == '{':
            if depth == 0:
                start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and start >= 0:
                chunk = text[start:i+1]
                try:
                    obj = json.loads(chunk)
                    if isinstance(obj, dict) and "task_id" in obj:
                        results[str(obj["task_id"])] = obj
                except json.JSONDecodeError:
                    pass
                start = -1

    if results:
        return results

    # 4) Complete failure
    if task_ids:
        results[task_ids[0]] = {
            "verdict": "warn", "issues": [],
            "summary": text[:100] if text else "Parse failed"
        }

    return results
