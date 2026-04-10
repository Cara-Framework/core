from __future__ import annotations
"""Persistent lessons — reviewer → planner/applier feedback loop.

Every time the reviewer flags a real bug and the auto-fix loop resolves
it, we want the *next* run of the planner (and applier) to remember the
lesson so the same class of mistake doesn't repeat. FIX 5 persists those
lessons to ``.usta/lessons.jsonl`` (one JSON object per line) and
exposes two small helpers that the CLI / planner / runner call into.

Design
------
* **Append-only JSONL.**  Each record is self-describing and small, so
  multiple sessions can append concurrently without a lock file.
* **De-duplicated on read.**  ``load_lessons()`` drops records with the
  same ``signature`` (category + normalised message) and keeps the
  most recent ``count``. Old lessons naturally fade out.
* **Budget-aware prompt injection.**  ``format_for_prompt()`` returns a
  trimmed, bullet-style block capped at ``max_chars`` so we never
  blow through Opus's context window even after hundreds of runs.

Only dependencies are stdlib json/pathlib/hashlib/datetime.
"""

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional


LESSONS_FILE = ".usta/lessons.jsonl"

# Hard caps so a runaway reviewer can't inflate the prompt.
DEFAULT_MAX_LESSONS = 12
DEFAULT_MAX_CHARS = 2800


# ── Data shape ──────────────────────────────────────────────

@dataclass
class Lesson:
    """A single distilled lesson from a past review cycle."""
    category: str             # "import" | "signature" | "duplicate" | "test-inconsistency" | "leftover" | "generic"
    message: str              # the actual lesson text, <= 240 chars
    signature: str            # sha1(category + normalised message)
    count: int = 1            # how often we've seen this lesson
    first_seen: str = ""      # ISO date
    last_seen: str = ""       # ISO date
    task_hint: str = ""       # optional: task title or id that produced it
    source: str = "reviewer"  # "reviewer" | "verifier" | "user"

    def to_json(self) -> str:
        return json.dumps({
            "category": self.category,
            "message": self.message,
            "signature": self.signature,
            "count": self.count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "task_hint": self.task_hint,
            "source": self.source,
        }, ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: dict) -> "Lesson":
        return cls(
            category=str(d.get("category", "generic"))[:32],
            message=str(d.get("message", ""))[:240],
            signature=str(d.get("signature", "")),
            count=int(d.get("count", 1) or 1),
            first_seen=str(d.get("first_seen", "")),
            last_seen=str(d.get("last_seen", "")),
            task_hint=str(d.get("task_hint", ""))[:80],
            source=str(d.get("source", "reviewer")),
        )


def _lessons_path(project_dir: Path) -> Path:
    return project_dir / LESSONS_FILE


# ── Signature / classification ──────────────────────────────

_CAT_RULES: list[tuple[str, re.Pattern]] = [
    ("import",            re.compile(r"\b(import|imported|module|modulenotfound|__init__|__all__)\b", re.I)),
    ("signature",         re.compile(r"\b(signature|argument|param|positional|keyword|takes|missing\s+1\s+required)\b", re.I)),
    ("duplicate",         re.compile(r"\b(duplicate|twice|two\s+definitions|defined\s+twice)\b", re.I)),
    ("leftover",          re.compile(r"\b(leftover|old\s+version|stale|still\s+exists|not\s+removed)\b", re.I)),
    ("test-inconsistency", re.compile(r"\b(test\s+(?:and|vs)\s+impl|test\s+asserts?|expected.*got|assertion\s*error)\b", re.I)),
    ("verifier",          re.compile(r"\b(pytest|jest|vitest|cargo|go\s+test|failing\s+test)\b", re.I)),
]


def classify(message: str) -> str:
    """Best-effort category for a free-text reviewer message."""
    for name, rx in _CAT_RULES:
        if rx.search(message):
            return name
    return "generic"


_WS_RE = re.compile(r"\s+")
_PATH_RE = re.compile(r"(?:[a-zA-Z_]+/)+[a-zA-Z0-9_]+\.(?:py|ts|tsx|js|jsx|go|rs|php)")
_NUM_RE = re.compile(r"\b\d+\b")


def _normalise(msg: str) -> str:
    """Normalise a message for de-duplication.

    Strips file paths and numeric constants so e.g.
    "test_foo.py:42: AssertionError" and
    "test_bar.py:17: AssertionError" collapse to the same signature.
    """
    s = _PATH_RE.sub("<path>", msg)
    s = _NUM_RE.sub("<n>", s)
    s = _WS_RE.sub(" ", s).strip().lower()
    return s[:240]


def _signature(category: str, message: str) -> str:
    h = hashlib.sha1()
    h.update(category.encode("utf-8"))
    h.update(b"|")
    h.update(_normalise(message).encode("utf-8"))
    return h.hexdigest()[:16]


# ── Record / load / format ──────────────────────────────────

def record_lessons(
    project_dir: Path,
    messages: Iterable[str],
    task_hint: str = "",
    source: str = "reviewer",
) -> int:
    """Append one lesson per *message* to ``.usta/lessons.jsonl``.

    Returns the number of new rows written. Duplicates within the same
    call are collapsed so a review with twelve near-identical findings
    only records one.
    """
    path = _lessons_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    seen_this_call: set[str] = set()
    today = datetime.now().strftime("%Y-%m-%d")

    rows: list[str] = []
    for raw in messages:
        text = (raw or "").strip()
        if not text or len(text) < 8:
            continue
        cat = classify(text)
        sig = _signature(cat, text)
        if sig in seen_this_call:
            continue
        seen_this_call.add(sig)
        lesson = Lesson(
            category=cat,
            message=text[:240],
            signature=sig,
            count=1,
            first_seen=today,
            last_seen=today,
            task_hint=task_hint[:80],
            source=source,
        )
        rows.append(lesson.to_json())

    if not rows:
        return 0
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(row + "\n")
    return len(rows)


def load_lessons(
    project_dir: Path,
    max_rows: int = DEFAULT_MAX_LESSONS,
) -> list[Lesson]:
    """Return de-duplicated lessons, most frequent first.

    Multiple rows with the same ``signature`` are collapsed: counts are
    summed, ``first_seen`` is taken from the earliest, ``last_seen``
    from the latest. The result is sorted by ``(count, last_seen)``
    descending and trimmed to *max_rows*.
    """
    path = _lessons_path(project_dir)
    if not path.exists():
        return []

    bucket: dict[str, Lesson] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            l = Lesson.from_dict(d)
            if not l.signature:
                l.signature = _signature(l.category, l.message)
            existing = bucket.get(l.signature)
            if existing is None:
                bucket[l.signature] = l
                continue
            existing.count += l.count
            if l.first_seen and (
                not existing.first_seen or l.first_seen < existing.first_seen
            ):
                existing.first_seen = l.first_seen
            if l.last_seen and l.last_seen > existing.last_seen:
                existing.last_seen = l.last_seen
            if l.task_hint and not existing.task_hint:
                existing.task_hint = l.task_hint
    except OSError:
        return []

    lessons = list(bucket.values())
    lessons.sort(key=lambda x: (x.count, x.last_seen), reverse=True)
    return lessons[:max_rows]


def format_for_prompt(
    lessons: list[Lesson],
    max_chars: int = DEFAULT_MAX_CHARS,
) -> str:
    """Render *lessons* as a markdown block for the planner/applier.

    Returns an empty string when the list is empty. The block opens
    with a header the prompts can rely on, and each bullet is prefixed
    with the category so the LLM can triage at a glance.
    """
    if not lessons:
        return ""
    header = (
        "## Lessons from earlier runs\n"
        "These are patterns the reviewer flagged on previous cycles of "
        "this project. Treat them as hard rules unless the new objective "
        "explicitly overrides them.\n\n"
    )
    lines: list[str] = []
    for l in lessons:
        tag = f"[{l.category}×{l.count}]" if l.count > 1 else f"[{l.category}]"
        hint = f" _(from {l.task_hint})_" if l.task_hint else ""
        lines.append(f"- {tag} {l.message}{hint}")
    body = "\n".join(lines)
    block = header + body + "\n"
    if len(block) <= max_chars:
        return block
    # Too large — trim bullets from the end until we fit.
    while lines and len(header + "\n".join(lines) + "\n") > max_chars:
        lines.pop()
    if not lines:
        return ""
    return header + "\n".join(lines) + "\n… (older lessons trimmed)\n"


# ── Convenience: harvest from a review result dict ──────────

def harvest_from_review(review: dict) -> list[str]:
    """Pull the quotable bits out of a reviewer JSON record.

    Accepts the same shape :mod:`usta.reviewer` emits:
    ``{"verdict": "...", "issues": [...], "fix_instructions": "..."}``
    Returns a list of strings suitable for :func:`record_lessons`.

    We record only issues whose severity is error or warning — info
    nits are too noisy to carry across runs.
    """
    out: list[str] = []
    if not isinstance(review, dict):
        return out
    if review.get("verdict") not in ("warn", "fail"):
        return out
    for issue in review.get("issues") or []:
        if not isinstance(issue, dict):
            continue
        sev = str(issue.get("severity", "")).lower()
        if sev not in ("error", "warning"):
            continue
        msg = (issue.get("msg") or "").strip()
        if not msg:
            continue
        file_hint = issue.get("file") or ""
        if file_hint and file_hint not in msg:
            msg = f"{file_hint}: {msg}"
        out.append(msg[:240])
    return out
