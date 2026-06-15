"""Robust JSON parsing for AI responses.

LLM providers return JSON wrapped in markdown fences, followed by prose, or
truncated mid-structure when the token limit is hit. ``parse_json`` tolerates
all of these; ``_repair_truncated_json`` best-effort closes an unterminated
document.
"""

from __future__ import annotations

import contextlib
import json
import re
from typing import Any

from cara.ai.exceptions import AIResponseError


def _log(level: str, msg: str) -> None:
    # Logging must never break parsing.
    with contextlib.suppress(Exception):
        from cara.facades import Log

        getattr(Log, level)(msg, category="cara.ai")


def parse_json(raw: str, *, fallback: Any = None) -> Any:
    """Parse JSON, tolerating markdown fences, trailing commentary, and truncation.

    Strategy, in order: closed ```json fence → open (truncated) fence → direct
    parse → repair-first from the outermost opener → first-open/last-close
    substring. Returns ``fallback`` if provided and nothing parses, else raises
    :class:`AIResponseError`.
    """
    text = (raw or "").strip()

    # 1. Closed ```json ... ``` fence.
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    else:
        # 2. Open-ended fence (truncated — no closing ```).
        open_fence = re.match(r"```(?:json)?\s*([\s\S]*)$", text)
        if open_fence:
            text = open_fence.group(1).strip()

    # Direct parse.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        _log("debug", "Direct JSON decode failed; attempting repair path")

    # 3. Repair-first: start from the first outer opener and auto-close a
    # truncated/unbalanced structure. Preserves the OUTERMOST container.
    for open_c in ("{", "["):
        i = text.find(open_c)
        if i == -1:
            continue
        repaired = _repair_truncated_json(text[i:])
        if repaired is None:
            continue
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            continue

    # 4. Last resort: trim to first-open / last-close of the outermost container.
    for open_c, close_c in (("{", "}"), ("[", "]")):
        i = text.find(open_c)
        j = text.rfind(close_c)
        if i != -1 and j != -1 and j > i:
            candidate = text[i : j + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

    if fallback is not None:
        _log("warning", "parse_json: unable to parse, using fallback")
        return fallback
    raise AIResponseError(f"Unable to parse JSON from AI response: {raw[:200]}")


def _repair_truncated_json(text: str) -> str | None:
    """Best-effort close of an unterminated JSON document.

    Walks with a stack of open containers, tracking for each level the position
    just after the last *complete* element. When the input runs out mid-
    structure, roll back to the outermost container's last complete-element
    position and append closers, avoiding invalid fragments like ``{"key"}``.
    """
    stack: list[tuple[str, int]] = []
    in_string = False
    escape = False
    element_complete = False

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
                element_complete = True
            i += 1
            continue

        if ch == '"':
            in_string = True
            i += 1
            continue

        if ch in "{[":
            stack.append((ch, i + 1))
            element_complete = False
            i += 1
            continue

        if ch in "}]":
            if not stack:
                return None
            opener, _ = stack.pop()
            if (opener == "{" and ch != "}") or (opener == "[" and ch != "]"):
                return None
            element_complete = True
            if stack:
                stack[-1] = (stack[-1][0], i + 1)
            i += 1
            continue

        if ch == ":":
            element_complete = False
            i += 1
            continue

        if ch == ",":
            if stack and element_complete:
                stack[-1] = (stack[-1][0], i + 1)
            element_complete = False
            i += 1
            continue

        if ch in " \t\r\n":
            i += 1
            continue

        # Primitive: number / true / false / null.
        j = i
        while j < n and text[j] not in ",}] \t\r\n":
            j += 1
        if j > i:
            element_complete = True
            i = j
            continue

        i += 1  # safety; shouldn't hit

    if not stack and not in_string:
        return text  # already balanced

    if not stack:
        return None

    cut = stack[-1][1]
    trimmed = text[:cut].rstrip()
    if trimmed.endswith(","):
        trimmed = trimmed[:-1].rstrip()

    closers: list[str] = []
    for opener, _ in reversed(stack):
        closers.append("}" if opener == "{" else "]")
    return f"{trimmed}{''.join(closers)}"
