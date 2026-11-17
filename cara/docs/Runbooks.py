"""Alert → runbook binding: every alert names a runbook section that exists.

An alert that fires at night is only as useful as its first step. A product
puts a ``runbook_url`` annotation on each Prometheus alerting rule, pointing at
one ``#anchor`` of one hand-written runbook page, and that section lists the
alerts it covers. Three things break the binding silently: a rule added
without the annotation, a URL that points at another page or carries a typo,
and a section renamed so its anchor no longer exists. Nothing in Prometheus
or Alertmanager checks any of them, so this module does.

Pure filesystem and string work: the rule files are read as YAML, the page as
markdown. It never names a product; the product passes its rule files, its
page and the URL prefix its rules use.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from pathlib import Path

_FENCE_RE = re.compile(r"^\s*(```|~~~)")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_INLINE_CODE_RE = re.compile(r"`([^`]*)`")
_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")


def heading_slug(text: str) -> str:
    """The anchor GitHub gives a heading: lowercased, punctuation dropped,
    each space a hyphen. Inline code and link markup keep only their text."""
    text = _LINK_RE.sub(r"\1", _INLINE_CODE_RE.sub(r"\1", text))
    text = text.strip().lower()
    kept = []
    for char in text:
        if char in (" ", "-", "_") or unicodedata.category(char)[0] in ("L", "N"):
            kept.append(char)
    return "".join(kept).replace(" ", "-")


def heading_sections(markdown: str) -> dict[str, str]:
    """Map each heading's anchor to the text of its section.

    A section runs to the next heading of ANY level. Headings inside fenced
    code blocks are not headings. A repeated slug gets GitHub's ``-1``,
    ``-2`` … suffix, so the anchors here are the anchors a browser resolves.
    """
    sections: dict[str, list[str]] = {}
    seen: dict[str, int] = {}
    current: list[str] | None = None
    in_fence = False
    for line in markdown.splitlines():
        if _FENCE_RE.match(line):
            in_fence = not in_fence
        heading = None if in_fence else _HEADING_RE.match(line)
        if heading:
            slug = heading_slug(heading.group(2))
            count = seen.get(slug, 0)
            seen[slug] = count + 1
            anchor = slug if count == 0 else f"{slug}-{count}"
            current = sections.setdefault(anchor, [])
            continue
        if current is not None:
            current.append(line)
    return {anchor: "\n".join(lines) for anchor, lines in sections.items()}


def alert_rules(rule_file: Path) -> list[tuple[str, str | None]]:
    """``(alert name, runbook_url or None)`` for every alerting rule in a file.

    Recording rules (``record:``) are not alerts and are skipped.
    """
    import yaml  # local: heavy optional dep

    document = yaml.safe_load(rule_file.read_text(encoding="utf-8")) or {}
    out: list[tuple[str, str | None]] = []
    for group in document.get("groups") or []:
        for rule in group.get("rules") or []:
            if "alert" not in rule:
                continue
            url = (rule.get("annotations") or {}).get("runbook_url")
            out.append((str(rule["alert"]), url if isinstance(url, str) else None))
    return out


def runbook_link_problems(
    rule_files: Iterable[Path],
    runbook: Path,
    url_prefix: str,
    *,
    require_listed: bool = True,
) -> list[str]:
    """Every way an alert fails to reach a first step, one line each.

    ``url_prefix`` is the page's published URL without the fragment (for
    example a repository blob URL); every ``runbook_url`` must be that prefix,
    ``#`` and an anchor of a heading on ``runbook``. With ``require_listed``
    the section must also name the alert in backticks, so a reader who lands
    on the anchor sees the alert they came for. Empty list = every alert is
    bound.
    """
    sections = heading_sections(runbook.read_text(encoding="utf-8"))
    problems: list[str] = []
    for rule_file in rule_files:
        for name, url in alert_rules(rule_file):
            where = f"{rule_file.name}: {name}"
            if url is None:
                problems.append(f"{where} has no runbook_url annotation")
                continue
            page, sep, anchor = url.partition("#")
            if page != url_prefix or not sep or not anchor:
                problems.append(
                    f"{where} runbook_url {url!r} is not {url_prefix}#<anchor>"
                )
                continue
            if anchor not in sections:
                problems.append(
                    f"{where} runbook_url anchor #{anchor} is no heading on {runbook.name}"
                )
                continue
            if require_listed and f"`{name}`" not in sections[anchor]:
                problems.append(
                    f"{where} is not listed in section #{anchor} of {runbook.name}"
                )
    return problems


__all__ = [
    "alert_rules",
    "heading_sections",
    "heading_slug",
    "runbook_link_problems",
]
