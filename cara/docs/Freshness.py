"""Navigation manifest and the freshness report for hand-written docs.

Freshness is a DATE comparison and nothing more: every hand-written page
declares the ``sources:`` globs it describes and a ``verified:`` date — a
human's assertion that they read the page against that code on that day. When
a source has changed since, the page is STALE.

The date deliberately is not the file's mtime. Touching the file would then
clear the flag without anyone re-reading anything, which turns the whole check
into a formality.
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from cara.docs.DocsManifest import DocsManifest
from cara.docs.Support import (
    Say,
    newest_change,
    read,
    verified_ts,
    write_if_changed,
)

FM_RE = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)

# How many pages ask git for their newest source change at once. Each answer
# is one or more ``git log`` processes this pass only waits on.
_GIT_WORKERS = 8


def front_matter(src: str) -> dict:
    """Parse a document's leading ``---`` block into a shallow mapping."""
    match = FM_RE.match(src)
    if not match:
        return {}
    out: dict = {}
    key = None
    for line in match.group(1).splitlines():
        if re.match(r"^\s*-\s+", line) and key:
            out.setdefault(key, [])
            if isinstance(out[key], list):
                out[key].append(line.split("-", 1)[1].strip())
        elif ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            out[key] = value.strip() or []
    return out


def source_files(root: Path, pattern: str) -> list[Path]:
    """Files one ``sources:`` glob binds a page to (bytecode never counts).

    A pattern ``Path.glob`` refuses — empty, or absolute — binds the page to
    no file of this checkout, which is what the report says about it. It used
    to raise out of the whole pass with a traceback naming neither the page
    nor the entry.
    """
    try:
        matches = list(root.glob(pattern))
    except ValueError, NotImplementedError:
        return []
    return [f for f in matches if f.is_file() and "__pycache__" not in f.parts]


def hand_written_pages(manifest: DocsManifest) -> list[Path]:
    """Every markdown under docs/ except the generated reference."""
    return [
        f
        for f in sorted(manifest.docs.rglob("*.md"))
        if manifest.reference not in f.parents
    ]


def empty_source_globs(manifest: DocsManifest) -> list[str]:
    """Every ``sources:`` entry, across hand-written pages, matching no file.

    The STRUCTURAL half of freshness. Being stale is normal — code moves and a
    human re-reads the page — but a glob that matches nothing binds the page
    to no code at all, and no amount of re-reading fixes that: the page must be
    re-pointed. The report marks it STALE among the ordinary stale rows; a
    product gate fails on this list alone.
    """
    empty: list[str] = []
    for doc in hand_written_pages(manifest):
        sources = front_matter(read(doc)).get("sources") or []
        if not isinstance(sources, list):
            continue
        empty += [
            f"{doc.relative_to(manifest.docs)}: {pattern}"
            for pattern in sources
            if not source_files(manifest.root, pattern)
        ]
    return empty


def doc_title(path: Path) -> str:
    """Title from front matter, else the first heading, else the file stem."""
    src = read(path)
    fm = front_matter(src)
    if isinstance(fm.get("title"), str) and fm["title"]:
        return fm["title"]
    match = re.search(r"^#\s+(.+)$", src, re.M)
    return match.group(1).strip() if match else path.stem


def gen_nav(manifest: DocsManifest, say: Say) -> None:
    """Tree-shaped sidebar manifest split into viewer SPACES.

    Every section carries space: "internal" | "external" and the viewer shows
    one space at a time (the toggle appears only when both exist). A product
    with no external tree gets a single-space sidebar.
    """
    docs = manifest.docs
    reference = manifest.reference

    def entry(f: Path, title: str | None = None) -> dict:
        return {"title": title or doc_title(f), "path": str(f.relative_to(docs))}

    def items_in(d: Path) -> list:
        return [entry(f) for f in sorted(d.glob("*.md")) if f.name != "README.md"]

    internal: dict = {
        "section": "Internal",
        "space": "internal",
        "items": items_in(docs / "internal"),
        "groups": [],
    }
    if (docs / "README.md").exists():
        internal["items"].append(entry(docs / "README.md", "About these docs"))
    for title, sub in [
        ("Architecture", "architecture"),
        ("Operations", "operations"),
        ("Strategy", "strategy"),
        ("Security", "security"),
    ]:
        directory = docs / "internal" / sub
        items = items_in(directory) if directory.is_dir() else []
        if items:
            internal["groups"].append({"title": title, "items": items})
    ref_items = items_in(reference) if reference.is_dir() else []
    if ref_items:
        internal["groups"].append(
            {"title": "Reference", "generated": True, "items": ref_items}
        )

    external: dict = {
        "section": "External",
        "space": "external",
        "items": [],
        "groups": [],
    }
    ext = docs / "external"
    if (ext / "README.md").exists():
        external["items"].append(entry(ext / "README.md", "Overview"))
    external["items"] += items_in(ext) if ext.is_dir() else []
    kb = ext / "kb"
    if kb.is_dir():
        if (kb / "README.md").exists():
            external["items"].append(entry(kb / "README.md", "KB editorial guide"))
        categories = sorted(
            (p for p in kb.iterdir() if p.is_dir()),
            key=lambda p: (p.name != "getting-started", p.name),
        )
        for category in categories:
            items = items_in(category)
            if items:
                external["groups"].append(
                    {
                        "title": category.name.replace("-", " ").capitalize(),
                        "items": items,
                    }
                )

    sections = [s for s in (internal, external) if s["items"] or s["groups"]]
    write_if_changed(
        docs / "nav.json",
        json.dumps(sections, ensure_ascii=False, indent=1),
        "nav.json",
        say,
    )


def freshness(manifest: DocsManifest, write: bool, now: str, say: Say) -> int:
    """Compare every hand-written doc's sources against its verified date."""
    root = manifest.root
    docs = manifest.docs
    reference = manifest.reference
    rows: list = []
    stale_count = 0
    # Pages whose verdict is the date of their newest source change: the
    # row's position, the page, its verified date and the files it binds.
    dated: list[tuple[int, Path, float, list[Path]]] = []
    # Every markdown under docs/ except the generated reference — the same
    # surface ``owned_markdowns`` judges. Enumerating the subtrees by name
    # instead left docs/README.md (which documents this very command and the
    # viewer) permanently unscanned: no front-matter, no STALE, ever.
    for doc in hand_written_pages(manifest):
        fm = front_matter(read(doc))
        sources = fm.get("sources") or []
        if not isinstance(sources, list) or not sources:
            rows.append((doc, "—", "no sources declared", False))
            continue
        doc_ts = verified_ts(fm)
        if doc_ts is None:
            rows.append((doc, "—", "no verified: date", True))
            stale_count += 1
            continue
        bound: list[Path] = []
        missing: list[str] = []
        for source_glob in sources:
            matches = source_files(root, source_glob)
            if not matches:
                missing.append(source_glob)
                continue
            bound.extend(matches)
        if missing:
            rows.append((doc, "—", f"empty source glob(s): {missing}", True))
            stale_count += 1
            continue
        dated.append((len(rows), doc, doc_ts, bound))
        rows.append(None)
    # One page's answer is a ``git log`` per repository it binds, and no page
    # needs another's: asked one after the other they were 604 processes and
    # 7.5 s of waiting on Synkronus. The questions and the row order are
    # unchanged; only the waiting overlaps.
    with ThreadPoolExecutor(max_workers=_GIT_WORKERS) as pool:
        changes = list(pool.map(lambda page: newest_change(page[3], root), dated))
    for (position, doc, doc_ts, _bound), newest in zip(dated, changes, strict=True):
        if newest[0] > doc_ts:
            days = (newest[0] - doc_ts) / 86400
            rows[position] = (doc, newest[1], f"source is {days:.1f} days newer", True)
            stale_count += 1
        else:
            rows[position] = (doc, "", "fresh", False)
    lines = [
        f"<!-- GENERATED by `craft {manifest.generator_command}` -->",
        "",
        "# Freshness report",
        "",
        f"_Checked: {now} · each doc's `sources:` globs are compared against "
        "its `verified:` date._",
        "",
        "| Doc | Status | Note |",
        "|---|---|---|",
    ]
    for doc, source, note, stale in rows:
        relative = doc.relative_to(docs)
        flag = "🔴 STALE" if stale else ("⚪" if note == "no sources declared" else "🟢")
        extra = f" (`{source}`)" if source and stale else ""
        lines.append(f"| `{relative}` | {flag} | {note}{extra} |")
        if stale:
            say(f"  🔴 STALE: {relative} — {note}{extra}")
    if write:
        write_if_changed(
            reference / "FRESHNESS.md",
            "\n".join(lines) + "\n",
            f"reference/FRESHNESS.md ({stale_count} stale / {len(rows)} docs)",
            say,
        )
    else:
        say(f"Freshness: {stale_count} stale / {len(rows)} docs")
    if stale_count:
        say(
            "  → fix: re-read each stale doc, re-verify against its sources, "
            "update the prose, bump its verified: date"
        )
    return stale_count


__all__ = [
    "FM_RE",
    "doc_title",
    "empty_source_globs",
    "freshness",
    "front_matter",
    "gen_nav",
    "hand_written_pages",
    "source_files",
]
