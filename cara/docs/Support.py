"""Filesystem, git and rendering primitives shared by the documentation passes."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path

from cara.docs.DocsManifest import DocsManifest

Say = Callable[[str], None]


def read(path: Path) -> str:
    """Return a file's text, or an empty string when it cannot be read."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def git_root(path: Path) -> Path | None:
    """Nested-repo reality: any subfolder may be its own git repository."""
    current = path if path.is_dir() else path.parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    return None


@cache
def dirty_paths(root: Path) -> frozenset[str]:
    """Repository-relative paths whose worktree or index content changed."""
    try:
        output = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        ).stdout
    except OSError, subprocess.SubprocessError:
        return frozenset()

    dirty: set[str] = set()
    records = output.split("\0")
    index = 0
    while index < len(records):
        record = records[index]
        index += 1
        if len(record) < 4:
            continue
        status = record[:2]
        dirty.add(record[3:])
        if any(marker in status for marker in ("R", "C")) and index < len(records):
            renamed_from = records[index]
            index += 1
            if renamed_from:
                dirty.add(renamed_from)
    return frozenset(dirty)


def latest_committed_change(root: Path, relative_paths: list[str]) -> tuple[float, str]:
    """Latest commit touching any path, queried in bounded batches."""
    newest = (0.0, "")
    batch_size = 256
    for offset in range(0, len(relative_paths), batch_size):
        batch = relative_paths[offset : offset + batch_size]
        try:
            output = subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "log",
                    "-1",
                    "--format=%ct",
                    "--name-only",
                    "--no-renames",
                    "--",
                    *batch,
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            ).stdout
        except OSError, subprocess.SubprocessError:
            continue
        lines = [line for line in output.splitlines() if line]
        if not lines:
            continue
        try:
            timestamp = float(lines[0])
        except ValueError:
            continue
        changed_path = lines[1] if len(lines) > 1 else batch[0]
        if timestamp > newest[0]:
            newest = (timestamp, changed_path)
    return newest


def newest_change(paths: list[Path], root: Path) -> tuple[float, str]:
    """Newest committed or genuinely uncommitted change across source files.

    Clean-file mtimes are ignored: formatters, checkouts, and restores may
    touch a file without changing its content. Dirty and untracked files still
    use mtime so uncommitted work participates in freshness checks.

    ``root`` is the checkout the reported path is expressed relative to; it is
    a parameter rather than module state because a caller may point the pass
    at a fixture tree.
    """
    grouped: dict[Path, list[Path]] = {}
    newest = (0.0, "")
    for path in sorted(set(paths)):
        repository = git_root(path)
        if repository is None:
            with suppress(OSError):
                timestamp = path.stat().st_mtime
                if timestamp > newest[0]:
                    newest = (timestamp, str(path.relative_to(root)))
            continue
        grouped.setdefault(repository, []).append(path)

    for repository, repository_paths in grouped.items():
        relative = [str(path.relative_to(repository)) for path in repository_paths]
        committed_at, committed_path = latest_committed_change(repository, relative)
        if committed_at > newest[0]:
            newest = (
                committed_at,
                str((repository / committed_path).relative_to(root)),
            )

        dirty = dirty_paths(repository)
        for path, relative_path in zip(repository_paths, relative, strict=True):
            if relative_path not in dirty:
                continue
            with suppress(OSError):
                timestamp = path.stat().st_mtime
                if timestamp > newest[0]:
                    newest = (timestamp, str(path.relative_to(root)))
    return newest


def verified_ts(front_matter: dict) -> float | None:
    """When a human last checked this doc against the code, from its
    ``verified:`` date — ``None`` when the doc never declares one.

    Deliberately NOT the file's timestamp. A doc is fresh because someone
    re-read it against its sources, not because the file was touched: a
    typo fix, a reformat, or simply committing the docs would otherwise
    clear every STALE flag at once and leave the check blind.

    The date is taken as the END of that day, so code changed earlier on
    the same day the doc was verified counts as covered.
    """
    raw = front_matter.get("verified")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        day = datetime.strptime(raw.strip(), "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None
    return (day + timedelta(days=1)).timestamp()


def _stable(text: str) -> str:
    """Ignore volatile timestamps while retaining metadata on the same line."""
    return re.sub(
        r"^(_(?:Generated|Checked):\s*)[^·\n]+(?=\s*·)",
        r"\1<TIMESTAMP>",
        text,
        flags=re.MULTILINE,
    )


def write_if_changed(path: Path, content: str, label: str, say: Say) -> None:
    """Write only when the content actually differs, timestamps excluded.

    Repeated runs (an editor hook fires one on every session start) must never
    churn mtimes or git status. The cost of that idempotence is that a
    generator which produces NOTHING reports "(unchanged)" forever, so a
    generator with no subject must say so in its page rather than emit an
    empty table.
    """
    content = content.rstrip() + "\n"
    if _stable(read(path)) == _stable(content):
        say(f"  = {label} (unchanged)")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    say(f"  ✓ {label}")


def header(manifest: DocsManifest, title: str, sources: list[str], now: str) -> str:
    """Front block every generated reference page opens with."""
    source_list = " · ".join(sources)
    return (
        f"<!-- GENERATED by `craft {manifest.generator_command}` — "
        "DO NOT EDIT BY HAND. -->\n\n"
        f"# {title}\n\n"
        f"_Generated: {now} · sources: {source_list} · product: {manifest.product}_\n\n"
    )


def md_escape(text: str) -> str:
    """Escape the one character that breaks a markdown table cell."""
    return text.replace("|", "\\|")


__all__ = [
    "Say",
    "dirty_paths",
    "git_root",
    "header",
    "latest_committed_change",
    "md_escape",
    "newest_change",
    "read",
    "verified_ts",
    "write_if_changed",
]
