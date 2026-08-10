"""Freshness distinguishes a content change from a touched file.

A doc is stale when its sources CHANGED, not when they were touched. Clean
files get their timestamp from git, because a checkout, a restore or a
formatter rewrites mtimes without changing a byte and would otherwise mark
every doc stale at once. Dirty and untracked files still use mtime, so
uncommitted work participates.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from cara.docs.Support import dirty_paths, newest_change


def _git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def test_newest_change_ignores_clean_mtime_but_tracks_dirty_content(tmp_path):
    source = tmp_path / "Source.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", "Source.py")
    _git(
        tmp_path,
        "-c",
        "user.name=Docs Test",
        "-c",
        "user.email=docs@example.test",
        "commit",
        "-qm",
        "initial",
    )
    committed_at = float(_git(tmp_path, "show", "-s", "--format=%ct", "HEAD"))

    dirty_paths.cache_clear()
    touched_at = committed_at + 3600
    os.utime(source, (touched_at, touched_at))
    assert newest_change([source], tmp_path) == (committed_at, "Source.py")

    source.write_text("VALUE = 2\n", encoding="utf-8")
    dirty_at = committed_at + 7200
    os.utime(source, (dirty_at, dirty_at))
    dirty_paths.cache_clear()
    assert newest_change([source], tmp_path) == (dirty_at, "Source.py")


def test_newest_change_reports_paths_relative_to_the_given_root(tmp_path):
    """The reported path belongs to the checkout the caller named.

    ``root`` is a parameter rather than module state so a caller pointed at a
    fixture tree gets fixture-relative answers. The version that read a
    module-level root reported paths from the real repository while walking
    somebody else's directory.
    """
    nested = tmp_path / "services" / "app"
    nested.mkdir(parents=True)
    source = nested / "Source.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    dirty_paths.cache_clear()

    timestamp, reported = newest_change([source], tmp_path)

    assert reported == "services/app/Source.py"
    assert timestamp > 0
