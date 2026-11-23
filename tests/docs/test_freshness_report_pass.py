"""Gate: the freshness pass asks git side by side, and survives a bad entry.

Each page's verdict is the date of its newest source change — a ``git log``
per repository the page binds. Asked one page after the other that was 604
processes and 7.5 s on Synkronus for a report an editor hook rebuilds on every
session start (audit 2026-11-23); no page needs another's answer, so the
waiting now overlaps (2.2 s) and the report is byte-identical.

The second half is the entry no glob accepts. ``Path.glob`` refuses an empty
or an absolute pattern by raising, which took the whole report down with a
traceback naming neither the page nor the entry. Such an entry binds the page
to no file of the checkout, and that is what the report says.
"""

from __future__ import annotations

import threading

import pytest

from cara.docs import Freshness as freshness_module
from cara.docs import empty_source_globs
from cara.docs.Freshness import freshness, source_files

from ._fixtures import make_checkout, manifest_for, write


def _page(sources: list[str], verified: str = "2026-01-01") -> str:
    listed = "\n".join(f"  - {source}" for source in sources)
    return f"---\nverified: {verified}\nsources:\n{listed}\n---\n# Page\n"


def _report(manifest) -> list[str]:
    said: list[str] = []
    freshness(manifest, True, "2026-01-01 00:00", said.append)
    page = (manifest.reference / "FRESHNESS.md").read_text(encoding="utf-8")
    return [line for line in page.splitlines() if line.startswith("| `")]


def test_pages_are_asked_side_by_side_and_reported_in_page_order(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    root = make_checkout(tmp_path, "alpha")
    names = ["a", "b", "c", "d"]
    for name in names:
        write(root / "api" / "app" / f"{name}.py")
        write(root / "docs" / "internal" / f"{name}.md", _page([f"api/app/{name}.py"]))
    # Two lookups must be in flight together to get past this; asked one
    # after the other, the first waits out the timeout and the pass fails.
    together = threading.Barrier(2, timeout=10)
    asked: list[str] = []

    def newest(paths, _root):
        together.wait()
        name = paths[0].stem
        asked.append(name)
        # ``b`` and ``d`` changed after the pages were verified.
        return (4102444800.0, f"api/app/{name}.py") if name in "bd" else (0.0, "")

    monkeypatch.setattr(freshness_module, "newest_change", newest)

    rows = _report(manifest_for(root, "alpha"))

    assert sorted(asked) == names
    assert [row.split("|")[1].strip() for row in rows] == [
        f"`internal/{name}.md`" for name in names
    ]
    assert [row.split("|")[2].strip() for row in rows] == [
        "🟢",
        "🔴 STALE",
        "🟢",
        "🔴 STALE",
    ]
    assert "(`api/app/b.py`)" in rows[1]


def test_a_page_with_no_dated_verdict_keeps_its_place_among_those_with_one(
    tmp_path,
):
    root = make_checkout(tmp_path, "alpha")
    write(root / "api" / "app" / "Live.py")
    write(root / "docs" / "internal" / "a.md", _page(["api/app/Live.py"], "2999-01-01"))
    write(root / "docs" / "internal" / "b.md", "# No front matter\n")
    write(root / "docs" / "internal" / "c.md", _page(["api/app/Gone.py"]))
    write(root / "docs" / "internal" / "d.md", _page(["api/app/Live.py"], "2999-01-01"))

    rows = _report(manifest_for(root, "alpha"))

    notes = [row.split("|")[3].strip() for row in rows]
    assert notes[0] == "fresh"
    assert notes[1] == "no sources declared"
    assert notes[2].startswith("empty source glob(s): ['api/app/Gone.py']")
    assert notes[3] == "fresh"


@pytest.mark.parametrize("pattern", ["", "/etc/hosts", "/api/app/*.py"])
def test_a_pattern_glob_refuses_binds_no_file(tmp_path, pattern):
    write(tmp_path / "api" / "app" / "Live.py")

    assert source_files(tmp_path, pattern) == []


def test_a_page_bound_to_an_absolute_path_is_reported_by_name(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(root / "api" / "app" / "Live.py")
    write(
        root / "docs" / "internal" / "page.md",
        _page(["/api/app/Live.py", "api/app/Live.py"]),
    )
    manifest = manifest_for(root, "alpha")

    # Both used to raise ``NotImplementedError`` out of the whole pass.
    assert empty_source_globs(manifest) == ["internal/page.md: /api/app/Live.py"]
    rows = _report(manifest)
    assert "🔴 STALE" in rows[0]
    assert "empty source glob(s): ['/api/app/Live.py']" in rows[0]
