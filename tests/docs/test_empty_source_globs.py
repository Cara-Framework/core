"""Gate: a page bound to a glob that matches nothing is reported by name.

Freshness compares dates; a renamed source leaves the date alone and the page
bound to no code. ``empty_source_globs`` is the structural half a product gate
fails on, and it must judge exactly the pages the report judges.
"""

from __future__ import annotations

from cara.docs import empty_source_globs

from ._fixtures import make_checkout, manifest_for, write


def _page(sources: list[str]) -> str:
    listed = "\n".join(f"  - {s}" for s in sources)
    return f"---\nverified: 2026-01-01\nsources:\n{listed}\n---\n# Page\n"


def test_only_globs_matching_no_file_are_listed(tmp_path):
    root = make_checkout(tmp_path, "solo")
    write(root / "api" / "app" / "Live.py")
    write(root / "api" / "app" / "__pycache__" / "Gone.cpython-314.pyc")
    write(
        root / "docs" / "internal" / "page.md",
        _page(["api/app/Live.py", "api/app/Gone*", "api/app/Renamed.py"]),
    )

    assert empty_source_globs(manifest_for(root, "solo")) == [
        "internal/page.md: api/app/Gone*",
        "internal/page.md: api/app/Renamed.py",
    ]


def test_the_generated_reference_is_not_judged(tmp_path):
    root = make_checkout(tmp_path, "solo")
    manifest = manifest_for(root, "solo")
    write(manifest.reference / "ROUTES.md", _page(["api/nowhere.py"]))

    assert empty_source_globs(manifest) == []
