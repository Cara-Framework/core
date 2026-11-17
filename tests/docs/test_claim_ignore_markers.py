"""Gate: QUOTING an ignore marker does not switch the verifier off.

The docs README explains the markers in backticks. Read raw, that sentence
carried ``docs-check: ignore-file``, so the very page that documents the gate
was skipped whole and its stale paths stayed green. A marker counts only where
it is written as a marker — outside inline code and fenced blocks.
"""

from __future__ import annotations

from cara.docs import verify_claims
from cara.docs.Claims import ignores_file, marked
from cara.docs.ClaimSources import forget_path_index

from ._fixtures import make_checkout, manifest_for, write


def test_a_quoted_marker_is_not_a_marker():
    assert not marked("mark it with `<!-- docs-check: ignore -->`", "docs-check: ignore")
    assert marked("`a/b.py` example <!-- docs-check: ignore -->", "docs-check: ignore")
    assert not ignores_file("or carry `<!-- docs-check: ignore-file -->` anywhere\n")
    assert not ignores_file("```\n<!-- docs-check: ignore-file -->\n```\n")
    assert ignores_file("# Page\n<!-- docs-check: ignore-file -->\n")


def test_a_page_that_documents_the_markers_is_still_checked(tmp_path):
    forget_path_index()
    root = make_checkout(tmp_path, "solo")
    write(
        root / "docs" / "README.md",
        "Cites `api/app/Gone.py`.\n"
        "Skip a page with `<!-- docs-check: ignore-file -->`.\n"
        "Example `api/app/Example.py` <!-- docs-check: ignore -->\n",
    )

    broken, _ = verify_claims(manifest_for(root, "solo"), lambda _line: None)

    paths = [
        (label, number, claim)
        for label, number, kind, claim, _ in broken
        if kind == "path"
    ]
    assert paths == [("docs/README.md", 1, "api/app/Gone.py")]
