"""Claims the verifier used to skip, and the command/port rules it had no test for.

* A path cited with lines (`api/app/X.py:42`) made the extension read
  ``.py:42`` and the claim was silently skipped — a deleted file stayed green.
* A page that declares shorthand prefixes ("Evidence paths: P = dashboard/…")
  cites `P/orders/page.tsx`; nothing expanded it, so none of those were read.
* `craft x:y` and port claims carried rules (an unknown namespaced command is
  broken, a bare word never fails, a port never fails) with no test pinning them.
"""

from __future__ import annotations

from cara.docs import verify_claims
from cara.docs.ClaimSources import forget_path_index

from ._fixtures import make_checkout, manifest_for, write


def _verify(tmp_path, note: str):
    forget_path_index()
    root = make_checkout(tmp_path, "alpha")
    write(root / "api" / "app" / "support" / "Gate.py", "a = 1\nb = 2\nc = 3\n")
    write(
        root / "dashboard" / "src" / "app" / "(pages)" / "orders" / "[id]" / "page.tsx",
        "export {}\n" * 10,
    )
    write(
        root / "services" / "app" / "commands" / "SyncVerify.py",
        'class SyncVerify(Command):\n    name = "sync:verify"\n',
    )
    write(root / "api" / "config" / "server.py", 'PORT = env("PORT", 8400)\n')
    write(root / "docs" / "internal" / "note.md", note)
    broken, unverifiable = verify_claims(manifest_for(root, "alpha"), lambda _l: None)
    pick = lambda rows: [(row[2], row[3], row[4]) for row in rows]  # noqa: E731
    return pick(broken), pick(unverifiable)


def test_a_line_numbered_path_to_a_missing_file_is_broken(tmp_path):
    broken, _ = _verify(tmp_path, "See `api/app/support/Gone.py:42`.\n")

    assert ("path", "api/app/support/Gone.py:42", "path does not exist") in broken


def test_a_line_numbered_path_inside_the_file_is_ok(tmp_path):
    broken, unverifiable = _verify(
        tmp_path, "See `api/app/support/Gate.py:2` and `api/app/support/Gate.py:1-3`.\n"
    )

    assert broken == []
    assert [row for row in unverifiable if row[0] == "path"] == []


def test_a_line_past_the_end_is_reported_as_drift_not_failed(tmp_path):
    broken, unverifiable = _verify(tmp_path, "See `api/app/support/Gate.py:3,18-20`.\n")

    assert broken == []
    assert (
        "path",
        "api/app/support/Gate.py:3,18-20",
        "line 20 is past the end (3 lines); line drift",
    ) in unverifiable


def test_shorthand_prefixes_expand_from_the_page_legend(tmp_path):
    note = (
        "Evidence paths: P = dashboard/src/app/(pages) ·\n"
        "A = api/app; everything else is repo-relative.\n"
        "\n"
        "| gap | `P/orders/[id]/page.tsx:4` | `A/support/Gate.py` |\n"
        "| gap | `P/orders/missing.tsx:9` |\n"
    )
    broken, unverifiable = _verify(tmp_path, note)

    assert broken == [("path", "P/orders/missing.tsx:9", "path does not exist")]
    assert [row for row in unverifiable if row[0] == "path"] == []


def test_a_new_legend_replaces_the_previous_one():
    from cara.docs.Claims import expand_claim, path_legends

    legends = path_legends(
        [
            "Prefix: P/ = services/packages/amazon/.",
            "",
            "row",
            "Prefixes: E/ = services/packages/ebay/ · D/ = docs/internal/.",
            "row",
        ]
    )

    assert legends[2] == {"P": "services/packages/amazon"}
    assert legends[4] == {"E": "services/packages/ebay", "D": "docs/internal"}
    assert expand_claim("E/connector/X.py:12-14", legends[4]) == (
        "services/packages/ebay/connector/X.py",
        [12, 14],
        True,
    )


def test_an_unknown_namespaced_command_is_broken_a_known_one_is_not(tmp_path):
    broken, _ = _verify(
        tmp_path, "Run `craft sync:verify`, then `craft sync:verfy --all`.\n"
    )

    assert broken == [("command", "craft sync:verfy", "no such command in alpha")]


def test_a_bare_word_after_craft_is_never_failed(tmp_path):
    broken, unverifiable = _verify(tmp_path, "The craft tooling runs everything.\n")

    assert broken == []
    assert ("command", "craft tooling", "bare word, not a known command") in unverifiable


def test_a_port_is_reported_never_failed(tmp_path):
    broken, unverifiable = _verify(
        tmp_path, "The API listens on :8400; Grafana on port 3005.\n"
    )

    assert broken == []
    ports = [row for row in unverifiable if row[0] == "port"]
    assert ports == [("port", "3005", "not declared by this repo")]
