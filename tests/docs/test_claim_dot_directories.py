"""Gate: a real CI workflow path is a real path.

The basename index pruned EVERY dot-directory, so `api/.github/workflows/ci.yml`
read "path does not exist" in a checkout that carries it — a correct sentence
failed the gate. Tracked config directories are indexed now; caches, tool state
and snapshot copies stay pruned, because the index matches by suffix and a stale
copy would certify a claim the live tree no longer satisfies.
"""

from __future__ import annotations

from cara.docs.ClaimSources import check_path_claim, forget_path_index

from ._fixtures import write


def test_a_tracked_workflow_path_resolves(tmp_path):
    forget_path_index()
    write(tmp_path / "api" / ".github" / "workflows" / "ci.yml")

    assert check_path_claim("api/.github/workflows/ci.yml", [tmp_path]) == ("ok", "")
    assert check_path_claim("api/.github/workflows/gone.yml", [tmp_path]) == (
        "broken",
        "path does not exist",
    )


def test_a_root_level_workflow_path_is_judged_not_skipped(tmp_path):
    forget_path_index()
    write(tmp_path / ".github" / "workflows" / "ci.yml")

    assert check_path_claim(".github/workflows/ci.yml", [tmp_path]) == ("ok", "")
    assert check_path_claim(".github/workflows/gone.yml", [tmp_path]) == (
        "broken",
        "path does not exist",
    )


def test_a_snapshot_dot_directory_never_certifies_a_claim(tmp_path):
    forget_path_index()
    write(tmp_path / "api" / "app" / "Live.py")
    write(tmp_path / ".snap1" / "api" / "app" / "Deleted.py")

    assert check_path_claim("api/app/Live.py", [tmp_path]) == ("ok", "")
    assert check_path_claim("api/app/Deleted.py", [tmp_path]) == (
        "broken",
        "path does not exist",
    )


def test_a_dotted_host_is_still_prose(tmp_path):
    forget_path_index()
    write(tmp_path / "api" / "app" / "Live.py")

    assert check_path_claim("example.com/api/app.py", [tmp_path]) is None
