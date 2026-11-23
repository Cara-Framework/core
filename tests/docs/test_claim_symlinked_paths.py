"""Gate: a path that exists as written is not "path does not exist".

A deployable sees its kernel through a symlink — ``api/commons -> ../commons``,
``api/cara -> ../commons/cara/cara`` — and the basename index does not descend
into a symlinked directory. A doc that cited a file the way the deployable
imports it (``api/commons/models/Product.py``) was therefore BROKEN, failing
the gate on a path that opens in every editor (audit 2026-11-23). The
verifier's first rule is zero false positives.

The literal check certifies only what the doc literally names: no suffix
match, so the neighbour and snapshot rules — a copy elsewhere proves nothing —
are untouched. The second half pins the neighbour's command inventory being
read once, not once per claim.
"""

from __future__ import annotations

import importlib
import os

import pytest

from cara.docs import check_path_claim, verify_claims
from cara.docs.ClaimSources import forget_path_index

from ._fixtures import make_checkout, manifest_for, write

# By name: ``cara.docs.Claims`` is a regular submodule the barrel does not
# re-export, so the patch below lands on the module ``verify_claims`` reads.
claims_module = importlib.import_module("cara.docs.Claims")


def _linked_kernel(root):
    write(root / "commons" / "models" / "Product.py")
    (root / "api").mkdir(parents=True, exist_ok=True)
    os.symlink("../commons", root / "api" / "commons")


def test_a_path_through_a_symlinked_directory_resolves(tmp_path):
    forget_path_index()
    _linked_kernel(tmp_path)

    assert check_path_claim("commons/models/Product.py", [tmp_path]) == ("ok", "")
    assert check_path_claim("api/commons/models/Product.py", [tmp_path]) == ("ok", "")
    assert check_path_claim("api/commons/models/", [tmp_path]) == ("ok", "")


def test_a_missing_file_below_the_symlink_is_still_broken(tmp_path):
    forget_path_index()
    _linked_kernel(tmp_path)

    assert check_path_claim("api/commons/models/Gone.py", [tmp_path]) == (
        "broken",
        "path does not exist",
    )


def test_a_literal_path_never_certifies_a_copy_elsewhere(tmp_path):
    # The file exists, but not where the doc says: a pruned snapshot
    # directory is only reachable by naming it.
    forget_path_index()
    write(tmp_path / "api" / "app" / "Live.py")
    write(tmp_path / "storage" / "snapshot" / "api" / "app" / "Deleted.py")

    assert check_path_claim("api/app/Deleted.py", [tmp_path]) == (
        "broken",
        "path does not exist",
    )
    assert check_path_claim("api/../storage/snapshot/api/app/Deleted.py", [tmp_path]) == (
        "broken",
        "path does not exist",
    )


def test_a_doc_citing_the_kernel_the_way_a_deployable_sees_it_passes(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    _linked_kernel(alpha)
    write(
        alpha / "docs" / "internal" / "note.md",
        "The model lives in `api/commons/models/Product.py`.\n",
    )

    broken, _unverifiable = verify_claims(manifest_for(alpha, "alpha"), lambda _l: None)

    assert broken == []


def test_a_neighbours_command_inventory_is_read_once(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    beta = make_checkout(tmp_path, "beta")
    write(
        beta / "services" / "app" / "commands" / "Sync.py",
        'class Sync(CommandBase):\n    name = "beta:sync"\n',
    )
    write(
        alpha / "docs" / "internal" / "note.md",
        "beta.example runs `craft beta:sync` nightly.\n\n"
        "Operators can also run craft beta:sync by hand.\n\n"
        "And craft beta:sync once more after a deploy.\n",
    )
    read: list[str] = []
    real = claims_module.command_names

    def counting(root):
        read.append(root.parent.name)
        return real(root)

    monkeypatch.setattr(claims_module, "command_names", counting)

    broken, unverifiable = verify_claims(manifest_for(alpha, "alpha"), lambda _l: None)

    # Three claims about the neighbour's command: one read of its tree. It
    # used to be re-read for every claim, and the result thrown away.
    assert read.count("beta.example") == 1
    assert broken == []
    assert [row[4] for row in unverifiable if row[2] == "command"] == [
        "belongs to beta.example, not alpha"
    ] * 3
