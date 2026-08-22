"""Gate: a neighbouring checkout may not make THIS product's claims come true.

The claim verifier resolves a doc's backticked paths against the product it
belongs to and — because a shared atlas legitimately describes more than one
checkout — also against a neighbour the doc names. That second root was once
applied at DOCUMENT granularity and counted as proof: any path that existed
next door came back green.

It cost a real claim. An atlas said a registry was guarded by a test file this
product does not carry; the sentence was false for weeks and the verifier
reported it green, because a file of that name existed in the neighbour and the
neighbour was mentioned elsewhere in the same document. Green-by-neighbour is
the docs equivalent of green-by-omission.

The rule pinned here is the one the command check already used: a neighbour can
only DOWNGRADE a verdict, never certify it. Absent here + present there is
`unverifiable`, which reports without failing the gate; absent in both stays
`broken`; present here stays `ok`.
"""

from __future__ import annotations

from pathlib import Path

from cara.docs import DocsManifest, check_path_claim, sibling_roots, verify_claims
from cara.docs.ClaimSources import _owned_markdowns, forget_path_index

from ._fixtures import make_checkout, manifest_for, write


def test_a_path_present_only_in_a_neighbour_is_never_ok(tmp_path):
    forget_path_index()
    here, there = tmp_path / "here", tmp_path / "there"
    write(there / "api" / "tests" / "test_registry.py")
    here.mkdir(parents=True, exist_ok=True)

    assert check_path_claim("tests/test_registry.py", [here]) == (
        "broken",
        "path does not exist",
    )
    assert check_path_claim("tests/test_registry.py", [there]) == ("ok", "")


def test_a_path_present_here_stays_ok(tmp_path):
    forget_path_index()
    here = tmp_path / "here"
    write(here / "api" / "tests" / "test_architecture.py")

    assert check_path_claim("tests/test_architecture.py", [here]) == ("ok", "")


def test_a_node_modules_path_can_never_be_proven_or_disproven(tmp_path):
    """A dependency tree is present on a machine that installed and absent on
    one that did not, and the basename index deliberately skips it. Failing a
    correct sentence there would train people to ignore the whole report."""
    forget_path_index()
    here = tmp_path / "here"
    write(here / "package.json", "{}")

    assert check_path_claim("dashboard/node_modules/next/dist/index.js", [here]) == (
        "unverifiable",
        "inside node_modules; not indexed",
    )


def test_neighbours_are_recognised_by_workspace_shape_not_by_name(tmp_path):
    """Naming the neighbour would put one product's vocabulary in the other's
    tooling — the coupling this engine was extracted to remove."""
    alpha = make_checkout(tmp_path, "alpha")
    beta = make_checkout(tmp_path, "beta")

    assert sibling_roots(alpha) == [beta]
    assert sibling_roots(beta) == [alpha]


def test_a_claim_resolving_only_next_door_is_downgraded_not_failed(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha", atlas="`some:command` is FORBIDDEN.\n")
    beta = make_checkout(tmp_path, "beta", atlas="`some:command` is FORBIDDEN.\n")
    write(beta / "api" / "tests" / "test_registry.py")
    write(
        alpha / "docs" / "internal" / "note.md",
        "The registry is guarded by `tests/test_registry.py` in beta.example.\n",
    )
    manifest = manifest_for(alpha, "alpha")

    broken, unverifiable = verify_claims(manifest, lambda _line: None)

    assert [row[3] for row in broken] == []
    assert ("tests/test_registry.py", "resolves in beta.example, not alpha") in [
        (row[3], row[4]) for row in unverifiable
    ]


def test_a_claim_absent_everywhere_still_fails(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    make_checkout(tmp_path, "beta")
    write(
        alpha / "docs" / "internal" / "note.md",
        "The registry is guarded by `tests/test_nowhere.py` in beta.example.\n",
    )
    manifest = manifest_for(alpha, "alpha")

    broken, _unverifiable = verify_claims(manifest, lambda _line: None)

    assert ("tests/test_nowhere.py", "path does not exist") in [
        (row[3], row[4]) for row in broken
    ]


def test_a_citation_of_an_ABSENT_neighbour_is_unverifiable_not_broken(tmp_path):
    """CI checks out ONE product. The neighbour's tree is simply not there.

    Locally the shape finds it and the verdict reads "resolves in <product>";
    remotely the same sentence used to flip to BROKEN — a verdict about the
    environment rather than about the sentence. A product that DECLARES its
    neighbours keeps the honest answer in both places.
    """
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")  # no neighbour beside it
    write(
        alpha / "docs" / "internal" / "note.md",
        "The bridge lives in `api/app/support/auth/Access.py` in beta.example.\n",
    )
    manifest = manifest_for(alpha, "alpha", sibling_products=("beta.example",))

    broken, unverifiable = verify_claims(manifest, lambda _line: None)

    assert [row[3] for row in broken] == []
    assert (
        "api/app/support/auth/Access.py",
        "names beta.example, not checked out here",
    ) in [(row[3], row[4]) for row in unverifiable]


def test_an_undeclared_neighbour_does_not_excuse_a_bad_path(tmp_path):
    """The escape hatch is the DECLARED list, not "mentions any name"."""
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    write(
        alpha / "docs" / "internal" / "note.md",
        "The bridge lives in `api/app/support/auth/Access.py` in gamma.example.\n",
    )
    manifest = manifest_for(alpha, "alpha", sibling_products=("beta.example",))

    broken, _unverifiable = verify_claims(manifest, lambda _line: None)

    assert ("api/app/support/auth/Access.py", "path does not exist") in [
        (row[3], row[4]) for row in broken
    ]


def test_a_present_neighbour_still_answers_for_itself(tmp_path):
    """Declaring a sibling must not blind the verifier when it IS checked out:
    a path that resolves next door still says so, by name."""
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    beta = make_checkout(tmp_path, "beta")
    write(beta / "api" / "tests" / "test_registry.py")
    write(
        alpha / "docs" / "internal" / "note.md",
        "The registry is guarded by `tests/test_registry.py` in beta.example.\n",
    )
    manifest = manifest_for(alpha, "alpha", sibling_products=("beta.example",))

    broken, unverifiable = verify_claims(manifest, lambda _line: None)

    assert [row[3] for row in broken] == []
    assert ("tests/test_registry.py", "resolves in beta.example, not alpha") in [
        (row[3], row[4]) for row in unverifiable
    ]


def test_a_workspace_pointer_is_unverifiable_not_broken(tmp_path):
    """``~/Desktop/<product>/…`` describes a LAYOUT, not a file in a checkout.

    The operator's backups directory sits beside the code, not inside it, so
    only a machine that holds the workspace can judge the pointer — and CI
    clones one repository into a path of its own choosing.
    """
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    write(
        alpha / "docs" / "internal" / "note.md",
        "Backups live under `~/Desktop/alpha.example/backups`, and the rest is "
        "in `~/Desktop/beta.example/code/docs/internal/backlog.md`.\n",
    )
    manifest = manifest_for(alpha, "alpha", sibling_products=("beta.example",))

    broken, unverifiable = verify_claims(manifest, lambda _line: None)

    assert [row[3] for row in broken] == []
    reasons = [row[4] for row in unverifiable if row[2] == "pointer"]
    assert len(reasons) == 2
    assert all("only the machine that holds it can verify" in r for r in reasons)


def test_a_home_pointer_naming_no_workspace_still_fails(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    write(
        alpha / "docs" / "internal" / "note.md",
        "Run the script at `~/tools/definitely-not-here.sh` first.\n",
    )
    manifest = manifest_for(alpha, "alpha", sibling_products=("beta.example",))

    broken, _unverifiable = verify_claims(manifest, lambda _line: None)

    assert ("~/tools/definitely-not-here.sh", "does not exist on disk") in [
        (row[3], row[4]) for row in broken
    ]


def test_owned_markdown_covers_the_docs_tree_and_the_neighbours_shared_atlas(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    beta = make_checkout(
        tmp_path, "beta", atlas="`x:y` is FORBIDDEN. alpha shares this atlas.\n"
    )
    write(alpha / "docs" / "internal" / "note.md", "# note\n")
    write(alpha / "docs" / "internal" / "reference" / "routes.md", "# generated\n")

    owned = _owned_markdowns(
        alpha,
        alpha / "docs",
        alpha / "docs" / "internal" / "reference",
        "alpha",
    )

    assert alpha / "docs" / "internal" / "note.md" in owned
    assert alpha / "CLAUDE.md" in owned
    assert beta / "CLAUDE.md" in owned
    # Generated pages are extracted from code and cannot lie, so they are never
    # judged as claims.
    assert alpha / "docs" / "internal" / "reference" / "routes.md" not in owned


def test_a_file_reachable_by_two_routes_is_judged_once(tmp_path):
    """The atlas is a tracked docs file exposed at the root by symlink.

    Three enumerations overlap — the docs tree, the root, and every subtree —
    so the same physical page arrives up to twice. Before deduplication every
    finding it made was reported twice under two different labels, which reads
    as two separate problems and invites a reader to fix one copy.
    """
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    (alpha / "CLAUDE.md").unlink()
    write(alpha / "docs" / "CLAUDE.md", "`x:y` is FORBIDDEN.\n")
    (alpha / "CLAUDE.md").symlink_to(Path("docs") / "CLAUDE.md")
    write(alpha / "docs" / "README.md", "# about these docs\n")

    owned = _owned_markdowns(
        alpha,
        alpha / "docs",
        alpha / "docs" / "internal" / "reference",
        "alpha",
    )

    resolved = [path.resolve() for path in owned]
    assert len(resolved) == len(set(resolved))
    # The root spelling is the one kept: that is where the atlas lives and the
    # label `atlas_bans` already reports.
    assert alpha / "CLAUDE.md" in owned
    assert alpha / "docs" / "CLAUDE.md" not in owned


def test_the_product_name_in_a_verdict_comes_from_the_manifest(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    write(alpha / "docs" / "internal" / "note.md", "Run `craft nothing:here`.\n")
    manifest = DocsManifest(product="whatever-we-say", root=alpha, viewer_port=9999)

    broken, _unverifiable = verify_claims(manifest, lambda _line: None)

    assert any("whatever-we-say" in row[4] for row in broken)
