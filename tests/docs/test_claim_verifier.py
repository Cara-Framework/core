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

from cara.docs import DocsManifest, check_path_claim, sibling_roots, verify_claims
from cara.docs.ClaimSources import forget_path_index, owned_markdowns

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


def test_owned_markdown_covers_the_docs_tree_and_the_neighbours_shared_atlas(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    beta = make_checkout(
        tmp_path, "beta", atlas="`x:y` is FORBIDDEN. alpha shares this atlas.\n"
    )
    write(alpha / "docs" / "internal" / "note.md", "# note\n")
    write(alpha / "docs" / "internal" / "reference" / "routes.md", "# generated\n")

    owned = owned_markdowns(
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


def test_the_product_name_in_a_verdict_comes_from_the_manifest(tmp_path):
    forget_path_index()
    alpha = make_checkout(tmp_path, "alpha")
    write(alpha / "docs" / "internal" / "note.md", "Run `craft nothing:here`.\n")
    manifest = DocsManifest(product="whatever-we-say", root=alpha, viewer_port=9999)

    broken, _unverifiable = verify_claims(manifest, lambda _line: None)

    assert any("whatever-we-say" in row[4] for row in broken)
