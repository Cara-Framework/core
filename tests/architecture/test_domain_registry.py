"""DomainRegistry: the mirror rule, registry membership, forbidden names,
flows partition (DOCTRINE §3)."""

from __future__ import annotations

from cara.architecture.scanners import DomainRegistry

from ._fixtures import make_manifest, write


def test_unregistered_layer_folder_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",), domains={})
    write(
        tmp_path / "app" / "services" / "channels" / "Thing.py",
        "class Thing:\n    pass\n",
    )
    findings = DomainRegistry.scan(manifest)
    assert any("not a DOMAINS or FLOWS key" in f.message for f in findings)


def test_flow_stage_folder_is_exempt_from_domain_registration(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("jobs",),
        domains={},
        flows={"pipeline": "Catalog enrichment pipeline stages."},
    )
    write(
        tmp_path / "app" / "jobs" / "pipeline" / "Stage1.py", "class Stage1:\n    pass\n"
    )
    findings = DomainRegistry.scan(manifest)
    assert not any("not a DOMAINS or FLOWS key" in f.message for f in findings)


def test_memberless_domain_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path, layers=("services",), domains={"catalog": "Catalog domain."}
    )
    findings = DomainRegistry.scan(manifest)
    assert any("has no member module" in f.message for f in findings)


def test_forbidden_domain_name_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path, layers=("services",), domains={"misc": "Grab bag."}
    )
    write(tmp_path / "app" / "services" / "misc" / "Thing.py", "class Thing:\n    pass\n")
    findings = DomainRegistry.scan(manifest)
    assert any("forbidden domain name" in f.message for f in findings)


def test_missing_universal_domain_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services",),
        domains={"catalog": "Catalog domain."},
        universal_domains=frozenset({"user"}),
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    findings = DomainRegistry.scan(manifest)
    assert any("universal domain 'user'" in f.message for f in findings)


def test_loose_layer_root_file_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path, layers=("services",), domains={"catalog": "Catalog domain."}
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    write(tmp_path / "app" / "services" / "Loose.py", "class Loose:\n    pass\n")
    findings = DomainRegistry.scan(manifest)
    assert any("loose module" in f.message for f in findings)


def test_allowlisted_loose_layer_root_file_passes(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services",),
        domains={"catalog": "Catalog domain."},
        domain_layer_root_allowlist=frozenset({"services/BaseService.py"}),
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    write(
        tmp_path / "app" / "services" / "BaseService.py", "class BaseService:\n    pass\n"
    )
    findings = DomainRegistry.scan(manifest)
    assert not any("loose module" in f.message for f in findings)


def test_stale_allowlist_entry_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services",),
        domains={"catalog": "Catalog domain."},
        domain_layer_root_allowlist=frozenset({"services/Ghost.py"}),
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    findings = DomainRegistry.scan(manifest)
    assert any("no longer exists" in f.message for f in findings)


def test_blank_charter_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",), domains={"catalog": "   "})
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    findings = DomainRegistry.scan(manifest)
    assert any("no real charter" in f.message for f in findings)


def test_clean_registry_passes(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services",),
        domains={"catalog": "Catalog domain.", "user": "Identity/account domain."},
        universal_domains=frozenset({"user"}),
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )
    write(tmp_path / "app" / "services" / "user" / "Thing.py", "class Thing:\n    pass\n")
    assert DomainRegistry.scan(manifest) == []


def test_cross_cutting_layer_is_not_treated_as_a_domain_layer(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services", "support"),
        domain_layers=("services",),
        domains={"catalog": "Catalog domain."},
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py",
        "class Thing:\n    pass\n",
    )
    write(
        tmp_path / "app" / "support" / "http" / "Client.py",
        "class Client:\n    pass\n",
    )
    assert DomainRegistry.scan(manifest) == []


# ── registry shape (folded in from a product-only hand-written test) ──


def _two_layer(tmp_path, domains, **overrides):
    """A manifest over two domain layers, with a member module per domain."""
    manifest = make_manifest(
        tmp_path,
        layers=("services", "repositories"),
        domains=domains,
        **overrides,
    )
    for name in {**domains, **overrides.get("flows", {})}:
        for layer in ("services", "repositories"):
            write(
                tmp_path / "app" / layer / name / "Thing.py", "class Thing:\n    pass\n"
            )
    return manifest


def test_unsorted_registry_keys_are_a_finding(tmp_path):
    manifest = _two_layer(
        tmp_path, {"user": "User domain.", "catalog": "Catalog domain."}
    )

    assert any("not alphabetical" in f.message for f in DomainRegistry.scan(manifest))


def test_a_key_that_is_not_a_lowercase_identifier_is_a_finding(tmp_path):
    manifest = _two_layer(tmp_path, {"Catalog": "Catalog domain."})

    assert any(
        "not a lowercase identifier" in f.message for f in DomainRegistry.scan(manifest)
    )


def test_a_name_in_both_registries_is_a_finding(tmp_path):
    manifest = _two_layer(
        tmp_path,
        {"catalog": "Catalog domain."},
        flows={"catalog": "Catalog pipeline stage."},
    )

    assert any(
        "BOTH a domain and a flow stage" in f.message
        for f in DomainRegistry.scan(manifest)
    )


def test_an_unpinned_single_layer_domain_is_a_finding(tmp_path):
    manifest = _two_layer(tmp_path, {"catalog": "Catalog domain."})
    (tmp_path / "app" / "repositories" / "catalog" / "Thing.py").unlink()
    (tmp_path / "app" / "repositories" / "catalog").rmdir()

    assert any(
        "exists in one domain layer only" in f.message
        for f in DomainRegistry.scan(manifest)
    )


def test_pinning_that_single_layer_domain_clears_it(tmp_path):
    manifest = _two_layer(
        tmp_path,
        {"catalog": "Catalog domain."},
        single_layer_domains=frozenset({"catalog"}),
    )
    (tmp_path / "app" / "repositories" / "catalog" / "Thing.py").unlink()
    (tmp_path / "app" / "repositories" / "catalog").rmdir()

    assert DomainRegistry.scan(manifest) == []


def test_a_pin_whose_domain_grew_a_second_layer_is_stale(tmp_path):
    manifest = _two_layer(
        tmp_path,
        {"catalog": "Catalog domain."},
        single_layer_domains=frozenset({"catalog"}),
    )

    assert any(
        "stale single_layer_domains pin" in f.message
        for f in DomainRegistry.scan(manifest)
    )


def test_registry_size_is_unenforced_until_a_product_declares_a_budget(tmp_path):
    domains = {name: f"{name} domain." for name in ("a", "b", "c")}

    assert not any(
        "budget" in f.message for f in DomainRegistry.scan(_two_layer(tmp_path, domains))
    )

    manifest = _two_layer(tmp_path, domains, registry_size_bounds=(6, 14))
    assert any("budget of 6-14" in f.message for f in DomainRegistry.scan(manifest))


def test_a_pin_is_meaningless_when_the_deployable_has_one_domain_layer(tmp_path):
    manifest = make_manifest(
        tmp_path,
        layers=("services",),
        domains={"catalog": "Catalog domain."},
        single_layer_domains=frozenset({"catalog"}),
    )
    write(
        tmp_path / "app" / "services" / "catalog" / "Thing.py", "class Thing:\n    pass\n"
    )

    assert any(
        "fewer than two domain layers" in f.message for f in DomainRegistry.scan(manifest)
    )
