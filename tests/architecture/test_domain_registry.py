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
