"""KernelMembership: direction, purity, single-consumer eviction (DOCTRINE §2)."""

from __future__ import annotations

from cara.architecture.scanners import KernelMembership

from ._fixtures import make_manifest, write


def test_models_importing_gates_is_a_direction_violation(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "commons" / "models" / "User.py",
        "from commons.gates import PERMISSIONS\n\n\nclass User:\n    pass\n",
    )
    findings = KernelMembership.scan(manifest)
    assert any("kernel direction violation" in f.message for f in findings)


def test_models_importing_nothing_else_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "User.py", "class User:\n    pass\n")
    assert KernelMembership.scan(manifest) == []


def test_contracts_may_import_models_but_not_gates(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "commons" / "contracts" / "Envelope.py",
        "from commons.models import User\n\n\nclass Envelope:\n    pass\n",
    )
    assert KernelMembership.scan(manifest) == []

    write(
        tmp_path / "commons" / "contracts" / "Leaky.py",
        "from commons.gates import PERMISSIONS\n",
    )
    findings = KernelMembership.scan(manifest)
    assert any("kernel direction violation" in f.message for f in findings)


def test_direction_allowlist_suppresses_a_pinned_leak(tmp_path):
    manifest = make_manifest(
        tmp_path,
        seam_allowlists={"kernel_direction": {"commons/models/User.py": 1}},
    )
    write(
        tmp_path / "commons" / "models" / "User.py",
        "from commons.gates import PERMISSIONS\n\n\nclass User:\n    pass\n",
    )
    assert KernelMembership.scan(manifest) == []


def test_direction_allowlist_flags_a_stale_pin(tmp_path):
    manifest = make_manifest(
        tmp_path,
        seam_allowlists={"kernel_direction": {"commons/models/User.py": 1}},
    )
    write(tmp_path / "commons" / "models" / "User.py", "class User:\n    pass\n")
    findings = KernelMembership.scan(manifest)
    assert any("stale allowlist pin" in f.message for f in findings)


def test_pure_module_importing_a_side_effect_facade_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        pure_modules=frozenset({"ProfitMath"}),
        side_effect_facade_roots=frozenset({"cara.facades"}),
    )
    write(
        tmp_path / "commons" / "shared" / "ProfitMath.py",
        "from cara.facades import DB\n\n\ndef margin():\n    return DB\n",
    )
    findings = KernelMembership.scan(manifest)
    assert any("side-effect facade" in f.message for f in findings)


def test_pure_module_without_facade_import_passes(tmp_path):
    manifest = make_manifest(
        tmp_path,
        pure_modules=frozenset({"ProfitMath"}),
        side_effect_facade_roots=frozenset({"cara.facades"}),
    )
    write(
        tmp_path / "commons" / "shared" / "ProfitMath.py", "def margin():\n    return 1\n"
    )
    assert KernelMembership.scan(manifest) == []


def test_single_consumer_shared_module_is_a_finding(tmp_path):
    consumer_a = tmp_path / "deployable_a" / "app"
    consumer_b = tmp_path / "deployable_b" / "app"
    write(consumer_a / "services" / "Uses.py", "from app.shared import Fx\n")
    write(consumer_b / "services" / "Other.py", "X = 1\n")
    from dataclasses import replace

    manifest = make_manifest(tmp_path)
    manifest = replace(
        manifest,
        roots=replace(manifest.roots, consumer_app_roots=(consumer_a, consumer_b)),
    )
    write(tmp_path / "commons" / "shared" / "Fx.py", "def convert():\n    return 1\n")
    findings = KernelMembership.scan(manifest)
    assert any("'Fx' is consumed by exactly one" in f.message for f in findings)


def test_single_consumer_allowlist_suppresses_the_finding(tmp_path):
    from dataclasses import replace

    consumer_a = tmp_path / "deployable_a" / "app"
    consumer_b = tmp_path / "deployable_b" / "app"
    write(consumer_a / "services" / "Uses.py", "from app.shared import Fx\n")
    write(consumer_b / "services" / "Other.py", "X = 1\n")
    manifest = make_manifest(tmp_path, single_consumer_allowlist=frozenset({"Fx"}))
    manifest = replace(
        manifest,
        roots=replace(manifest.roots, consumer_app_roots=(consumer_a, consumer_b)),
    )
    write(tmp_path / "commons" / "shared" / "Fx.py", "def convert():\n    return 1\n")
    assert KernelMembership.scan(manifest) == []


def test_single_consumer_check_noops_with_fewer_than_two_consumer_roots(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "shared" / "Fx.py", "def convert():\n    return 1\n")
    assert KernelMembership.scan(manifest) == []
