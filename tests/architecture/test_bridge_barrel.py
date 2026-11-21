"""BridgeBarrel: ``app/<kernel-pkg>`` bridges are generated from the kernel (§2)."""

from __future__ import annotations

import ast

from cara.architecture.BarrelGenerator import BarrelGenerator

from ._fixtures import make_manifest, write


def _all_of(path) -> list[str]:
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
        ):
            return [e.value for e in node.value.elts]
    raise AssertionError(f"{path} has no __all__")


def _manifest(tmp_path):
    # The kernel fixtures below hand-state their ``__all__``; keep the kernel
    # walk off so only the bridges are generated (as in a services twin).
    return make_manifest(tmp_path, kernel_barrel_packages=frozenset())


def _kernel(tmp_path, pkg: str, names: list[str]) -> None:
    listed = "".join(f'    "{n}",\n' for n in names)
    write(
        tmp_path / "commons" / pkg / "__init__.py",
        f'"""Kernel {pkg}."""\n\n__all__ = [\n{listed}]\n',
    )


def test_bridge_is_the_sorted_kernel_surface_and_drift_fails_check(tmp_path):
    manifest = _manifest(tmp_path)
    _kernel(tmp_path, "gates", ["Zeta", "ALPHA", "persistence", "beta"])
    # A hand-listed bridge: two blocks, an unsorted __all__, a missing name.
    write(
        tmp_path / "app" / "gates" / "__init__.py",
        '"""Gates bridge, worded by this deployable."""\n\n'
        "from commons.gates import beta\nfrom commons.gates import Zeta\n\n"
        '__all__ = ["beta", "Zeta"]\n',
    )

    drift = BarrelGenerator.check(manifest)
    assert "app/gates/__init__.py" in drift.changed

    BarrelGenerator.write(manifest)
    bridge = tmp_path / "app" / "gates" / "__init__.py"
    source = bridge.read_text()
    assert _all_of(bridge) == ["ALPHA", "Zeta", "beta"]
    assert source.startswith('"""Gates bridge, worded by this deployable."""')
    assert source.count("from commons.gates import") == 1
    # gates/persistence is kernel-internal: neither exported nor mirrored.
    assert '_alias_kernel(_kernel, __name__, skip=("persistence",))' in source
    assert "app/gates/__init__.py" not in BarrelGenerator.check(manifest).changed


def test_flattened_models_bridge_is_not_mirrored(tmp_path):
    manifest = _manifest(tmp_path)
    _kernel(tmp_path, "models", ["Listing"])
    BarrelGenerator.write(manifest)
    source = (tmp_path / "app" / "models" / "__init__.py").read_text()
    assert "from commons.models import Listing" in source
    assert "_alias_kernel" not in source
    assert BarrelGenerator.write(manifest).changed == []


def test_empty_kernel_package_needs_no_bridge(tmp_path):
    manifest = _manifest(tmp_path)
    BarrelGenerator.write(manifest)
    assert not (tmp_path / "app" / "contracts" / "__init__.py").exists()
