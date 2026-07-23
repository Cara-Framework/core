"""ImportForm: barrel-for-consumers / direct-for-siblings / kernel-via-app-barrels."""

from __future__ import annotations

from cara.architecture.scanners import ImportForm

from ._fixtures import make_manifest, write


def _layered_manifest(tmp_path):
    return make_manifest(tmp_path, layers=("controllers", "services"))


def test_deep_leaf_import_from_outside_a_layer_fails(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "channels" / "__init__.py",
        '"""Domain."""\n\n__all__ = ["ChannelService"]\n',
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ChannelService.py",
        "class ChannelService:\n    pass\n",
    )
    write(
        tmp_path / "app" / "controllers" / "Foo.py",
        "from app.services.channels.ChannelService import ChannelService\n",
    )
    findings = ImportForm.scan(manifest)
    assert any("deep import" in f.message for f in findings)


def test_domain_barrel_import_from_outside_a_layer_passes(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "channels" / "__init__.py",
        '"""Domain."""\n\n__all__ = ["ChannelService"]\n',
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ChannelService.py",
        "class ChannelService:\n    pass\n",
    )
    write(
        tmp_path / "app" / "controllers" / "Bar.py",
        "from app.services.channels import ChannelService\n",
    )
    assert ImportForm.scan(manifest) == []


def test_module_object_contract_exempts_deep_import(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "channels" / "__init__.py",
        '"""Domain."""\n\nfrom . import ChannelService\n\n__all__ = ["ChannelService"]\n',
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ChannelService.py",
        "SOME_CONST = 1\n",
    )
    write(
        tmp_path / "app" / "controllers" / "Baz.py",
        "from app.services.channels.ChannelService import SOME_CONST\n",
    )
    assert ImportForm.scan(manifest) == []


def test_sibling_importing_own_layer_barrel_fails(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n__all__: list[str] = []\n',
    )
    write(
        tmp_path / "app" / "services" / "channels" / "Something.py",
        "from app.services import Foo\n",
    )
    findings = ImportForm.scan(manifest)
    assert any("own layer barrel" in f.message for f in findings)


def test_kernel_reached_directly_outside_the_four_barrels_fails(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "controllers" / "Straggler.py",
        "from commons.gates import PERMISSIONS\n",
    )
    findings = ImportForm.scan(manifest)
    assert any("app.* barrels" in f.message for f in findings)


def test_kernel_barrel_itself_may_import_commons(tmp_path):
    manifest = _layered_manifest(tmp_path)
    write(
        tmp_path / "app" / "gates" / "__init__.py",
        'from commons.gates import PERMISSIONS\n\n__all__ = ["PERMISSIONS"]\n',
    )
    write(
        tmp_path / "app" / "controllers" / "Fine.py",
        "from app.gates import PERMISSIONS\n",
    )
    assert ImportForm.scan(manifest) == []
