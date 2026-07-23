"""PortMembership: a port needs >=2 implementors or a '# port: <reason>' tag."""

from __future__ import annotations

from cara.architecture.scanners import PortMembership

from ._fixtures import make_manifest, write


def test_single_implementor_untagged_port_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path, layers=("ports", "services"))
    write(
        tmp_path / "app" / "ports" / "channels" / "AccessContract.py",
        "class AccessContract:\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "channels" / "OnlyImpl.py",
        "from app.ports.channels.AccessContract import AccessContract\n\n\n"
        "class OnlyImpl(AccessContract):\n    pass\n",
    )
    findings = PortMembership.scan(manifest)
    assert findings and "1 implementor" in findings[0].message


def test_two_implementors_pass(tmp_path):
    manifest = make_manifest(tmp_path, layers=("ports", "services"))
    write(
        tmp_path / "app" / "ports" / "channels" / "AccessContract.py",
        "class AccessContract:\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ImplA.py",
        "from app.ports.channels.AccessContract import AccessContract\n\n\n"
        "class ImplA(AccessContract):\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "channels" / "ImplB.py",
        "from app.ports.channels.AccessContract import AccessContract\n\n\n"
        "class ImplB(AccessContract):\n    pass\n",
    )
    assert PortMembership.scan(manifest) == []


def test_documented_single_implementor_port_passes(tmp_path):
    manifest = make_manifest(tmp_path, layers=("ports", "services"))
    write(
        tmp_path / "app" / "ports" / "channels" / "AccessContract.py",
        "# port: single external-system edge, swap is hypothetical only\n"
        "class AccessContract:\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "channels" / "OnlyImpl.py",
        "from app.ports.channels.AccessContract import AccessContract\n\n\n"
        "class OnlyImpl(AccessContract):\n    pass\n",
    )
    assert PortMembership.scan(manifest) == []


def test_absent_ports_layer_noops(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "channels" / "Thing.py",
        "class Thing:\n    pass\n",
    )
    assert PortMembership.scan(manifest) == []


def test_supporting_value_class_in_port_module_is_not_itself_a_port(tmp_path):
    manifest = make_manifest(tmp_path, layers=("ports", "services"))
    write(
        tmp_path / "app" / "ports" / "catalog" / "AccessContract.py",
        "class AccessResult:\n    pass\n\n\n"
        "# port: consumer-owned persistence boundary\n"
        "class AccessContract:\n    pass\n",
    )
    assert PortMembership.scan(manifest) == []
