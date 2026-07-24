from __future__ import annotations

from dataclasses import replace

from cara.architecture.scanners.WriteOwnership import WriteOwnership

from ._fixtures import make_manifest, write


def _manifest(tmp_path, *, deployable="api", ownership=None, debt=None):
    write(
        tmp_path / "commons/models/Product.py",
        'class Product:\n    __table__ = "product"\n',
    )
    return replace(
        make_manifest(tmp_path, deployable=deployable),
        write_ownership=ownership or {"product": "api-owned"},
        seam_allowlists={} if debt is None else {"write_ownership": debt},
    )


def test_model_tables_require_valid_declared_owner(tmp_path):
    missing = replace(_manifest(tmp_path), write_ownership={})
    findings = WriteOwnership.scan(missing)
    assert any("has no write owner" in item.message for item in findings)

    invalid = replace(_manifest(tmp_path), write_ownership={"product": "both"})
    findings = WriteOwnership.scan(invalid)
    assert any("invalid write owner" in item.message for item in findings)


def test_owner_deployable_may_write_model(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        "from app.models import Product\nProduct.create({})\n",
    )
    assert WriteOwnership.scan(_manifest(tmp_path)) == []


def test_other_deployable_model_write_is_a_finding(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        "from app.models import Product\nProduct.where('id', 1).update({'x': 1})\n",
    )
    findings = WriteOwnership.scan(_manifest(tmp_path, deployable="services"))
    assert any("cross-owner write" in item.message for item in findings)


def test_model_alias_and_typed_or_assigned_instances_are_detected(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        """
from app.models import Product as ProductModel

def persist(product: ProductModel):
    product.save()
    loaded = ProductModel.find(1)
    loaded.delete()
""",
    )

    findings = WriteOwnership.scan(_manifest(tmp_path, deployable="services"))

    assert len(findings) == 1
    assert "2 cross-owner write" in findings[0].message


def test_literal_sql_and_query_builder_writes_are_detected(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        "from cara.facades import DB\n"
        "DB.statement('UPDATE product SET title = %s', ['x'])\n"
        "DB.table('product').delete()\n",
    )
    findings = WriteOwnership.scan(_manifest(tmp_path, deployable="services"))
    assert len(findings) == 1
    assert "2 cross-owner write" in findings[0].message


def test_shared_gate_owner_writes_only_through_gate_persistence(tmp_path):
    ownership = {"product": "shared-gate-owned"}
    write(
        tmp_path / "commons/gates/persistence/ProductWriter.py",
        "from app.models import Product\nProduct.create({})\n",
    )
    assert WriteOwnership.scan(_manifest(tmp_path, ownership=ownership)) == []

    write(
        tmp_path / "app/repositories/ProductRepository.py",
        "from app.models import Product\nProduct.create({})\n",
    )
    findings = WriteOwnership.scan(_manifest(tmp_path, ownership=ownership))
    assert any("cross-owner write" in item.message for item in findings)


def test_cross_owner_debt_is_exact_shrink_only(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        "from app.models import Product\nProduct.create({})\n",
    )
    identity = "app/repositories/ProductRepository.py::product"
    manifest = _manifest(
        tmp_path,
        deployable="services",
        debt={identity: 1},
    )
    assert WriteOwnership.scan(manifest) == []

    stale = replace(
        manifest,
        seam_allowlists={"write_ownership": {identity: 2}},
    )
    findings = WriteOwnership.scan(stale)
    assert any("stale write-ownership pin" in item.message for item in findings)
