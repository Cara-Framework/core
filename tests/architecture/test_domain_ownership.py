"""DomainOwnership: cross-domain work enters through the owning service."""

from __future__ import annotations

from cara.architecture.scanners import DomainOwnership

from ._fixtures import make_manifest, write


def _repositories(tmp_path) -> None:
    write(
        tmp_path / "app" / "repositories" / "catalog" / "ProductRepository.py",
        "class ProductRepository:\n    pass\n",
    )
    write(
        tmp_path / "app" / "repositories" / "pricing" / "PriceRepository.py",
        "class PriceRepository:\n    pass\n",
    )


def test_service_may_use_its_own_repository(tmp_path):
    _repositories(tmp_path)
    write(
        tmp_path / "app" / "services" / "catalog" / "ProductService.py",
        "from app.repositories import ProductRepository\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert DomainOwnership.scan(make_manifest(tmp_path)) == []


def test_service_cannot_import_another_domain_repository_from_barrel(tmp_path):
    _repositories(tmp_path)
    write(
        tmp_path / "app" / "services" / "catalog" / "ProductService.py",
        "from app.repositories import PriceRepository\nclass ProductService:\n    pass\n",
    )
    findings = DomainOwnership.scan(make_manifest(tmp_path))
    assert len(findings) == 1
    assert "PriceRepository belongs to domain(s) pricing" in findings[0].message


def test_service_cannot_deep_import_another_domain_repository(tmp_path):
    _repositories(tmp_path)
    write(
        tmp_path / "app" / "services" / "catalog" / "ProductService.py",
        "from app.repositories.pricing.PriceRepository import PriceRepository\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert len(DomainOwnership.scan(make_manifest(tmp_path))) == 1


def test_repository_cannot_reach_another_domain_repository(tmp_path):
    _repositories(tmp_path)
    write(
        tmp_path / "app" / "repositories" / "catalog" / "ProductReadRepository.py",
        "from app.repositories import PriceRepository\n"
        "class ProductReadRepository:\n"
        "    pass\n",
    )
    assert len(DomainOwnership.scan(make_manifest(tmp_path))) == 1


def test_cross_domain_service_call_is_the_legal_route(tmp_path):
    _repositories(tmp_path)
    write(
        tmp_path / "app" / "services" / "catalog" / "ProductService.py",
        "from app.services.pricing.PriceService import PriceService\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert DomainOwnership.scan(make_manifest(tmp_path)) == []


def test_domain_ownership_debt_is_exact_and_shrink_only(tmp_path):
    _repositories(tmp_path)
    path = tmp_path / "app" / "services" / "catalog" / "ProductService.py"
    write(
        path,
        "from app.repositories import PriceRepository\nclass ProductService:\n    pass\n",
    )
    manifest = make_manifest(
        tmp_path,
        seam_allowlists={
            "domain_ownership": {"app/services/catalog/ProductService.py": 1}
        },
    )
    assert DomainOwnership.scan(manifest) == []

    write(
        path,
        "from app.repositories import PriceRepository, ProductRepository\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert DomainOwnership.scan(manifest) == []

    write(
        path,
        "from app.repositories import PriceRepository\n"
        "from app.repositories.pricing import PriceRepository as OtherPriceRepository\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert any(
        "debt grew" in finding.message for finding in DomainOwnership.scan(manifest)
    )

    write(
        path,
        "from app.repositories import ProductRepository\n"
        "class ProductService:\n"
        "    pass\n",
    )
    assert any("stale" in finding.message for finding in DomainOwnership.scan(manifest))
