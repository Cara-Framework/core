from __future__ import annotations

from dataclasses import replace

from cara.architecture.scanners.TransactionOwnership import TransactionOwnership

from ._fixtures import make_manifest, write


def test_transport_edge_may_not_open_transaction(tmp_path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
from cara.facades import DB

class RefreshJob:
    def handle(self):
        with DB.transaction():
            pass
""",
    )

    findings = TransactionOwnership.scan(make_manifest(tmp_path))

    assert len(findings) == 1
    assert "transaction-ownership violation" in findings[0].message


def test_repository_may_not_commit_or_open_transaction(tmp_path):
    write(
        tmp_path / "app/repositories/ProductRepository.py",
        """
from cara.facades import DB

class ProductRepository:
    def save(self):
        with DB.transaction():
            DB.commit()
""",
    )

    findings = TransactionOwnership.scan(make_manifest(tmp_path))

    assert len(findings) == 1
    assert "2 transaction-ownership violation" in findings[0].message


def test_db_alias_and_connection_lifecycle_calls_are_detected(tmp_path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
from cara.facades import DB as Database

class RefreshJob:
    def handle(self):
        Database.begin_transaction()
        Database.connection().commit()
        Database.commit_open_transactions()
""",
    )

    findings = TransactionOwnership.scan(make_manifest(tmp_path))

    assert len(findings) == 1
    assert "3 transaction-ownership violation" in findings[0].message


def test_use_case_service_owns_transaction(tmp_path):
    write(
        tmp_path / "app/services/ProductService.py",
        """
from cara.facades import DB

class ProductService:
    def save(self):
        with DB.transaction():
            pass
""",
    )

    assert TransactionOwnership.scan(make_manifest(tmp_path)) == []


def test_declared_atomic_repository_method_is_exact_and_stale_checked(tmp_path):
    write(
        tmp_path / "app/repositories/LeaseRepository.py",
        """
from cara.facades import DB

class LeaseRepository:
    def claim(self):
        with DB.transaction():
            pass
""",
    )
    manifest = replace(
        make_manifest(tmp_path),
        atomic_repository_methods=frozenset(
            {"app/repositories/LeaseRepository.py::LeaseRepository.claim"}
        ),
    )

    assert TransactionOwnership.scan(manifest) == []

    stale = replace(
        manifest,
        atomic_repository_methods=frozenset(
            {"app/repositories/LeaseRepository.py::LeaseRepository.release"}
        ),
    )
    findings = TransactionOwnership.scan(stale)
    assert any("stale atomic_repository_methods" in item.message for item in findings)


def test_transaction_debt_is_exact_shrink_only(tmp_path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
from cara.facades import DB

class RefreshJob:
    def handle(self):
        with DB.transaction():
            pass
""",
    )
    manifest = replace(
        make_manifest(tmp_path),
        seam_allowlists={"transaction_ownership": {"app/jobs/RefreshJob.py": 1}},
    )

    assert TransactionOwnership.scan(manifest) == []

    stale = replace(
        manifest,
        seam_allowlists={"transaction_ownership": {"app/jobs/RefreshJob.py": 2}},
    )
    findings = TransactionOwnership.scan(stale)
    assert len(findings) == 1
    assert "stale transaction-ownership pin" in findings[0].message
