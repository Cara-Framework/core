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


def test_atomic_context_decorator_and_run_are_detected(tmp_path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
from cara.eloquent import atomic
from cara.eloquent.Atomic import atomic as tx

class RefreshJob:
    def handle(self):
        with atomic():
            pass
        tx(attempts=3).run(lambda: None)

    @atomic()
    def other(self):
        pass
""",
    )

    findings = TransactionOwnership.scan(make_manifest(tmp_path))

    assert len(findings) == 1
    assert "3 transaction-ownership violation" in findings[0].message
    assert "atomic() in app/jobs/RefreshJob.py::RefreshJob.handle" in findings[0].message


def test_a_local_name_called_atomic_is_not_cara_atomic(tmp_path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
def atomic():
    return None

class RefreshJob:
    def handle(self):
        atomic()
""",
    )

    assert TransactionOwnership.scan(make_manifest(tmp_path)) == []


def test_use_case_service_owns_atomic(tmp_path):
    write(
        tmp_path / "app/services/ProductService.py",
        """
from cara.eloquent import atomic

class ProductService:
    def save(self):
        with atomic():
            pass
""",
    )

    assert TransactionOwnership.scan(make_manifest(tmp_path)) == []
