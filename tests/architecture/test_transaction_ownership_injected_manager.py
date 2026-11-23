"""TransactionOwnership counts a transaction opened on an INJECTED manager.

The live defect: only a receiver named ``DB`` (or an alias of the imported
facade) was recognised, so ``self.db.transaction()``, ``db.transaction()`` on
a parameter, ``connection.commit()``, a parameter annotated with the database
manager / connection type, or a name bound to ``make("DB")`` opened the same
transaction at a transport edge and was never reported.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.architecture.scanners.TransactionOwnership import TransactionOwnership

from ._fixtures import make_manifest, write

_INJECTED = {
    "self_db": """
class RefreshJob:
    def __init__(self, db):
        self.db = db

    def handle(self):
        with self.db.transaction():
            pass
""",
    "parameter": """
def handle(db):
    with db.transaction():
        pass
""",
    "connection": """
def handle(connection):
    connection.commit()
""",
    "annotated": """
from cara.eloquent import DatabaseManager

def handle(manager: DatabaseManager):
    with manager.transaction():
        pass
""",
    "container": """
def handle(app):
    manager = app.make("DB")
    with manager.transaction():
        pass
""",
}


@pytest.mark.parametrize("source", _INJECTED.values(), ids=_INJECTED.keys())
def test_an_injected_manager_at_a_transport_edge_is_a_finding(
    tmp_path: Path, source: str
):
    write(tmp_path / "app/jobs/RefreshJob.py", source)

    findings = TransactionOwnership.scan(make_manifest(tmp_path))

    assert len(findings) == 1
    assert "1 transaction-ownership violation" in findings[0].message


def test_a_transaction_on_a_non_database_receiver_is_not_a_finding(tmp_path: Path):
    write(
        tmp_path / "app/jobs/RefreshJob.py",
        """
class RefreshJob:
    def __init__(self, redis):
        self.redis = redis

    def handle(self, pipeline):
        self.redis.transaction(lambda pipe: None)
        pipeline.commit()
""",
    )

    assert TransactionOwnership.scan(make_manifest(tmp_path)) == []
