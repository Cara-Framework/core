"""The forms ``docs/GUARDPACK.md`` documents are the forms the scanners catch.

Several scanners gained forms (2026-11-23) the manual never described: a
module-level ``import app.L.x.Leaf`` and a literal ``import_module("app…")``
for ``import_form``, a literal ``import_module("commons…")`` for the kernel
rule, a bare ``os.environ`` reference for ``env_read_discipline`` (with
``dict(os.environ)`` / ``{**os.environ}`` as the ``copy`` snapshot), an
injected manager for ``transaction_ownership`` and a models MODULE receiver
for ``model_query_discipline``. An operator reading the manual could not
predict a finding. Each documented example is run through its scanner here,
and the manual is pinned to name the form.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.scanners.EnvReadDiscipline import EnvReadDiscipline
from cara.architecture.scanners.ImportForm import ImportForm
from cara.architecture.scanners.ModelQueryDiscipline import ModelQueryDiscipline
from cara.architecture.scanners.TransactionOwnership import TransactionOwnership
from tests.architecture._fixtures import make_manifest, write

_MANUAL = (Path(__file__).resolve().parents[2] / "docs" / "GUARDPACK.md").read_text(
    encoding="utf-8"
)


def test_the_manual_names_every_new_form():
    for phrase in (
        'importlib.import_module("app.L.x.Leaf")',
        "module-level `import app.L.x.Leaf`",
        'import_module("commons...")',
        "a BARE reference to `os.environ`",
        "`dict(os.environ)`",
        "`self.db.transaction()`",
        '`make("DB")`',
        "`m.Product.where(...)`",
    ):
        assert phrase in _MANUAL, phrase


def test_documented_import_form_reaches_are_findings(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services", "controllers"))
    write(tmp_path / "app/services/__init__.py", "")
    write(tmp_path / "app/services/catalog/__init__.py", "")
    write(tmp_path / "app/services/catalog/PriceService.py", "class PriceService: ...\n")
    write(
        tmp_path / "app/controllers/catalog/ProductController.py",
        "import importlib\n"
        "import app.services.catalog.PriceService as prices\n\n\n"
        "def load():\n"
        '    importlib.import_module("app.services.catalog.PriceService")\n'
        '    return importlib.import_module("commons.gates.Rules")\n',
    )

    messages = [finding.message for finding in ImportForm.scan(manifest)]

    assert any("import app.services.catalog.PriceService" in m for m in messages)
    assert any(
        'import_module("app.services.catalog.PriceService")' in m for m in messages
    )
    assert any("`commons.gates.Rules`" in m for m in messages)


def test_documented_env_examples(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app/Bad.py", 'import os\n\nif "SENTRY_DSN" in os.environ:\n    pass\n'
    )
    write(
        tmp_path / "app/Good.py",
        "import os\nimport subprocess\n\n"
        'subprocess.run(["x"], env={**os.environ, "LC_ALL": "C"})\n'
        'subprocess.run(["x"], env=dict(os.environ))\n',
    )

    paths = {finding.path for finding in EnvReadDiscipline.scan(manifest)}

    assert paths == {"app/Bad.py"}


def test_documented_injected_manager_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app/repositories/OrderRepository.py",
        "class OrderRepository:\n"
        "    def __init__(self, db: DatabaseManager):\n"
        "        self.db = db\n\n"
        "    def close(self, order_id: int) -> None:\n"
        "        with self.db.transaction():\n"
        "            pass\n",
    )

    findings = TransactionOwnership.scan(manifest)

    assert any("DB.transaction" in finding.message for finding in findings)


def test_documented_models_module_receiver_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app/services/OrderService.py",
        "from app import models\n\n\n"
        "def open_orders():\n"
        '    return models.Product.where("open", True).get()\n',
    )

    findings = ModelQueryDiscipline.scan(manifest)

    assert any(finding.path == "app/services/OrderService.py" for finding in findings)
