"""FlowLaw: controllers/jobs cannot skip the use-case service."""

from __future__ import annotations

from cara.architecture.scanners import FlowLaw

from ._fixtures import make_manifest, write


def test_controller_repository_import_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "controllers" / "ProductController.py",
        "from app.repositories import ProductRepository\n"
        "class ProductController:\n"
        "    pass\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "flow-law violation" in findings[0].message


def test_job_model_and_db_imports_are_findings(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "RefreshJob.py",
        "from cara.facades import DB, Log\n"
        "from app.models import Product\n"
        "class RefreshJob:\n"
        "    pass\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "2 flow-law violation" in findings[0].message


def test_barrel_evasions_and_cara_db_export_are_findings(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "RefreshJob.py",
        "from app import repositories\n"
        "from commons import models\n"
        "from cara import DB\n"
        "class RefreshJob:\n"
        "    pass\n",
    )

    findings = FlowLaw.scan(manifest)

    assert len(findings) == 1
    assert "3 flow-law violation" in findings[0].message


def test_use_case_service_import_is_clean(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "RefreshJob.py",
        "from app.services.catalog import RefreshProductService\n"
        "class RefreshJob:\n"
        "    pass\n",
    )
    assert FlowLaw.scan(manifest) == []


def test_repository_container_lookup_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "RefreshJob.py",
        "class RefreshJob:\n"
        "    def execute(self):\n"
        "        return self.resolve('ProductRepository')\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "resolves repository" in findings[0].message


def test_qualified_repository_container_lookup_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "RefreshJob.py",
        "class RefreshJob:\n"
        "    def execute(self):\n"
        "        return self.application.make(ports.ProductRepository)\n",
    )

    findings = FlowLaw.scan(manifest)

    assert len(findings) == 1
    assert "resolves repository" in findings[0].message


def test_counted_flow_debt_is_shrink_only(tmp_path):
    path = tmp_path / "app" / "jobs" / "RefreshJob.py"
    write(
        path,
        "from app.repositories import ProductRepository\n"
        "class RefreshJob:\n"
        "    pass\n",
    )
    manifest = make_manifest(
        tmp_path,
        seam_allowlists={"flow_law": {"app/jobs/RefreshJob.py": 1}},
    )
    assert FlowLaw.scan(manifest) == []

    write(
        path,
        "from app.repositories import ProductRepository\n"
        "from app.models import Product\n"
        "class RefreshJob:\n"
        "    pass\n",
    )
    assert any("debt grew" in finding.message for finding in FlowLaw.scan(manifest))

    write(
        path,
        "from app.services.catalog import RefreshProductService\n"
        "class RefreshJob:\n"
        "    pass\n",
    )
    assert any("stale" in finding.message for finding in FlowLaw.scan(manifest))


def test_support_helper_named_jobs_is_not_an_edge(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "support" / "jobs" / "WorkerHooks.py",
        "from app.repositories import WorkerRepository\n",
    )
    assert FlowLaw.scan(manifest) == []
