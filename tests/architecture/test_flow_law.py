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
        "from app.repositories import ProductRepository\nclass RefreshJob:\n    pass\n",
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


# ── the models packages are judged on the BOUND NAME ────────────────────────


def test_pure_helper_from_the_models_foundation_is_allowed(tmp_path):
    """``models`` is the kernel's foundation, so pure value helpers live there
    (DOCTRINE §2). Binding one at an edge reaches no data layer: ``models``
    imports nothing from the kernel, so nothing in it can touch a repository,
    a gate or a connection."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app.models import normalize_gtin\n"
        "from commons.models import MIN_GTIN_DIGITS\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    assert FlowLaw.scan(manifest) == []


def test_model_class_from_the_same_module_is_still_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app.models import normalize_gtin, Product\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "app.models" in findings[0].message


def test_deep_model_module_path_is_judged_the_same_way(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app.models.core.IdentifierNormalization import normalize_gtin\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    assert FlowLaw.scan(manifest) == []


def test_repositories_and_gates_are_still_banned_outright(tmp_path):
    """The name carve-out is for the FOUNDATION only — a lowercase binding
    from a gate or a repository is still the edge skipping the use case."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app.gates import pricing_floor\n"
        "from app.repositories import product_rows\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "2 flow-law violation" in findings[0].message


def test_importing_the_models_module_object_stays_forbidden(tmp_path):
    """``from app import models`` hands the edge every class in the package,
    so the barrel-member check is not softened by the name carve-out."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app import models\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    findings = FlowLaw.scan(manifest)
    assert len(findings) == 1
    assert "barrel" in findings[0].message


def test_upper_snake_constant_is_not_mistaken_for_a_class(tmp_path):
    """A leading capital is not enough: ``MIN_GTIN_DIGITS`` is a constant of
    the same pure helper module, not a model."""
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "ScrapeJob.py",
        "from app.models import MIN_GTIN_DIGITS\n"
        "class ScrapeJob:\n"
        "    pass\n",
    )
    assert FlowLaw.scan(manifest) == []
