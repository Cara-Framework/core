"""Scanners read the app namespace from the manifest, never from a literal.

``ImportForm`` compared imports against ``"app.<layer>"`` and paths against
``"app/<layer>/"``; ``SourceShape._is_edge_path`` asked whether a layer's
parent directory was literally ``"app"``. Aimed at a tree rooted under any
other name, both matched NOTHING and returned an empty finding list — a
vacuously green guard, which is strictly worse than no guard because it is
indistinguishable from a clean codebase. That is what kept the Guard Pack
from ever being pointed at cara itself (``_cara_self_scan``).

These trees are rooted at ``fw/`` rather than ``app/`` for exactly that
reason: every assertion here passes trivially, and silently, against the
literal-matching versions. The product-shaped counterparts next door
(``test_import_form``, ``test_source_shape``) pin the ``app/`` behaviour, so
the two suites together say the rules did not change — only their reach.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.Manifest import Manifest
from cara.architecture.ManifestRoots import ManifestRoots
from cara.architecture.scanners import ImportForm, SourceShape
from tests.architecture._fixtures import write


def _framework_manifest(root: Path, **overrides) -> Manifest:
    """A manifest whose app package is ``fw/``, not ``app/``."""
    app = root / "fw"
    app.mkdir(parents=True, exist_ok=True)
    defaults: dict = dict(
        product="acme",
        deployable="fw",
        roots=ManifestRoots(
            deployable=root,
            app=app,
            scanner_roots={
                scanner: (app,)
                for scanner in ("import_form", "import_tiers", "source_shape")
            },
            kernel={},
            local_root_names=(),
        ),
        layers=("services",),
        domain_layers=(),
        domains={},
        scan_plugin_string_literals=False,
        kernel_packages=frozenset(),
        kernel_barrel_packages=frozenset(),
        seam_kernel_packages=frozenset(),
    )
    defaults.update(overrides)
    return Manifest(**defaults)


class TestTheManifestExposesTheNamespace:
    def test_the_namespace_and_path_prefix_follow_the_app_directory(self, tmp_path):
        roots = _framework_manifest(tmp_path).roots
        assert roots.app_namespace == "fw"
        assert roots.app_path_prefix == "fw"

    def test_a_product_still_reads_app(self, tmp_path):
        app = tmp_path / "app"
        app.mkdir()
        roots = ManifestRoots(deployable=tmp_path, app=app)
        assert roots.app_namespace == "app"
        assert roots.app_path_prefix == "app"

    def test_the_path_prefix_falls_back_when_app_is_not_under_the_deployable(
        self, tmp_path
    ):
        # A deployable that reaches its app tree through a sibling checkout
        # has no relative path to offer; the dotted name is still the truth.
        app = tmp_path / "elsewhere" / "fw"
        app.mkdir(parents=True)
        roots = ManifestRoots(deployable=tmp_path / "deployable", app=app)
        assert roots.app_path_prefix == "fw"


class TestImportFormReachesANonAppRoot:
    def test_a_deep_import_into_a_layer_is_found(self, tmp_path):
        manifest = _framework_manifest(tmp_path)
        app = manifest.roots.app
        write(app / "services" / "__init__.py", '"""Services."""\n\n__all__ = []\n')
        write(app / "services" / "catalog" / "__init__.py", '"""Catalog."""\n')
        write(
            app / "services" / "catalog" / "ProductService.py",
            "class ProductService:\n    pass\n",
        )
        write(
            app / "support" / "Caller.py",
            "from fw.services.catalog.ProductService import ProductService\n",
        )

        findings = ImportForm.scan(manifest)

        deep = [f for f in findings if "deep import" in f.message]
        assert [f.path for f in deep] == ["fw/support/Caller.py"]
        # The message names the barrel to use, and it must name fw's.
        assert "fw.services barrel" in deep[0].message

    def test_a_sibling_reaching_its_own_layer_barrel_is_found(self, tmp_path):
        manifest = _framework_manifest(tmp_path)
        app = manifest.roots.app
        write(app / "services" / "__init__.py", '"""Services."""\n\n__all__ = []\n')
        write(
            app / "services" / "OrderService.py",
            "from fw.services import ProductService\n",
        )

        findings = ImportForm.scan(manifest)

        sibling = [f for f in findings if "own layer barrel" in f.message]
        assert [f.path for f in sibling] == ["fw/services/OrderService.py"]

    def test_a_layer_sibling_is_still_exempt_from_the_deep_import_rule(self, tmp_path):
        # The sibling exemption is a PATH prefix test; if it stopped matching
        # for a non-app root, siblings would be reported as deep importers and
        # the rule would invert. Fail-closed does not mean fail-wrong.
        manifest = _framework_manifest(tmp_path)
        app = manifest.roots.app
        write(app / "services" / "__init__.py", '"""Services."""\n\n__all__ = []\n')
        write(app / "services" / "catalog" / "__init__.py", '"""Catalog."""\n')
        write(
            app / "services" / "catalog" / "ProductService.py",
            "class ProductService:\n    pass\n",
        )
        write(
            app / "services" / "catalog" / "OrderService.py",
            "from fw.services.catalog.ProductService import ProductService\n",
        )

        findings = ImportForm.scan(manifest)

        assert [f for f in findings if "deep import" in f.message] == []

    def test_a_kernel_barrel_exemption_follows_the_app_root(self, tmp_path):
        # ``_kernel_barrel_files`` built ``app/<pkg>/__init__.py``. With a
        # different root the exemption never matched, so the ONE file allowed
        # to import the kernel was reported as a violation.
        manifest = _framework_manifest(tmp_path, kernel_packages=frozenset({"models"}))
        app = manifest.roots.app
        write(
            app / "models" / "__init__.py",
            '"""Bridge."""\n\nfrom commons.models import Product\n\n'
            '__all__ = ["Product"]\n',
        )
        write(app / "support" / "Caller.py", "from commons.models import Product\n")

        findings = ImportForm.scan(manifest)

        kernel = [f for f in findings if "consumed only" in f.message]
        assert [f.path for f in kernel] == ["fw/support/Caller.py"]


class TestSourceShapeReachesANonAppRoot:
    def test_an_oversized_edge_method_is_found(self, tmp_path):
        manifest = _framework_manifest(
            tmp_path,
            layers=("jobs",),
            source_shape_edge_layers=frozenset({"jobs"}),
            source_shape_edge_method_limit=3,
        )
        body = "\n".join(f"        x{i} = {i}" for i in range(8))
        write(
            manifest.roots.app / "jobs" / "SendInvoiceJob.py",
            f"class SendInvoiceJob:\n    def handle(self):\n{body}\n",
        )

        findings = SourceShape.scan(manifest)

        assert [f.path for f in findings] == ["fw/jobs/SendInvoiceJob.py"]
        assert "SendInvoiceJob.handle" in findings[0].message

    def test_a_non_edge_layer_is_still_not_policed(self, tmp_path):
        manifest = _framework_manifest(
            tmp_path,
            layers=("services",),
            source_shape_edge_layers=frozenset({"jobs"}),
            source_shape_edge_method_limit=3,
        )
        body = "\n".join(f"        x{i} = {i}" for i in range(8))
        write(
            manifest.roots.app / "services" / "OrderService.py",
            f"class OrderService:\n    def place(self):\n{body}\n",
        )

        assert SourceShape.scan(manifest) == []
