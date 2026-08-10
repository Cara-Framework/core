"""One module-object predicate, read by the barrel writer AND the readers.

``from . import X`` in a package ``__init__`` binds the SUBMODULE only when
``X`` does not itself export a symbol called ``X``; for the class-per-file
shape cara and both products use (``ChannelService.py`` holding
``class ChannelService``), the name resolves to the CLASS. The
BarrelGenerator knew that. ``_ast_utils.module_object_names`` — which
``BarrelCompleteness`` and ``ImportForm`` ask — did not: it accepted any
existing submodule, so the class-per-file case was handed the module-object
EXEMPTION and its public names were never required in the barrel.

That is name/submodule shadowing, the one failure §5.1 names outright ("a
public name missing from its barrel is a bug even before anyone imports it
— name/submodule shadowing taught us this the hard way"), and the guard for
it was blind to it. Two implementations of one predicate, and the wrong one
was the one being read (§5: read the SSOT, never restate it).
"""

from __future__ import annotations

from cara.architecture._ast_utils import is_module_object, module_object_names
from cara.architecture.BarrelGenerator import _Preserved
from cara.architecture.scanners import BarrelCompleteness, ImportForm
from tests.architecture._fixtures import make_manifest, write

SHADOWING_INIT = '"""Services."""\n\nfrom . import ChannelService\n\n__all__ = []\n'


def _shadowing_package(root):
    """``app/services/`` whose barrel binds a leaf that shadows its own name."""
    manifest = make_manifest(root, layers=("services",))
    services = manifest.roots.app / "services"
    write(services / "__init__.py", SHADOWING_INIT)
    write(
        services / "ChannelService.py",
        '"""Channel service."""\n\n\nclass ChannelService:\n    pass\n',
    )
    return manifest, services


class TestThePredicateIsSingleAndShared:
    def test_a_leaf_that_exports_its_own_name_is_not_a_module_object(self, tmp_path):
        _manifest, services = _shadowing_package(tmp_path)
        assert is_module_object(services, "ChannelService") is False
        assert module_object_names(services) == set()

    def test_a_leaf_that_does_not_is_still_a_module_object(self, tmp_path):
        # The exemption is narrowed, not removed: a theme module kept
        # deliberately module-qualified still gets it.
        manifest = make_manifest(tmp_path, layers=("services",))
        services = manifest.roots.app / "services"
        write(services / "__init__.py", '"""Services."""\n\nfrom . import catalog\n')
        write(services / "catalog.py", "PRODUCT_LIMIT = 5\n")
        assert is_module_object(services, "catalog") is True
        assert module_object_names(services) == {"catalog"}

    def test_a_subpackage_barrel_that_re_exports_its_own_name_is_not_one(self, tmp_path):
        manifest = make_manifest(tmp_path, layers=("services",))
        services = manifest.roots.app / "services"
        write(services / "__init__.py", '"""Services."""\n\nfrom . import catalog\n')
        write(
            services / "catalog" / "__init__.py",
            '"""Catalog."""\n\nfrom .catalog import catalog\n\n__all__ = ["catalog"]\n',
        )
        write(services / "catalog" / "catalog.py", "def catalog():\n    pass\n")
        assert is_module_object(services, "catalog") is False

    def test_writer_and_reader_agree_on_the_shadowing_case(self, tmp_path):
        # The claim, stated directly: what the generator PRESERVES as a module
        # object and what the scanners EXEMPT as one are the same set. They
        # were not — the writer said "not a module object", the reader said it
        # was, and the barrel-superset rule went quiet on the difference.
        _manifest, services = _shadowing_package(tmp_path)
        written = _Preserved(services / "__init__.py", "app.services").module_objects
        assert written == module_object_names(services) == set()

    def test_writer_and_reader_agree_on_a_genuine_module_object(self, tmp_path):
        manifest = make_manifest(tmp_path, layers=("services",))
        services = manifest.roots.app / "services"
        write(services / "__init__.py", '"""Services."""\n\nfrom . import catalog\n')
        write(services / "catalog.py", "PRODUCT_LIMIT = 5\n")
        written = _Preserved(services / "__init__.py", "app.services").module_objects
        assert written == module_object_names(services) == {"catalog"}


class TestBarrelCompletenessSeesTheShadowing:
    def test_the_shadowed_class_is_reported_as_a_missing_re_export(self, tmp_path):
        manifest, _services = _shadowing_package(tmp_path)

        findings = BarrelCompleteness.scan(manifest)

        missing = [f for f in findings if "missing re-export" in f.message]
        assert [f.path for f in missing] == ["app/services/__init__.py"]
        assert "ChannelService" in missing[0].message

    def test_a_genuine_module_object_stays_exempt(self, tmp_path):
        manifest = make_manifest(tmp_path, layers=("services",))
        services = manifest.roots.app / "services"
        write(
            services / "__init__.py",
            '"""Services."""\n\nfrom . import catalog\n\n__all__ = ["catalog"]\n',
        )
        write(services / "catalog.py", "PRODUCT_LIMIT = 5\n")

        assert BarrelCompleteness.scan(manifest) == []


class TestImportFormSeesTheShadowing:
    def test_a_deep_import_of_a_shadowed_leaf_is_no_longer_excused(self, tmp_path):
        manifest, _services = _shadowing_package(tmp_path)
        write(
            manifest.roots.app / "controllers" / "ChannelController.py",
            "from app.services.ChannelService import ChannelService\n",
        )

        findings = ImportForm.scan(manifest)

        deep = [f for f in findings if "deep import" in f.message]
        assert [f.path for f in deep] == ["app/controllers/ChannelController.py"]
