"""BarrelGenerator: idempotent AST barrel generation (DOCTRINE §5.1)."""

from __future__ import annotations

import importlib
import sys

from cara.architecture.BarrelGenerator import MAX_LINE, BarrelGenerator

from ._fixtures import make_manifest, write


def test_generates_a_missing_barrel(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    plan = BarrelGenerator.write(manifest)
    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert '"Foo": (".Foo", "Foo")' in content
    assert '"Foo"' in content


def test_second_write_is_a_no_op_idempotence(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "Bar.py", "class Bar:\n    pass\n")
    first = BarrelGenerator.write(manifest)
    assert first.changed  # something was generated
    second = BarrelGenerator.write(manifest)
    assert second.changed == []
    assert second.collisions == []


def test_deleted_child_module_is_removed_instead_of_preserved(tmp_path):
    """Regeneration must not resurrect a dead generated relative import."""
    manifest = make_manifest(tmp_path, layers=("services",))
    child = tmp_path / "app" / "services" / "Removed.py"
    write(child, "class Removed:\n    pass\n")
    BarrelGenerator.write(manifest)
    child.unlink()

    plan = BarrelGenerator.write(manifest)

    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "Removed" not in content
    assert BarrelGenerator.write(manifest).changed == []


def test_removed_child_symbol_is_removed_instead_of_preserved(tmp_path):
    """A surviving module must not keep a symbol it stopped exporting."""
    manifest = make_manifest(tmp_path, layers=("services",))
    child = tmp_path / "app" / "services" / "Child.py"
    write(child, "class Removed:\n    pass\n")
    BarrelGenerator.write(manifest)
    write(child, "class Current:\n    pass\n")

    BarrelGenerator.write(manifest)

    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert '"Current": (".Child", "Current")' in content
    assert "Removed" not in content
    assert BarrelGenerator.write(manifest).changed == []


def test_check_reports_drift_without_writing(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    plan = BarrelGenerator.check(manifest)
    assert plan.changed
    assert not (tmp_path / "app" / "services" / "__init__.py").exists()


def test_hand_written_marker_does_not_exempt_a_barrel(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n# barrel: hand-written\n\n__all__: list[str] = []\n',
    )

    plan = BarrelGenerator.write(manifest)

    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "# barrel: hand-written" not in content
    assert '"Foo": (".Foo", "Foo")' in content


def test_generated_lazy_export_loads_on_demand_and_resists_module_shadowing(
    tmp_path, monkeypatch
):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "__init__.py", "")
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    BarrelGenerator.write(manifest)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in ("app.services.Foo", "app.services", "app"):
        sys.modules.pop(name, None)

    package = importlib.import_module("app.services")
    assert "app.services.Foo" not in sys.modules
    exported = package.Foo
    assert exported.__name__ == "Foo"

    importlib.import_module("app.services.Foo")
    assert package.Foo is exported

    replacement = object()
    package.Foo = replacement
    assert package.Foo is replacement


def test_foreign_lazy_export_remains_in_the_complete_public_contract(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        "from cara._LazyExports import _install_lazy_exports\n\n"
        "_LAZY_EXPORTS = {\n"
        '    "External": ("cara.exceptions.types.CaraException", "CaraException"),\n'
        "}\n\n"
        "__all__: list[str] = []\n\n"
        "_install_lazy_exports(__name__, _LAZY_EXPORTS)\n",
    )

    BarrelGenerator.write(manifest)

    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert '"External": (' in content
    assert '__all__ = [\n    "External",\n    "Foo",\n]' in content
    assert BarrelGenerator.write(manifest).changed == []


def test_docstring_is_preserved_across_regeneration(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""A deliberately hand-written docstring."""\n\n__all__: list[str] = []\n',
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "A deliberately hand-written docstring." in content


def test_module_object_contract_is_preserved(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "Text.py",
        "def helper():\n    pass\n\n\nOTHER = 1\n",
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom . import Text\n\n__all__ = [\n    "Text",\n]\n',
    )
    plan = BarrelGenerator.write(manifest)
    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "from . import Text" in content
    assert "helper" not in content  # Text's own symbols stay module-qualified
    assert BarrelGenerator.write(manifest).changed == []


def test_aliased_import_is_preserved(tmp_path):
    """The existing aliased import survives regeneration verbatim — the
    generator adds the plain (unaliased) export alongside it rather than
    silently dropping either binding."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(tmp_path / "app" / "services" / "Renamed.py", "class Original:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\nfrom .Renamed import Original as Aliased\n\n'
        '__all__ = [\n    "Aliased",\n    "Foo",\n]\n',
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "from .Renamed import Original as Aliased" in content
    assert '"Aliased"' in content

    # idempotent from here: a second run changes nothing further.
    second = BarrelGenerator.write(manifest)
    assert second.changed == []


def test_long_aliased_import_is_wrapped_the_way_ruff_wraps_it(tmp_path):
    """A preserved import keeps its NAMES, not its bytes: it is re-rendered
    at ``MAX_LINE`` like any generated one.

    Echoing a 96-char ``from X import Y as Z`` back on a single line made
    ``ruff format`` wrap it and the next ``arch:barrels`` pass unwrap it
    again — the generator and the formatter ping-ponging over one file, so
    ``ruff format --check`` and ``arch:barrels`` (check) could never both
    be green. The wrapped shape below is byte-for-byte what ruff emits, and
    its trailing comma is what stops ruff collapsing it back.
    """
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "caching" / "StorefrontRevalidate.py",
        "def dispatch_for_product():\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        "from .caching.StorefrontRevalidate import "
        "dispatch_for_product as dispatch_storefront_revalidate\n\n"
        '__all__ = [\n    "dispatch_storefront_revalidate",\n]\n',
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert (
        "from .caching.StorefrontRevalidate import (\n"
        "    dispatch_for_product as dispatch_storefront_revalidate,\n"
        ")"
    ) in content
    assert max(len(line) for line in content.splitlines()) <= MAX_LINE

    # ...and that wrapped form is a fixed point: re-running the generator
    # over ruff's own shape must not unwrap it.
    assert BarrelGenerator.write(manifest).changed == []


def test_long_module_object_bind_is_wrapped(tmp_path):
    """The module-object bind is rendered by the same width-aware helper —
    the overflow is systemic to how imports are emitted, not specific to
    the aliased form."""
    manifest = make_manifest(tmp_path, layers=("services",))
    stems = (
        "StorefrontRevalidationCachingHelpers",
        "StorefrontRevalidationQueueingHelpers",
    )
    for stem in stems:
        write(tmp_path / "app" / "services" / f"{stem}.py", "def helper():\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        f"from . import {', '.join(stems)}\n\n"
        "__all__ = [\n" + "".join(f'    "{s}",\n' for s in stems) + "]\n",
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "from . import (\n" + "".join(f"    {s},\n" for s in stems) + ")" in content
    assert max(len(line) for line in content.splitlines()) <= MAX_LINE
    assert BarrelGenerator.write(manifest).changed == []


def test_documented_import_gets_the_blank_line_ruff_puts_before_a_comment(tmp_path):
    """A preserved import's leading comment block is spaced the way ruff
    spaces it: ruff inserts a blank line before an own-line comment that
    follows code, so emitting the comment butted against the previous
    import was the same generator-vs-formatter ping-pong as the width bug,
    one file over."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        "from acme.kernel.Widgets import Widget\n"
        "# Text helpers bind FIRST: a heavy module below back-imports them.\n"
        "from acme.kernel.Text import format_title\n\n"
        '__all__ = [\n    "Widget",\n    "format_title",\n]\n',
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert (
        "from acme.kernel.Widgets import Widget\n"
        "\n"
        "# Text helpers bind FIRST: a heavy module below back-imports them.\n"
        "from acme.kernel.Text import format_title"
    ) in content
    assert BarrelGenerator.write(manifest).changed == []


def test_post_all_deliberate_late_bind_is_preserved(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\n\n__all__ = [\n    "Foo",\n    "Late",\n]\n\n'
        "from .Foo import Foo as Late  # deliberate late bind\n",
    )
    plan = BarrelGenerator.write(manifest)
    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert content.rstrip().endswith(
        "from .Foo import Foo as Late  # deliberate late bind"
    )


def test_future_imports_are_kept_first(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom __future__ import annotations\n\n__all__: list[str] = []\n',
    )
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    doc_end = content.index('"""', 3) + 3
    rest = content[doc_end:].lstrip("\n")
    assert rest.startswith("from __future__ import annotations")


def test_preserved_registry_is_emitted_after_the_imports_it_uses(tmp_path):
    """Executable barrel metadata must never run before its symbol imports."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "Scanner.py",
        "class Scanner:\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        'REGISTRY = {"scanner": Scanner}\n\n'
        "from .Scanner import Scanner\n\n"
        '__all__ = ["REGISTRY", "Scanner"]\n',
    )

    BarrelGenerator.write(manifest)

    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert content.index("from .Scanner import Scanner") < content.index("REGISTRY =")
    assert BarrelGenerator.write(manifest).changed == []


def test_dependency_free_helper_follows_generated_imports(tmp_path):
    """Generated barrels keep every import before executable declarations."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(
        tmp_path / "app" / "services" / "Configuration.py",
        "class Configuration:\n    pass\n",
    )
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\n'
        "def config():\n    return Configuration()\n\n"
        "from .Configuration import Configuration\n\n"
        '__all__ = ["Configuration", "config"]\n',
    )

    BarrelGenerator.write(manifest)

    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert content.index("from .Configuration import Configuration") < content.index(
        "def config"
    )
    assert '"config"' in content
    assert BarrelGenerator.write(manifest).changed == []


def test_registry_declaration_follows_generated_symbol_imports(tmp_path):
    """A barrel-owned registry must not reference children before they bind."""
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nREGISTRY = {"foo": Foo}\n\n__all__ = ["REGISTRY"]\n',
    )

    BarrelGenerator.write(manifest)

    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert content.index("from .Foo import Foo") < content.index(
        'REGISTRY = {"foo": Foo}'
    )
    assert BarrelGenerator.write(manifest).changed == []


def test_collision_between_two_modules_is_reported(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Shared:\n    pass\n")
    write(tmp_path / "app" / "services" / "Bar.py", "class Shared:\n    pass\n")
    plan = BarrelGenerator.check(manifest)
    assert plan.collisions
    assert any("Shared" in c for c in plan.collisions)


def test_kernel_package_nested_subpackage_regenerates_depth_first(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "commons" / "models" / "core" / "User.py", "class User:\n    pass\n")
    plan = BarrelGenerator.write(manifest)
    assert "commons/models/core/__init__.py" in plan.changed
    assert "commons/models/__init__.py" in plan.changed
    core_content = (tmp_path / "commons" / "models" / "core" / "__init__.py").read_text()
    assert '"User": (".User", "User")' in core_content
    root_content = (tmp_path / "commons" / "models" / "__init__.py").read_text()
    assert '"User": (".core", "User")' in root_content
    # idempotent across the whole kernel tree too
    second = BarrelGenerator.write(manifest)
    assert second.changed == []


def test_job_root_class_pin_binds_first_in_a_job_layer(tmp_path):
    manifest = make_manifest(
        tmp_path, layers=("jobs",), job_roots=("jobs",), job_root_class="BaseJob"
    )
    write(tmp_path / "app" / "jobs" / "BaseJob.py", "class BaseJob:\n    pass\n")
    write(tmp_path / "app" / "jobs" / "DoThingJob.py", "class DoThingJob:\n    pass\n")
    BarrelGenerator.write(manifest)
    content = (tmp_path / "app" / "jobs" / "__init__.py").read_text()
    base_idx = content.index("from .BaseJob import BaseJob")
    other_idx = content.index('"DoThingJob": (".DoThingJob", "DoThingJob")')
    assert base_idx < other_idx
    assert '"BaseJob"' in content and '"DoThingJob"' in content
