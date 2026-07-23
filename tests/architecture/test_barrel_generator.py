"""BarrelGenerator: idempotent AST barrel generation (DOCTRINE §5.1)."""

from __future__ import annotations

from cara.architecture.BarrelGenerator import BarrelGenerator

from ._fixtures import make_manifest, write


def test_generates_a_missing_barrel(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    plan = BarrelGenerator.write(manifest)
    assert "app/services/__init__.py" in plan.changed
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "from .Foo import Foo" in content
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


def test_check_reports_drift_without_writing(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    plan = BarrelGenerator.check(manifest)
    assert plan.changed
    assert not (tmp_path / "app" / "services" / "__init__.py").exists()


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
    assert plan.changed == []  # already exactly what generation would produce
    content = (tmp_path / "app" / "services" / "__init__.py").read_text()
    assert "from . import Text" in content
    assert "helper" not in content  # Text's own symbols stay module-qualified


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


def test_post_all_deliberate_late_bind_is_preserved(tmp_path):
    manifest = make_manifest(tmp_path, layers=("services",))
    write(tmp_path / "app" / "services" / "Foo.py", "class Foo:\n    pass\n")
    write(
        tmp_path / "app" / "services" / "__init__.py",
        '"""Layer."""\n\nfrom .Foo import Foo\n\n__all__ = [\n    "Foo",\n    "Late",\n]\n\n'
        "from .Foo import Foo as Late  # deliberate late bind\n",
    )
    plan = BarrelGenerator.write(manifest)
    assert plan.changed == []
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
    assert "from .User import User" in core_content
    root_content = (tmp_path / "commons" / "models" / "__init__.py").read_text()
    assert "from .core import User" in root_content
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
    other_idx = content.index("from .DoThingJob import DoThingJob")
    assert base_idx < other_idx
    assert '"BaseJob"' in content and '"DoThingJob"' in content
