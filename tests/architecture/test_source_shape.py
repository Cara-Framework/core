"""SourceShape: hard source budgets and exact shrink-only debts."""

from __future__ import annotations

from dataclasses import replace

from cara.architecture.scanners import SourceShape

from ._fixtures import make_manifest, write


def _manifest(tmp_path, **overrides):
    manifest = make_manifest(tmp_path, layers=("controllers", "jobs"))
    return replace(manifest, **overrides)


def test_hard_line_limit_is_enforced(tmp_path):
    manifest = _manifest(tmp_path, source_shape_hard_limit=5)
    write(
        tmp_path / "app" / "services" / "Large.py",
        "\n".join(f"VALUE_{index} = {index}" for index in range(6)),
    )
    findings = SourceShape.scan(manifest)
    assert any("hard 5-line limit" in finding.message for finding in findings)


def test_exact_line_debt_blocks_growth_and_stale_pin(tmp_path):
    path = tmp_path / "app" / "services" / "Large.py"
    write(path, "\n".join(f"VALUE_{index} = {index}" for index in range(6)))
    manifest = _manifest(
        tmp_path,
        source_shape_hard_limit=5,
        seam_allowlists={"source_shape_lines": {"app/services/Large.py": 6}},
    )
    assert SourceShape.scan(manifest) == []

    write(path, path.read_text() + "\nVALUE_6 = 6\n")
    assert any("debt grew" in finding.message for finding in SourceShape.scan(manifest))

    write(path, "\n".join(f"VALUE_{index} = {index}" for index in range(5)))
    assert any("stale" in finding.message for finding in SourceShape.scan(manifest))


def test_multiple_public_classes_are_counted(tmp_path):
    manifest = _manifest(tmp_path)
    write(
        tmp_path / "app" / "services" / "Grouped.py",
        "class First:\n    pass\n\nclass Second:\n    pass\n",
    )
    findings = SourceShape.scan(manifest)
    assert any("multiple public classes" in finding.message for finding in findings)


def test_single_public_class_must_match_filename(tmp_path):
    manifest = _manifest(tmp_path)
    write(tmp_path / "app" / "services" / "Expected.py", "class Actual:\n    pass\n")
    findings = SourceShape.scan(manifest)
    assert any("must be named for file" in finding.message for finding in findings)


def test_edge_method_limit_is_exact_debt(tmp_path):
    manifest = _manifest(tmp_path, source_shape_edge_method_limit=3)
    write(
        tmp_path / "app" / "jobs" / "LargeJob.py",
        "class LargeJob:\n"
        "    def execute(self):\n"
        "        first = 1\n"
        "        second = 2\n"
        "        return first + second\n",
    )
    findings = SourceShape.scan(manifest)
    assert any("edge method exceeds 3-line limit" in finding.message for finding in findings)

    pinned = replace(
        manifest,
        seam_allowlists={
            "source_shape_edge_methods": {
                "app/jobs/LargeJob.py::LargeJob.execute": 4
            }
        },
    )
    assert SourceShape.scan(pinned) == []


def test_generated_barrels_are_excluded(tmp_path):
    manifest = _manifest(tmp_path, source_shape_hard_limit=2)
    write(
        tmp_path / "app" / "jobs" / "__init__.py",
        "\n".join(f"from .Job{index} import Job{index}" for index in range(10)),
    )
    assert SourceShape.scan(manifest) == []
