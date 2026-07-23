"""JobIdempotency: every queued job declares or documents its idempotency (DOCTRINE §8)."""

from __future__ import annotations

from dataclasses import replace

from cara.architecture.scanners import JobIdempotency

from ._fixtures import make_manifest, write


def test_undeclared_job_is_a_finding(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "app" / "jobs" / "FooJob.py", "class FooJob(BaseJob):\n    pass\n")
    findings = JobIdempotency.scan(manifest)
    assert findings and "FooJob" in findings[0].message


def test_declared_job_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "FooJob.py",
        "class FooJob(BaseJob):\n    idempotency_params = ('listing_id',)\n",
    )
    assert JobIdempotency.scan(manifest) == []


def test_inherited_declaration_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "Chain.py",
        "class BaseSyncJob(BaseJob):\n"
        "    idempotency_params = ('listing_id',)\n\n\n"
        "class ChildJob(BaseSyncJob):\n"
        "    pass\n",
    )
    assert JobIdempotency.scan(manifest) == []


def test_documented_opt_out_passes(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "SweepJob.py",
        "class SweepJob(BaseJob):\n"
        "    # idempotency: none — argument-free singleton sweep\n"
        "    pass\n",
    )
    assert JobIdempotency.scan(manifest) == []


def test_opt_out_without_a_reason_is_still_untagged(tmp_path):
    manifest = make_manifest(tmp_path)
    write(
        tmp_path / "app" / "jobs" / "SweepJob.py",
        "class SweepJob(BaseJob):\n    # idempotency: none —\n    pass\n",
    )
    findings = JobIdempotency.scan(manifest)
    assert findings and "SweepJob" in findings[0].message


def test_non_job_class_is_not_checked(tmp_path):
    manifest = make_manifest(tmp_path)
    write(tmp_path / "app" / "jobs" / "Helper.py", "class NotAJob:\n    pass\n")
    assert JobIdempotency.scan(manifest) == []


def test_exemption_pin_suppresses_the_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        job_idempotency_exemptions=frozenset({"app/jobs/FooJob.py::FooJob"}),
    )
    write(tmp_path / "app" / "jobs" / "FooJob.py", "class FooJob(BaseJob):\n    pass\n")
    assert JobIdempotency.scan(manifest) == []


def test_stale_pin_now_satisfied_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        job_idempotency_exemptions=frozenset({"app/jobs/FooJob.py::FooJob"}),
    )
    write(
        tmp_path / "app" / "jobs" / "FooJob.py",
        "class FooJob(BaseJob):\n    idempotency_params = ('x',)\n",
    )
    findings = JobIdempotency.scan(manifest)
    assert any("now declares/tags" in f.message for f in findings)


def test_stale_pin_no_such_class_is_a_finding(tmp_path):
    manifest = make_manifest(
        tmp_path,
        job_idempotency_exemptions=frozenset({"app/jobs/Ghost.py::GhostJob"}),
    )
    findings = JobIdempotency.scan(manifest)
    assert any("no such job class exists" in f.message for f in findings)


def test_plugin_package_jobs_are_scanned_too(tmp_path):
    manifest = make_manifest(tmp_path)
    packages = tmp_path / "packages"
    write(
        packages / "acme" / "jobs" / "PluginJob.py",
        "class PluginJob(BaseJob):\n    pass\n",
    )
    manifest = replace(manifest, roots=replace(manifest.roots, packages=packages))
    findings = JobIdempotency.scan(manifest)
    assert any("PluginJob" in f.message for f in findings)
