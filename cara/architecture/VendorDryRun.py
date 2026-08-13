"""VendorDryRun: the per-product sandbox proof of the production build (§2).

DOCTRINE §2 makes the vendor step a release gate: *"covered by framework
regression tests plus a per-product sandbox dry-run proof: copy the tree,
vendor it, compile it, import it. A release without a green dry-run is not a
release."* The framework's own tests pin ``VendorCommonsCommand``'s behaviour
against synthetic trees; only a dry-run over the REAL tree can prove that
*this product's* kernel still vendors into an importable image.

The gap this closes is not hypothetical. The vendor step folds deep
``commons.models`` imports onto the ``app.models`` barrel, so a kernel helper
that the barrel never re-exported resolves fine in development — where the
deep path still exists — and raises ``ImportError`` at image import. Unit
suites, ``arch:check`` and a successful ``docker build`` all stayed green
while the image was dead on boot.

Staging mirrors the Dockerfile's source stage exactly, including reading each
repository's ``.dockerignore`` for what the build context excludes (§5: read
the SSOT, never restate it). That fidelity matters — a scratch directory left
in the kernel repo by a migration command makes the real build fail the same
way it makes this dry-run fail, instead of being discovered in CI.

Five steps, each a hard gate:

1. **stage**  — copy the deployable, drop the ``cara``/``commons`` symlinks,
   copy the kernel dev root in as ``commons/``, re-link ``cara``.
2. **vendor** — run ``build:vendor-commons`` boot-free in the sandbox.
3. **verify** — ``commons/`` is gone and no ``commons.<pkg>`` reference survives.
4. **compile** — ``compileall`` over the shipped trees.
5. **import** — import every kernel barrel and prove ``commons`` is unimportable.
"""

from __future__ import annotations

import fnmatch
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from cara.architecture.Manifest import Manifest

from .DryRunResult import DryRunResult

_VENDOR = (
    "from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand;"
    "raise SystemExit(VendorCommonsCommand(application=None).handle())"
)

# Never staged regardless of .dockerignore: build/venv/VCS noise that a
# .dockerignore may omit because BuildKit would not reach it anyway.
_ALWAYS_SKIP = frozenset(
    {".git", "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", "node_modules"}
)


def _dockerignore(root: Path) -> list[str]:
    """The repository's own build-context exclusions, verbatim."""
    path = root / ".dockerignore"
    if not path.is_file():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _ignored(rel: str, name: str, patterns: list[str]) -> bool:
    if name in _ALWAYS_SKIP:
        return True
    return any(
        fnmatch.fnmatch(rel, pattern)
        or fnmatch.fnmatch(name, pattern)
        or rel.startswith(pattern.rstrip("/") + "/")
        for pattern in patterns
    )


def _stage_tree(src: Path, dst: Path, patterns: list[str], prefix: str = "") -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for child in sorted(src.iterdir()):
        rel = f"{prefix}{child.name}"
        if _ignored(rel, child.name, patterns):
            continue
        target = dst / child.name
        if child.is_symlink():
            continue  # the Dockerfile drops and recreates the two kernel links
        if child.is_dir():
            _stage_tree(child, target, patterns, prefix=f"{rel}/")
        else:
            shutil.copy2(child, target)


def _run(python: str, args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [python, *args], cwd=cwd, capture_output=True, text=True, check=False
    )


class VendorDryRun:
    """Copy → vendor → compile → import, over the product's real tree."""

    @staticmethod
    def run(manifest: Manifest, python: str | None = None) -> DryRunResult:
        python = python or sys.executable
        result = DryRunResult(deployable=manifest.deployable)
        deployable = manifest.roots.deployable
        kernel_root = deployable / manifest.roots.kernel_dev_root_name
        if not kernel_root.is_dir():
            result.failures.append(f"kernel dev root not found: {kernel_root}")
            return result

        with tempfile.TemporaryDirectory(prefix="cara-vendor-dryrun-") as tmp:
            box = Path(tmp) / "build"

            # 1) stage — the Dockerfile's source stage, .dockerignore included.
            _stage_tree(deployable, box, _dockerignore(deployable))
            _stage_tree(
                kernel_root.resolve(), box / "commons", _dockerignore(kernel_root)
            )
            framework = box / "commons" / "cara" / "cara"
            if not framework.is_dir():
                result.failures.append(
                    "staged kernel has no commons/cara/cara — the framework clone "
                    "is excluded from the build context"
                )
                return result
            (box / "cara").symlink_to(Path("commons/cara/cara"))
            result.steps.append("staged the deployable + kernel build context")

            # 2) vendor — boot-free, exactly as the Dockerfile runs it.
            vendored = _run(python, ["-c", _VENDOR], box)
            if vendored.returncode != 0:
                result.failures.append(
                    "build:vendor-commons failed:\n"
                    + (vendored.stderr or vendored.stdout).strip()
                )
                return result
            result.steps.append("vendored the kernel into app/")

            # 3) verify — the kernel is dev-only and leaves no trace behind.
            if (box / "commons").exists():
                result.failures.append("commons/ survived the vendor step")
            shipped = [box / "app", box / "database", box / "packages", box / "config"]
            residual = sorted(
                f"{py.relative_to(box)}:{n}"
                for root in shipped
                if root.is_dir()
                for py in root.rglob("*.py")
                for n, line in enumerate(py.read_text(encoding="utf-8").splitlines(), 1)
                if any(f"commons.{pkg}" in line for pkg in manifest.kernel_packages)
            )
            if residual:
                result.failures.append(
                    f"{len(residual)} residual commons.* reference(s) in the image "
                    f"tree: {', '.join(residual[:10])}"
                )
            result.steps.append("verified commons/ is absent with no residual imports")

            # 4) compile — every shipped tree parses.
            targets = [d.name for d in shipped if d.is_dir()]
            compiled = _run(python, ["-m", "compileall", "-q", *targets], box)
            if compiled.returncode != 0:
                result.failures.append(
                    "compileall failed:\n"
                    + (compiled.stdout or compiled.stderr).strip()[-4000:]
                )
                return result
            result.steps.append(f"compiled {', '.join(targets)}")

            # 5) import — the proof the other four steps cannot give.
            barrels = sorted(f"app.{pkg}" for pkg in manifest.kernel_packages)
            script = (
                "import importlib, importlib.util as u\n"
                f"for name in {barrels!r}:\n"
                "    importlib.import_module(name)\n"
                "assert u.find_spec('commons') is None, 'commons is still importable'\n"
            )
            imported = _run(python, ["-c", script], box)
            if imported.returncode != 0:
                result.failures.append(
                    "the vendored image does not import:\n"
                    + (imported.stderr or imported.stdout).strip()[-4000:]
                )
                return result
            result.steps.append(f"imported {', '.join(barrels)}")

        return result
