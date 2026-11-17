"""VendorDryRun: the per-product sandbox proof of the production build (§2).

DOCTRINE §2 makes the vendor step a release gate: *"covered by framework
regression tests plus a per-product sandbox dry-run proof: copy the tree,
vendor it, compile it, import it. A release without a green dry-run is not a
release."* The framework's own tests pin ``VendorCommonsCommand``'s behaviour
against synthetic trees; only a dry-run over the REAL tree can prove that
*this product's* kernel still vendors into an importable image.

The gap this closes is not hypothetical. The vendor step flattens
``commons.models``, so a deep import that resolves fine in development — where
the deep path still exists — can raise ``ImportError`` at image import. Unit
suites, ``arch:check`` and a successful ``docker build`` all stayed green
while the image was dead on boot. It happened through a kernel helper nothing
but a submodule of ``shared`` and ``gates`` imported, under barrels too thin to
reach it — which is why step 5 walks every kernel module, not the barrels.

Staging mirrors the Dockerfile's source stage exactly, including reading each
repository's build-context exclusions the way BuildKit does — ``Dockerfile.dockerignore``
beside the Dockerfile wins over ``.dockerignore``, patterns use Docker's
matching and ``!`` re-includes (§5: read the SSOT, never restate it). That fidelity matters — a scratch directory left
in the kernel repo by a migration command makes the real build fail the same
way it makes this dry-run fail, instead of being discovered in CI.

Five steps, each a hard gate:

1. **stage**  — copy the deployable, drop the ``cara``/``commons`` symlinks,
   copy the kernel dev root in as ``commons/``, re-link ``cara``.
2. **vendor** — run ``build:vendor-commons`` boot-free in the sandbox.
3. **verify** — ``commons/`` is gone and no ``commons.<pkg>`` reference survives.
4. **compile** — ``compileall`` over the shipped trees.
5. **import** — import every module under the kernel packages and prove
   ``commons`` is unimportable.
"""

from __future__ import annotations

import importlib
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Collection
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


# BuildKit reads ``<Dockerfile name>.dockerignore`` beside the Dockerfile
# before the context's ``.dockerignore``; a deployable that keeps both ships
# by the first, so reading the second would stage a tree the image never had.
_IGNORE_FILES = ("Dockerfile.dockerignore", ".dockerignore")

_Rule = tuple[bool, re.Pattern[str]]


def _docker_regex(pattern: str) -> re.Pattern[str]:
    """Docker's ``.dockerignore`` match: ``*`` and ``?`` stay inside one path
    segment, ``**`` spans segments, and a leading ``**/`` matches at the root
    too (``**/venv`` excludes ``venv``)."""
    out, i = [], 0
    while i < len(pattern):
        char = pattern[i]
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
            continue
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
            continue
        if char == "*":
            out.append("[^/]*")
        elif char == "?":
            out.append("[^/]")
        elif char == "[" and "]" in pattern[i + 1 :]:
            close = pattern.index("]", i + 1)
            body = pattern[i + 1 : close]
            out.append("[" + ("^" + body[1:] if body.startswith("!") else body) + "]")
            i = close
        elif char == "\\" and i + 1 < len(pattern):
            i += 1
            out.append(re.escape(pattern[i]))
        else:
            out.append(re.escape(char))
        i += 1
    return re.compile("".join(out) + "$")


def _dockerignore(root: Path) -> list[_Rule]:
    """The repository's own build-context exclusions, in file order."""
    path = next((root / n for n in _IGNORE_FILES if (root / n).is_file()), None)
    if path is None:
        return []
    rules: list[_Rule] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        negated = line.startswith("!")
        pattern = line[1:].strip() if negated else line
        pattern = pattern.lstrip("/").removeprefix("./").rstrip("/")
        if pattern:
            rules.append((negated, _docker_regex(pattern)))
    return rules


def _ignored(rel: str, name: str, rules: list[_Rule]) -> bool:
    """The last matching rule decides, as in Docker; an excluded directory is
    not walked, so a ``!`` re-include must name a path whose parents stay in
    (``fixtures/*`` + ``!fixtures/amazon``, not ``fixtures`` + ``!fixtures/amazon``)."""
    if name in _ALWAYS_SKIP:
        return True
    excluded = False
    for negated, regex in rules:
        if regex.match(rel):
            excluded = not negated
    return excluded


def _stage_tree(src: Path, dst: Path, rules: list[_Rule], prefix: str = "") -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for child in sorted(src.iterdir()):
        rel = f"{prefix}{child.name}"
        if _ignored(rel, child.name, rules):
            continue
        target = dst / child.name
        if child.is_symlink():
            continue  # the Dockerfile drops and recreates the two kernel links
        if child.is_dir():
            _stage_tree(child, target, rules, prefix=f"{rel}/")
        else:
            shutil.copy2(child, target)


def _run(python: str, args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # fixed argv, no shell
        [python, *args], cwd=cwd, capture_output=True, text=True, check=False
    )


class VendorDryRun:
    """Copy → vendor → compile → import, over the product's real tree."""

    @staticmethod
    def walk_kernel_modules(packages: Collection[str] | None = None) -> int:
        """Import every module under ``app/<kernel package>`` of the tree in the
        working directory; print each one that raises ``ImportError``, return 1.

        Step 5 runs this inside the sandbox and CI inside the built image, so
        both prove the same thing. ``ImportError`` only: a module that needs the
        booted container raises something else, and that is not what vendoring
        can break, but it is named and left out of the count because the imports
        below the raise never ran. A tree with no kernel modules at all is the
        wrong working directory, never a pass.
        """
        if packages is None:
            packages = Manifest.__dataclass_fields__["kernel_packages"].default
        modules = sorted(
            ".".join(
                path.parent.parts
                if path.stem == "__init__"
                else path.with_suffix("").parts
            )
            for package in sorted(packages)
            for path in Path("app", package).rglob("*.py")
        )
        if not modules:
            print(f"no kernel modules under {Path.cwd() / 'app'}")
            return 1
        broken: dict[str, str] = {}
        unverified: dict[str, str] = {}
        for module in modules:
            try:
                importlib.import_module(module)
            except ImportError as error:
                broken[module] = f"{type(error).__name__}: {error}"
            except Exception as error:
                unverified[module] = type(error).__name__
        for module, error in broken.items():
            print(f"{module}: {error}")
        for module, error in unverified.items():
            print(f"{module}: not verified, raised {error} without a booted app")
        imported = len(modules) - len(broken) - len(unverified)
        print(f"{imported} of {len(modules)} kernel modules import")
        return 1 if broken else 0

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
            script = (
                "import importlib.util as u\n"
                "from cara.architecture.VendorDryRun import VendorDryRun as v\n"
                f"code = v.walk_kernel_modules({sorted(manifest.kernel_packages)!r})\n"
                "assert u.find_spec('commons') is None, 'commons is still importable'\n"
                "raise SystemExit(code)\n"
            )
            imported = _run(python, ["-c", script], box)
            if imported.returncode != 0:
                result.failures.append(
                    "the vendored image does not import:\n"
                    + (imported.stdout + imported.stderr).strip()[-4000:]
                )
                return result
            result.steps.append(imported.stdout.strip().splitlines()[-1])

        return result
