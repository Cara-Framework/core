"""Vendor the dev-only ``commons/`` kernel into a service's ``app/`` tree.

A production/build step, project-agnostic: it knows only the framework's own
layout convention (a shared ``commons/`` kernel + each service's ``app/``
barrels), never any application's domain packages.

The kernel is DEV-ONLY (doctrine §2): the production image ships ``app.*``,
``cara.*`` and ``packages.*`` — nothing else. Run from a service root this
command:

1. auto-discovers every kernel package under ``commons/`` (a hardcoded list
   once shipped a broken image); only ``cara`` — the framework clone, a
   dependency rather than kernel content — is excluded,
2. flat-copies model modules into ``app/models`` and rewrites the barrel to
   per-name relative imports (the proven models-specific handling),
3. copies every other kernel package's whole tree into ``app/<pkg>/``,
   overwriting the dev barrel ``__init__.py`` with the package's real one,
4. rewrites every ``commons.<pkg>`` reference across the shipped tree
   (``app/``, ``database/migrations/``, ``packages/``) to ``app.<pkg>``,
5. materialises ``./cara`` as a real directory when it is a symlink into
   ``commons/``, and
6. deletes the entire ``commons/`` directory from the image tree.

It is a BOOT-FREE build command — ``cara.commands.run_build_command`` runs it
WITHOUT booting the application (no config, no providers, no secrets), so it
works inside a Docker build step where none of the production secrets exist.
``handle()`` does pure filesystem work.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from cara.commands import CommandBase
from cara.decorators import command

# ``commons.models`` collapse rewrites. Models are FLAT-copied into
# ``app/models`` (sub-packages disappear), so any dotted sub-path must fold
# into the barrel (``from commons.models.x import Y`` → ``from app.models
# import Y``) or the flat module name (``commons.models.x.Y`` →
# ``app.models.Y``), with no knowledge of what the sub-packages are called.
# Every OTHER kernel package ships verbatim, so its references keep their
# sub-paths and go through the generic word-boundary rewrite instead.
_FROM_IMPORT = re.compile(r"from commons\.models(?:\.\w+)* import")
_DOTTED_PREFIX = re.compile(r"\bcommons\.models(?:\.\w+)?\.")

_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc")


def _split_import_names(block: str) -> list[str]:
    """Names from a ``(a, b, # comment\\n c)`` block, tolerating ``# section``
    comment lines inside the parenthesised list."""
    names: list[str] = []
    for raw_line in block.replace("(", "").replace(")", "").split("\n"):
        line = raw_line.split("#", 1)[0]
        for tok in line.split(","):
            tok = tok.strip()
            if tok:
                names.append(tok)
    return names


@command(
    name="build:vendor-commons",
    help="Vendor the dev-only commons/ kernel into app/ and drop commons/ (production build step).",
)
class VendorCommonsCommand(CommandBase):
    """Materialise the shared kernel locally so the runtime image needs no commons.*."""

    def handle(self) -> int:
        root = Path.cwd()
        app_models = root / "app" / "models"
        if not app_models.exists():
            self.error("app/models not found — run from a service root")
            return 1
        commons = root / "commons"
        if not commons.exists():
            self.info("commons/ already vendored — nothing to do")
            return 0

        # 1) discover the kernel packages and VALIDATE BEFORE MUTATING.
        #    Doctrine §2 mandates exactly four kernel packages; ``cara`` (the
        #    framework clone), ``tests`` and ``docs`` are development artifacts
        #    that never ship. Anything else under commons/ is a doctrine
        #    violation and fails the build loudly — a permissive scan once
        #    shipped commons/tests into a production image's app tree.
        kernel_packages = {"models", "contracts", "gates", "shared"}
        development_only = {"cara", "tests", "docs"}
        found = sorted(
            child.name
            for child in commons.iterdir()
            if child.is_dir() and not child.name.startswith((".", "_"))
        )
        unknown = [
            name for name in found if name not in kernel_packages | development_only
        ]
        if unknown:
            self.error(
                f"unknown commons subdirectory(ies): {', '.join(unknown)} — doctrine §2 "
                f"allows exactly {sorted(kernel_packages)} "
                f"(+ dev-only {sorted(development_only)}); "
                "move the content into a kernel package or delete it"
            )
            return 1
        kernel = [name for name in found if name in kernel_packages]
        self.info(f"kernel packages: {', '.join(kernel) or '(none)'}")

        # pre-flight 2: duplicate model stems would silently clobber each other
        # in the flat copy — fail instead.
        models_src = commons / "models"
        if models_src.exists():
            stems: dict[str, Path] = {}
            for py in sorted(models_src.rglob("*.py")):
                if py.name == "__init__.py":
                    continue
                if py.name in stems:
                    self.error(
                        f"duplicate model module name {py.name!r}: {stems[py.name]} and {py} — "
                        "the flat app/models copy cannot hold both; rename one"
                    )
                    return 1
                stems[py.name] = py

        # pre-flight 3: a kernel package's app/<pkg> target must be a PURE dev
        # barrel (only __init__.py). Local members there are a doctrine §2
        # violation — local DI interfaces live in app/ports, and vendoring
        # NEVER merges barrels (a heuristic merge once nearly shipped; the
        # doctrine v1.2 answer is fail-fast).
        for name in kernel:
            if name == "models":
                continue
            target = root / "app" / name
            if not target.exists():
                continue
            locals_found = sorted(
                str(p.relative_to(target))
                for p in target.rglob("*.py")
                if "__pycache__" not in p.parts and p.name != "__init__.py"
            )
            if locals_found:
                self.error(
                    f"app/{name} carries local members ({', '.join(locals_found[:5])}"
                    f"{'…' if len(locals_found) > 5 else ''}) — doctrine §2: app/{name} is "
                    "exclusively the kernel barrel; move local DI interfaces to app/ports"
                )
                return 1

        # 2) models keep their proven handling: flat-copy every model module
        #    (at any depth) into the local app/models package, then turn the
        #    barrel's commons.models re-exports into relative ones.
        models_src = commons / "models"
        if models_src.exists():
            copied = 0
            for py in sorted(models_src.rglob("*.py")):
                if py.name == "__init__.py":
                    continue
                shutil.copy2(py, app_models / py.name)
                copied += 1
            self.info(f"copied {copied} model module(s) → app/models")
            self._rewrite_barrel(app_models / "__init__.py")

        # 3) every other kernel package ships verbatim: copy the whole tree
        #    into app/<pkg>/, the package's real __init__.py replacing the pure
        #    dev barrel (pre-flight 3 already guaranteed purity — local DI
        #    interfaces live in app/ports, never here).
        for name in kernel:
            if name == "models":
                continue
            shutil.copytree(
                commons / name, root / "app" / name, dirs_exist_ok=True, ignore=_IGNORE
            )
            self.info(f"copied commons/{name} → app/{name}")

        # 4) rewrite every remaining commons.<pkg> reference — dotted, from-,
        #    plain-import or string — across the shipped tree to app.<pkg>.
        #    Word-boundary safe: ``mycommons.gates`` and ``commons.gateskeeper``
        #    are not touched. The models collapse regexes run first (flat copy),
        #    the generic prefix-preserving rewrite covers everything else.
        generic = (
            re.compile(
                r"\bcommons\.(" + "|".join(re.escape(name) for name in kernel) + r")\b"
            )
            if kernel
            else None
        )
        scan = [root / "app", root / "database" / "migrations", root / "packages"]
        changed = 0
        for scan_dir in scan:
            if not scan_dir.exists():
                continue
            for py in scan_dir.rglob("*.py"):
                if "__pycache__" in str(py) or py.name.startswith("."):
                    continue
                text = py.read_text()
                new = _DOTTED_PREFIX.sub(
                    "app.models.", _FROM_IMPORT.sub("from app.models import", text)
                )
                if generic is not None:
                    new = generic.sub(r"app.\1", new)
                if new != text:
                    py.write_text(new)
                    changed += 1
        self.info(f"rewrote commons.* references in {changed} file(s)")

        # 5) materialise the framework: in the image ./cara must be a real
        #    directory, not a symlink into the (about to be deleted) commons/.
        cara_link = root / "cara"
        if cara_link.is_symlink():
            target = cara_link.resolve()
            if target.is_dir() and target.is_relative_to(commons.resolve()):
                cara_link.unlink()
                shutil.copytree(target, cara_link, ignore=_IGNORE)
                self.info("materialised ./cara (real copy of the framework clone)")

        # 6) the kernel is dev-only: drop commons/ entirely from the image tree.
        shutil.rmtree(commons)
        self.info("removed commons/ (kernel vendored into app/)")
        return 0

    def _rewrite_barrel(self, init_file: Path) -> None:
        """Convert ``from commons.models[...] import (...)`` in the app/models
        ``__init__`` into per-name relative imports (the models now live alongside)."""
        if not init_file.exists():
            return
        content = init_file.read_text()

        def repl(match: re.Match[str]) -> str:
            return "\n".join(
                f"from .{n} import {n}" for n in _split_import_names(match.group(1))
            )

        # multi-line ``import (...)`` FIRST (more specific), then single-line
        content = re.sub(
            r"from commons\.models(?:\.\w+)* import \((.*?)\)",
            repl,
            content,
            flags=re.DOTALL,
        )
        content = re.sub(
            r"from commons\.models(?:\.\w+)* import ([^\n(]+)", repl, content
        )
        init_file.write_text(content)
        self.info(f"rewrote {init_file.name} to relative imports")
