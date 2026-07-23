"""Vendor the shared ``commons/models`` package into a service's ``app/models``.

A production/build step, project-agnostic: it knows only the framework's own
layout convention (a shared ``commons/`` package + each service's
``app/models``), never any application's domain packages.

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

# ``commons.models`` optionally followed by any dotted sub-path — used to rewrite
# every reference (``from commons.models import``, ``from commons.models.x
# import``, ``from commons.models.x.Y import``) down to the local ``app.models``
# barrel, with no knowledge of what the sub-packages are called.
_FROM_IMPORT = re.compile(r"from commons\.models(?:\.\w+)* import")
_DOTTED_PREFIX = re.compile(r"\bcommons\.models(?:\.\w+)?\.")


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
    help="Vendor commons/models into app/models and drop commons/models (production build step).",
)
class VendorCommonsCommand(CommandBase):
    """Materialise the shared models locally so the runtime image needs no commons.models."""

    def handle(self) -> int:
        root = Path.cwd()
        commons = root / "commons"
        if not commons.exists():
            self.error("no commons/ directory here — nothing to vendor")
            return 1
        app_models = root / "app" / "models"
        if not app_models.exists():
            self.error("app/models not found — run from a service root")
            return 1
        models_src = commons / "models"
        if not models_src.exists():
            self.info("commons/models already vendored — nothing to do")
            return 0

        # 1) copy every model module (at any depth) into the local app/models package
        copied = 0
        for py in sorted(models_src.rglob("*.py")):
            if py.name == "__init__.py":
                continue
            shutil.copy2(py, app_models / py.name)
            copied += 1
        self.info(f"copied {copied} model module(s) → app/models")

        # 2) turn the app/models barrel's commons.models re-exports into relative ones
        self._rewrite_barrel(app_models / "__init__.py")

        # 3) rewrite every remaining commons.models reference across the shipped tree.
        #    EVERY commons subpackage that ships (support, jobs, repositories, and
        #    whatever gets added next) is auto-discovered — a hardcoded list here
        #    once missed commons/repositories and produced call-time
        #    ModuleNotFoundError in the vendored image. Only ``models`` (deleted
        #    below) and ``cara`` (the framework clone, project-agnostic — never
        #    imports commons.models) are excluded.
        scan = [
            root / "app",
            root / "database" / "migrations",
            root / "packages",
            *sorted(
                child
                for child in commons.iterdir()
                if child.is_dir()
                and not child.name.startswith((".", "_"))
                and child.name not in {"models", "cara"}
            ),
        ]
        changed = 0
        for scan_dir in scan:
            if not scan_dir.exists():
                continue
            for py in scan_dir.rglob("*.py"):
                if "__pycache__" in str(py) or py.name.startswith("."):
                    continue
                text = py.read_text()
                new = _DOTTED_PREFIX.sub("app.models.", _FROM_IMPORT.sub("from app.models import", text))
                if new != text:
                    py.write_text(new)
                    changed += 1
        self.info(f"rewrote commons.models imports in {changed} file(s)")

        # 4) drop the now-vendored models so the image no longer ships commons.models
        shutil.rmtree(models_src)
        self.info("removed commons/models (vendored into app/models)")
        return 0

    def _rewrite_barrel(self, init_file: Path) -> None:
        """Convert ``from commons.models[...] import (...)`` in the app/models
        ``__init__`` into per-name relative imports (the models now live alongside)."""
        if not init_file.exists():
            return
        content = init_file.read_text()

        def repl(match: re.Match[str]) -> str:
            return "\n".join(f"from .{n} import {n}" for n in _split_import_names(match.group(1)))

        # multi-line ``import (...)`` FIRST (more specific), then single-line
        content = re.sub(r"from commons\.models(?:\.\w+)* import \((.*?)\)", repl, content, flags=re.DOTALL)
        content = re.sub(r"from commons\.models(?:\.\w+)* import ([^\n(]+)", repl, content)
        init_file.write_text(content)
        self.info(f"rewrote {init_file.name} to relative imports")
