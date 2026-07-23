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
        _KERNEL = {"models", "contracts", "gates", "shared"}
        _DEV_ONLY = {"cara", "tests", "docs"}
        found = sorted(
            child.name
            for child in commons.iterdir()
            if child.is_dir() and not child.name.startswith((".", "_"))
        )
        unknown = [name for name in found if name not in _KERNEL | _DEV_ONLY]
        if unknown:
            self.error(
                f"unknown commons subdirectory(ies): {', '.join(unknown)} — doctrine §2 "
                f"allows exactly {sorted(_KERNEL)} (+ dev-only {sorted(_DEV_ONLY)}); "
                "move the content into a kernel package or delete it"
            )
            return 1
        kernel = [name for name in found if name in _KERNEL]
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

        # pre-flight 3: in a collision package (app/<pkg> with local members),
        # a kernel file sharing a LOCAL file's name would silently overwrite it.
        for name in kernel:
            if name == "models":
                continue
            target = root / "app" / name
            if not target.exists():
                continue
            local_files = {
                p.relative_to(target)
                for p in target.rglob("*.py")
                if "__pycache__" not in p.parts and p.name != "__init__.py"
            }
            if not local_files:
                continue
            kernel_files = {
                p.relative_to(commons / name)
                for p in (commons / name).rglob("*.py")
                if "__pycache__" not in p.parts and p.name != "__init__.py"
            }
            overlap = sorted(str(p) for p in local_files & kernel_files)
            if overlap:
                self.error(
                    f"app/{name} and commons/{name} both own: {', '.join(overlap)} — "
                    "rename the local file(s); vendoring must never silently overwrite local code"
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
        #    into app/<pkg>/. A PURE dev barrel (the target package holds only
        #    an __init__.py) is simply overwritten by the package's real
        #    __init__.py. A COLLISION package (the app tree has local members
        #    of its own — e.g. an app/contracts holding local DI contracts) is
        #    MERGED instead: kernel contents are copied in, the local
        #    __init__.py keeps its local statements, loses every dev-only
        #    ``commons.``-referencing statement (dead once commons/ is gone),
        #    gains the kernel __init__'s (relative) statements, and the two
        #    ``__all__`` lists are unioned. Overwriting a collision barrel once
        #    shipped an image whose request path lost its local contracts.
        for name in kernel:
            if name == "models":
                continue
            target = root / "app" / name
            local_init = target / "__init__.py"
            collision = target.exists() and any(
                p.suffix == ".py" and p.name != "__init__.py"
                for p in target.rglob("*.py")
                if "__pycache__" not in p.parts
            )
            local_source = local_init.read_text() if (collision and local_init.exists()) else None
            shutil.copytree(commons / name, target, dirs_exist_ok=True, ignore=_IGNORE)
            if local_source is not None:
                self._merge_collision_barrel(local_init, local_source, (commons / name / "__init__.py"))
                self.info(f"copied commons/{name} → app/{name} (merged collision barrel)")
            else:
                self.info(f"copied commons/{name} → app/{name}")

        # 4) rewrite every remaining commons.<pkg> reference — dotted, from-,
        #    plain-import or string — across the shipped tree to app.<pkg>.
        #    Word-boundary safe: ``mycommons.gates`` and ``commons.gateskeeper``
        #    are not touched. The models collapse regexes run first (flat copy),
        #    the generic prefix-preserving rewrite covers everything else.
        generic = (
            re.compile(r"\bcommons\.(" + "|".join(re.escape(name) for name in kernel) + r")\b")
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
                new = _DOTTED_PREFIX.sub("app.models.", _FROM_IMPORT.sub("from app.models import", text))
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

    def _merge_collision_barrel(self, init_file: Path, local_source: str, kernel_init: Path) -> None:
        """Merge a collision package's local ``__init__`` with the kernel's.

        Keeps every local top-level statement that does not reference the
        (about to vanish) ``commons.`` namespace and is not ``__all__``;
        appends the kernel ``__init__``'s statements (already package-relative)
        minus its docstring and ``__all__``; then emits one unioned,
        alphabetical ``__all__``. Pure AST slicing — no semantic rewrites.
        """
        import ast

        def strip_dev_blocks(source: str) -> str:
            """Remove ``# --- dev-only kernel wiring`` … ``# --- end dev-only`` blocks."""
            lines: list[str] = []
            in_block = False
            for line in source.splitlines(keepends=True):
                stripped = line.strip()
                if stripped.startswith("# --- dev-only kernel wiring"):
                    in_block = True
                    continue
                if stripped.startswith("# --- end dev-only kernel wiring"):
                    in_block = False
                    continue
                if not in_block:
                    lines.append(line)
            return "".join(lines)

        def parts(source: str) -> tuple[list[str], list[str]]:
            source = strip_dev_blocks(source)
            tree = ast.parse(source)
            kept: list[str] = []
            names: list[str] = []
            for node in tree.body:
                segment = ast.get_source_segment(source, node) or ""
                is_all = (
                    isinstance(node, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets)
                )
                if is_all:
                    try:
                        names.extend(str(x) for x in ast.literal_eval(node.value))
                    except Exception:
                        pass
                    continue
                if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                    continue  # docstrings handled separately
                if "commons." in segment or "commons import" in segment:
                    continue  # dev-only wiring; commons/ is deleted below
                kept.append(segment)
            return kept, names

        local_kept, local_names = parts(local_source)
        kernel_source = kernel_init.read_text() if kernel_init.exists() else ""
        kernel_kept, kernel_names = parts(kernel_source)

        docstring = ""
        try:
            doc = ast.get_docstring(ast.parse(local_source))
            if doc:
                docstring = f'"""{doc}"""\n\n'
        except SyntaxError:
            pass

        merged_names = sorted(set(local_names) | set(kernel_names))
        # kernel statements FIRST: any surviving local statement may lean on
        # kernel-bound names (e.g. ``envelopes``), never the other way around.
        body = "\n".join([*kernel_kept, "", *local_kept]).strip("\n")
        all_block = "\n\n__all__ = [\n" + "".join(f'    "{n}",\n' for n in merged_names) + "]\n"
        init_file.write_text(docstring + body + "\n" + all_block)

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
