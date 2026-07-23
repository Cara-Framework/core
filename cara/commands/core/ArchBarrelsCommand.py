"""ArchBarrelsCommand: generate/verify a product's barrels from its manifest.

A BOOT-FREE command (mirrors ``VendorCommonsCommand``'s contract): loads
only ``app/architecture_manifest.py`` and runs ``BarrelGenerator`` — pure
filesystem + AST work, no app config, no DB, no secrets.

``--check`` (the default) reports drift without writing and exits non-zero
if any barrel would change or a name collision was found — the CI gate.
``--write`` regenerates every managed barrel in place; passing both is a
usage error.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.BarrelGenerator import BarrelGenerator
from cara.architecture.Manifest import Manifest
from cara.commands import CommandBase
from cara.decorators import command


@command(
    name="arch:barrels",
    help="Generate or verify this product's barrels from its architecture manifest.",
    options={
        "--manifest=?": "Path to architecture_manifest.py (default: app/architecture_manifest.py)",
        "--check": "Report drift without writing (default)",
        "--write": "Regenerate every managed barrel in place",
    },
)
class ArchBarrelsCommand(CommandBase):
    """Boot-free barrel generation: manifest + pure-AST regeneration only."""

    def handle(self, manifest: str | None = None) -> int:
        write = bool(self.option("write"))
        check = bool(self.option("check"))
        if write and check:
            self.error("pass either --check or --write, not both")
            return 1

        manifest_path = (
            Path(manifest)
            if manifest
            else Path.cwd() / "app" / "architecture_manifest.py"
        )
        if not manifest_path.exists():
            self.error(f"manifest not found: {manifest_path}")
            return 1
        try:
            loaded = Manifest.load(manifest_path)
        except Exception as exc:  # noqa: BLE001 - surfaced to the operator, not swallowed
            self.error(f"failed to load manifest {manifest_path}: {exc}")
            return 1

        plan = BarrelGenerator.write(loaded) if write else BarrelGenerator.check(loaded)

        if plan.collisions:
            self.error(f"{len(plan.collisions)} barrel name collision(s):")
            for collision in plan.collisions:
                self.line(f"  {collision}")
            return 1

        if not plan.changed:
            self.success("barrels up to date — 0 changed")
            return 0

        verb = "wrote" if write else "would write"
        self.info(f"{verb} {len(plan.changed)} barrel(s):")
        for path in plan.changed:
            self.line(f"  {path}")
        return 0 if write else 1
