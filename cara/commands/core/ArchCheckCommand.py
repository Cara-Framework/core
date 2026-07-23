"""ArchCheckCommand: run the DOCTRINE Guard Pack against a product manifest.

A BOOT-FREE command (mirrors ``VendorCommonsCommand``'s contract): ``handle()``
loads only the product's ``app/architecture_manifest.py`` (a plain Python
module binding a ``Manifest`` — no app config, no DB, no secrets) and runs
pure-AST scanners over the filesystem. It never imports ``bootstrap`` and
never touches the container.

Every scanner in ``cara.architecture.scanners.REGISTRY`` runs by default;
``--scanner`` restricts the run to a comma-separated subset — useful for
iterating on one rule, or for a CI stage that only wants the fast subset.
Exit code is non-zero whenever any scanner reports a finding, so CI can
gate on it directly.
"""

from __future__ import annotations

from pathlib import Path

from cara.architecture.Manifest import Manifest
from cara.architecture.scanners import REGISTRY
from cara.commands import CommandBase
from cara.decorators import command


@command(
    name="arch:check",
    help="Run the DOCTRINE Guard Pack scanners against this product's architecture manifest.",
    options={
        "--manifest=?": "Path to architecture_manifest.py (default: app/architecture_manifest.py)",
        "--scanner=?": "Run only these scanners (comma-separated names; default: all)",
    },
)
class ArchCheckCommand(CommandBase):
    """Boot-free Guard Pack runner: manifest + pure-AST scanners only."""

    def handle(self, manifest: str | None = None, scanner: str | None = None) -> int:
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

        names = (
            sorted(s.strip() for s in scanner.split(",")) if scanner else sorted(REGISTRY)
        )
        unknown = [n for n in names if n not in REGISTRY]
        if unknown:
            self.error(
                f"unknown scanner(s): {', '.join(unknown)} — choices: {', '.join(sorted(REGISTRY))}"
            )
            return 1

        total = 0
        for name in names:
            findings = REGISTRY[name].scan(loaded)
            if findings:
                self.warning(f"{name}: {len(findings)} finding(s)")
                for finding in findings:
                    self.line(f"  {finding}")
            else:
                self.info(f"{name}: clean")
            total += len(findings)

        if total:
            self.error(f"{total} finding(s) across {len(names)} scanner(s)")
            return 1
        self.success(f"Guard Pack clean — {len(names)} scanner(s), 0 findings")
        return 0
