"""DocsGenerateCommand: regenerate and verify a product's documentation.

A BOOT-FREE command (the same contract ``ArchCheckCommand`` keeps): it reads
only the filesystem — AST, regex and git — and never imports ``bootstrap``,
never touches the container, never opens a database or a broker. That is what
lets an editor hook run it on every session start and a CI job run it without
credentials.

Subclass it in a product and bind ``manifest`` to that product's
:class:`~cara.docs.DocsManifest.DocsManifest`; everything else is the framework's.
The subclass exists so the product's command inventory still lists the command
under its own name — it carries configuration, never logic.
"""

from __future__ import annotations

import time

from cara.commands.core.DocsCommand import DocsCommand
from cara.docs import (
    Say,
    claims_report,
    freshness,
    gen_nav,
    generate_reference,
    verify_claims,
    write_if_changed,
)


class DocsGenerateCommand(DocsCommand):
    """Regenerate code-derived reference pages, then verify the prose."""

    name = "maintenance:docs"
    help = (
        "Regenerate the documentation reference from code + freshness check "
        "(--check = check only)"
    )
    _cli_options = [
        {
            "name": "--check",
            "is_flag": True,
            "help": "Freshness + claim check only, no writes (exit 1 if stale/broken)",
        },
        {
            "name": "--claims",
            "is_flag": True,
            "help": "Claim verification only (exit 1 if a doc makes a broken claim)",
        },
    ]

    async def handle(self, check: bool = False, claims: bool = False) -> int:
        """Run the selected documentation generation or verification mode."""
        manifest = self._manifest()
        say: Say = self.line
        now = time.strftime("%Y-%m-%d %H:%M")
        say(f"docs generate — product: {manifest.product}, root: {manifest.root}")
        if claims:
            return 1 if verify_claims(manifest, say)[0] else 0
        if check:
            # BOTH passes always run, and only then is the verdict combined.
            # `stale or verify_claims(...)` reads harmlessly but SHORT-CIRCUITS:
            # a single stale page would skip claim and forbidden-practice
            # verification for the whole checkout, so a doc that ORDERS a
            # banned command sails through unread for as long as anything else
            # is stale — green by omission, the exact failure this gate exists
            # to catch.
            stale = freshness(manifest, write=False, now=now, say=say)
            broken, _unverifiable = verify_claims(manifest, say)
            return 1 if (stale or broken) else 0

        generate_reference(manifest, now, say)
        freshness(manifest, write=True, now=now, say=say)
        broken, unverifiable = verify_claims(manifest, say)
        write_if_changed(
            manifest.reference / "CLAIMS.md",
            claims_report(manifest, broken, unverifiable, now),
            "reference/CLAIMS.md "
            f"({len(broken)} broken / {len(unverifiable)} unverifiable)",
            say,
        )
        if broken:
            say(
                "  → fix: correct the claim in the doc, or mark the line "
                "`<!-- docs-check: ignore -->` if the example is deliberate"
            )
        gen_nav(manifest, say)
        say("done.")
        return 0
