"""The documentation commands run without booting an application.

An editor hook fires ``maintenance:docs`` on every session start and CI runs it
without credentials, so the whole engine must stay reachable through the
bootless runner: no container dependencies in ``handle``, no import that drags
in the HTTP stack or a driver. The runner enforces the first property by
refusing to register a bootless command whose handler asks for a dependency;
this file pins the rest.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
import sys

import pytest

from cara.commands.core.DocsGenerateCommand import DocsGenerateCommand
from cara.commands.core.DocsServeCommand import DocsServeCommand

from ._fixtures import make_checkout, manifest_for, write


def test_handlers_declare_only_cli_options(tmp_path):
    for command in (DocsGenerateCommand, DocsServeCommand):
        parameters = list(inspect.signature(command.handle).parameters)
        assert parameters[0] == "self"
        assert all(
            inspect.signature(command.handle).parameters[name].default
            is not inspect.Parameter.empty
            for name in parameters[1:]
        ), f"{command.__name__}.handle takes a non-CLI parameter"


def test_a_command_without_a_manifest_says_so(tmp_path):
    with pytest.raises(TypeError, match="DocsManifest"):
        asyncio.run(DocsGenerateCommand().handle(claims=True))


def test_a_bound_manifest_drives_the_whole_run(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(root / "docs" / "internal" / "note.md", "# note\n")
    printed: list[str] = []

    class Bound(DocsGenerateCommand):
        manifest = manifest_for(root, "alpha")

    command = Bound()
    command.line = printed.append  # type: ignore[method-assign]

    assert asyncio.run(command.handle()) == 0
    reference = root / "docs" / "internal" / "reference"
    assert (reference / "routes.md").exists()
    assert (reference / "CLAIMS.md").exists()
    assert (reference / "FRESHNESS.md").exists()
    assert (root / "docs" / "nav.json").exists()
    assert any("product: alpha" in line for line in printed)


def _stale_checkout_with_a_forbidden_order(tmp_path):
    """A checkout holding one stale page AND one doc that orders a ban."""
    root = make_checkout(tmp_path, "alpha", atlas="`db:wipe` is FORBIDDEN.\n")
    # Stale: sources declared, no verified: date at all.
    write(
        root / "docs" / "internal" / "stale.md",
        "---\nsources:\n  - CLAUDE.md\n---\n\n# stale\n",
    )
    # Broken: prescribes the command the atlas bans, with no negation nearby.
    write(
        root / "docs" / "internal" / "orders.md",
        "---\nsources:\n  - CLAUDE.md\nverified: 2099-01-01\n---\n\n"
        "# orders\n\nRebuild the database with `craft db:wipe` first.\n",
    )
    return root


def test_check_mode_verifies_claims_even_when_a_page_is_stale(tmp_path):
    """Staleness must not silence the claim and forbidden-practice passes.

    `stale or verify_claims(...)` short-circuits, so ONE stale page would skip
    claim verification for the whole checkout and a doc ordering a banned
    command would never be read — green by omission, which is precisely what
    this gate exists to prevent.
    """
    root = _stale_checkout_with_a_forbidden_order(tmp_path)
    printed: list[str] = []

    class Bound(DocsGenerateCommand):
        manifest = manifest_for(root, "alpha")

    command = Bound()
    command.line = printed.append  # type: ignore[method-assign]

    assert asyncio.run(command.handle(check=True)) == 1
    assert any("STALE" in line for line in printed), printed
    # The forbidden order is REPORTED, not merely counted: the run that skips
    # this pass also exits 1 (on staleness alone), so the exit code cannot
    # distinguish the two — only the finding can.
    assert any("craft db:wipe" in line and "forbidden" in line for line in printed), (
        printed
    )


def test_check_mode_passes_a_fresh_and_truthful_checkout(tmp_path):
    root = make_checkout(tmp_path, "alpha", atlas="`db:wipe` is FORBIDDEN.\n")
    write(
        root / "docs" / "internal" / "fine.md",
        "---\nsources:\n  - CLAUDE.md\nverified: 2099-01-01\n---\n\n"
        "# fine\n\nThe atlas lives at `CLAUDE.md`.\n",
    )
    printed: list[str] = []

    class Bound(DocsGenerateCommand):
        manifest = manifest_for(root, "alpha")

    command = Bound()
    command.line = printed.append  # type: ignore[method-assign]

    assert asyncio.run(command.handle(check=True)) == 0, printed
    # Exit 0 must mean "both passes ran and found nothing", not "the claim
    # pass never ran" — so the claim summary has to be on the transcript.
    assert any(line.startswith("Claims:") for line in printed), printed


def _import_probe(script: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in [os.getcwd(), env.get("PYTHONPATH", "")] if part
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        env=env,
        text=True,
    )


def test_the_engine_is_usable_without_the_cli():
    """``cara.docs`` is imported by test suites and by the command alike.

    Reaching the verifier must not drag in the command runner, or a product's
    docs gate would pay for the CLI it never invokes.
    """
    result = _import_probe(
        "import sys\n"
        "import cara.docs\n"
        "assert 'cara.commands' not in sys.modules, sorted(sys.modules)\n"
        "assert 'cara.http.request.Request' not in sys.modules\n"
    )

    assert result.returncode == 0, result.stderr


def test_importing_the_commands_pulls_in_no_request_stack():
    """A boot-free command must not transitively import the HTTP stack."""
    result = _import_probe(
        "import sys\n"
        "from cara.commands.core.DocsGenerateCommand import DocsGenerateCommand\n"
        "from cara.commands.core.DocsServeCommand import DocsServeCommand\n"
        "assert 'cara.http.request.Request' not in sys.modules\n"
    )

    assert result.returncode == 0, result.stderr
