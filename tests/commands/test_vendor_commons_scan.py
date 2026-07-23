"""``build:vendor-commons`` must vendor the ENTIRE dev-only kernel (doctrine §2).

Regression history: the rewrite scan list was once hardcoded to two commons
subpackages; a later-added one was silently skipped, so its ``from
commons.models import ...`` lines survived vendoring while ``commons/models``
itself was deleted — a call-time ``ModuleNotFoundError`` in the production
image. The command now auto-discovers kernel packages and vendors ALL of them:

- ``models`` keep the proven flat-copy + relative-import barrel rewrite,
- every other kernel package (``contracts``, ``gates``, ``shared``, and any
  future one) is copied verbatim into ``app/<pkg>/``, its real ``__init__.py``
  replacing the dev barrel,
- every ``commons.<pkg>`` reference in the shipped tree (``app/``,
  ``database/migrations/``, ``packages/``) is rewritten to ``app.<pkg>``
  (word-boundary safe; models references collapse to the flat layout),
- ``./cara`` is materialised from a symlink into a real directory (content
  untouched — the framework is project-agnostic),
- ``commons/`` is deleted entirely: the image ships ``app.*``, ``cara.*``,
  ``packages.*`` and nothing else, and a second run is a clean no-op.
"""

from __future__ import annotations

import re
from pathlib import Path

from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand

_BOUNDARY_FIXTURE = 'NEIGHBOR = "mycommons.gates.Thing"\nNOT_A_PKG = "commons.gateskeeper.Thing"\n'


def _make_tree(root: Path) -> None:
    """A minimal service root: app barrels + a four-package kernel + cara symlink."""
    # --- app tree: models barrel, dev barrels, a job with cross-kernel imports
    (root / "app" / "models").mkdir(parents=True)
    (root / "app" / "models" / "__init__.py").write_text(
        'from commons.models import (\n    User,\n)\n\n__all__ = [\n    "User",\n]\n'
    )
    for pkg in ("contracts", "gates", "shared"):
        (root / "app" / pkg).mkdir()
        (root / "app" / pkg / "__init__.py").write_text(f"from commons.{pkg} import *  # dev barrel\n")
    (root / "app" / "jobs").mkdir()
    (root / "app" / "jobs" / "SomeJob.py").write_text(
        "from commons.models import User\n"
        "from commons.contracts.envelopes.SyncEnvelope import SyncEnvelope\n"
        "from commons.shared.catalog import slugify\n"
        "import commons.gates.persistence.LedgerRepository as ledger\n"
    )
    (root / "app" / "jobs" / "Boundary.py").write_text(_BOUNDARY_FIXTURE)
    (root / "database" / "migrations").mkdir(parents=True)
    (root / "database" / "migrations" / "create_users_table.py").write_text(
        'TABLE = "users"\nSOURCE = "commons.models.core.User"\n'
    )
    (root / "packages" / "acme").mkdir(parents=True)
    (root / "packages" / "acme" / "Connector.py").write_text("from commons.gates import PERMISSIONS\n")

    # --- the kernel
    commons = root / "commons"
    (commons / "models" / "core").mkdir(parents=True)
    (commons / "models" / "__init__.py").write_text('from .core.User import User\n\n__all__ = ["User"]\n')
    (commons / "models" / "core" / "__init__.py").write_text("")
    (commons / "models" / "core" / "User.py").write_text("class User:\n    pass\n")

    # contracts: nested subpackage, function-local model import (envelope shape)
    (commons / "contracts" / "envelopes").mkdir(parents=True)
    (commons / "contracts" / "__init__.py").write_text("from .envelopes.SyncEnvelope import SyncEnvelope\n")
    (commons / "contracts" / "envelopes" / "__init__.py").write_text("")
    (commons / "contracts" / "envelopes" / "SyncEnvelope.py").write_text(
        "class SyncEnvelope:\n    def body(self):\n        from commons.models import User\n        return User\n"
    )

    # gates: real __init__ (must replace the dev barrel) + nested persistence
    # with cross-refs to models AND contracts
    (commons / "gates" / "persistence").mkdir(parents=True)
    (commons / "gates" / "__init__.py").write_text("from .Permissions import PERMISSIONS\n")
    (commons / "gates" / "Permissions.py").write_text("PERMISSIONS = {}\n")
    (commons / "gates" / "persistence" / "__init__.py").write_text("")
    (commons / "gates" / "persistence" / "LedgerRepository.py").write_text(
        "from commons.models import User\nfrom commons.contracts import SyncEnvelope\n"
    )

    # shared: themed subpackage, plain-import of a nested model module (the
    # collapse shape: flat copy means commons.models.core.User → app.models.User)
    (commons / "shared" / "catalog").mkdir(parents=True)
    (commons / "shared" / "__init__.py").write_text("")
    (commons / "shared" / "catalog" / "__init__.py").write_text("from .Slug import slugify\n")
    (commons / "shared" / "catalog" / "Slug.py").write_text(
        "import commons.models.core.User as user_mod\n\n\ndef slugify(value):\n    return value\n"
    )

    # dev-only kernel tests: ignored by discovery, never shipped
    (commons / "tests").mkdir()
    (commons / "tests" / "test_kernel.py").write_text("def test_x():\n    pass\n")

    # the framework clone: never rewritten, materialised into ./cara
    (commons / "cara" / "cara").mkdir(parents=True)
    (commons / "cara" / "cara" / "__init__.py").write_text("")
    (commons / "cara" / "cara" / "untouched.py").write_text('TEXT = "from commons.models import User"\n')
    (root / "cara").symlink_to(commons / "cara" / "cara", target_is_directory=True)


def test_vendor_ships_the_full_kernel(tmp_path, monkeypatch):
    _make_tree(tmp_path)
    monkeypatch.chdir(tmp_path)

    assert VendorCommonsCommand(application=None).handle() == 0

    # the kernel is dev-only: commons/ is gone ENTIRELY
    assert not (tmp_path / "commons").exists()

    # models: flat copy + barrel rewritten to relative imports
    assert (tmp_path / "app" / "models" / "User.py").exists()
    barrel = (tmp_path / "app" / "models" / "__init__.py").read_text()
    assert "from .User import User" in barrel
    assert "commons.models" not in barrel

    # other kernel packages: whole tree in app/<pkg>, real __init__ replaced the dev barrel
    gates_init = (tmp_path / "app" / "gates" / "__init__.py").read_text()
    assert "from .Permissions import PERMISSIONS" in gates_init
    assert "dev barrel" not in gates_init
    assert (tmp_path / "app" / "contracts" / "envelopes" / "SyncEnvelope.py").exists()
    # contracts is a pure kernel barrel: real __init__ replaced the dev one
    merged = (tmp_path / "app" / "contracts" / "__init__.py").read_text()
    assert "from .envelopes.SyncEnvelope import SyncEnvelope" in merged
    assert "dev barrel" not in merged and "commons" not in merged
    assert (tmp_path / "app" / "gates" / "persistence" / "LedgerRepository.py").exists()
    assert (tmp_path / "app" / "shared" / "catalog" / "Slug.py").exists()

    # no shipped file references any kernel package as commons.* any more
    kernel_ref = re.compile(r"\bcommons\.(models|contracts|gates|shared)\b")
    for scan_dir in ("app", "database/migrations", "packages"):
        for py in (tmp_path / scan_dir).rglob("*.py"):
            assert not kernel_ref.search(py.read_text()), f"{py} still references commons.*"

    # cross-references landed on app.* with sub-paths preserved (non-models) …
    job = (tmp_path / "app" / "jobs" / "SomeJob.py").read_text()
    assert "from app.models import User" in job
    assert "from app.contracts.envelopes.SyncEnvelope import SyncEnvelope" in job
    assert "from app.shared.catalog import slugify" in job
    assert "import app.gates.persistence.LedgerRepository as ledger" in job
    ledger = (tmp_path / "app" / "gates" / "persistence" / "LedgerRepository.py").read_text()
    assert "from app.models import User" in ledger
    assert "from app.contracts import SyncEnvelope" in ledger
    envelope = (tmp_path / "app" / "contracts" / "envelopes" / "SyncEnvelope.py").read_text()
    assert "from app.models import User" in envelope
    assert "from app.gates import PERMISSIONS" in (tmp_path / "packages" / "acme" / "Connector.py").read_text()

    # … while models references collapse to the flat layout, strings included
    assert "import app.models.User as user_mod" in (tmp_path / "app" / "shared" / "catalog" / "Slug.py").read_text()
    assert 'SOURCE = "app.models.User"' in (tmp_path / "database" / "migrations" / "create_users_table.py").read_text()

    # word-boundary safety: near-miss names are untouched
    assert (tmp_path / "app" / "jobs" / "Boundary.py").read_text() == _BOUNDARY_FIXTURE

    # dev-only commons/tests was ignored — never vendored into the app tree
    assert not (tmp_path / "app" / "tests").exists()

    # the framework was materialised: ./cara is a REAL directory, content unrewritten
    cara_dir = tmp_path / "cara"
    assert cara_dir.is_dir() and not cara_dir.is_symlink()
    assert "commons.models" in (cara_dir / "untouched.py").read_text()


def test_vendor_is_idempotent(tmp_path, monkeypatch):
    _make_tree(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert VendorCommonsCommand(application=None).handle() == 0
    # second run: commons/ already gone -> clean no-op success, tree unchanged
    assert VendorCommonsCommand(application=None).handle() == 0
    assert not (tmp_path / "commons").exists()
    assert (tmp_path / "app" / "gates" / "persistence" / "LedgerRepository.py").exists()
    cara_dir = tmp_path / "cara"
    assert cara_dir.is_dir() and not cara_dir.is_symlink()


def test_vendor_fails_fast_on_unknown_commons_dir(tmp_path, monkeypatch):
    _make_tree(tmp_path)
    (tmp_path / "commons" / "junkpkg").mkdir()
    (tmp_path / "commons" / "junkpkg" / "Stray.py").write_text("X = 1\n")
    monkeypatch.chdir(tmp_path)
    assert VendorCommonsCommand(application=None).handle() == 1
    # nothing was mutated: commons/ still present, dev barrel untouched
    assert (tmp_path / "commons" / "models").exists()
    assert "commons.models" in (tmp_path / "app" / "models" / "__init__.py").read_text()


def test_vendor_fails_fast_on_local_members_in_kernel_barrel(tmp_path, monkeypatch):
    """Doctrine §2: app/<kernel-pkg> is exclusively the kernel barrel — local DI
    interfaces live in app/ports; the vendor never merges, it fails fast."""
    _make_tree(tmp_path)
    (tmp_path / "app" / "contracts" / "AccessContract.py").write_text("class AccessContract:\n    pass\n")
    monkeypatch.chdir(tmp_path)
    assert VendorCommonsCommand(application=None).handle() == 1
    # nothing was mutated
    assert (tmp_path / "commons" / "models").exists()
    assert (tmp_path / "app" / "contracts" / "AccessContract.py").exists()
