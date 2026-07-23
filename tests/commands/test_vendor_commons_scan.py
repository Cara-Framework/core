"""``build:vendor-commons`` must rewrite EVERY shipped commons subpackage.

Regression: the rewrite scan list was once hardcoded to ``commons/support`` +
``commons/jobs``; ``commons/repositories`` (added later) was silently skipped,
so its ``from commons.models import ...`` lines survived vendoring while
``commons/models`` itself was deleted — a call-time ``ModuleNotFoundError``
in the production image. The scan now auto-discovers commons subpackages
(``models`` and ``cara`` excluded), so any future subpackage is covered too.
"""

from __future__ import annotations

from pathlib import Path

from cara.commands.core.VendorCommonsCommand import VendorCommonsCommand


def _make_tree(root: Path) -> None:
    """A minimal service root: app/models barrel + commons with 4 subpackages."""
    (root / "app" / "models").mkdir(parents=True)
    (root / "app" / "models" / "__init__.py").write_text(
        "from commons.models import (\n    User,\n)\n\n__all__ = [\n    \"User\",\n]\n"
    )
    (root / "app" / "jobs").mkdir()
    (root / "app" / "jobs" / "SomeJob.py").write_text(
        "from commons.models import User\n"
    )

    commons = root / "commons"
    (commons / "models" / "core").mkdir(parents=True)
    (commons / "models" / "__init__.py").write_text(
        "from .core.User import User\n\n__all__ = [\"User\"]\n"
    )
    (commons / "models" / "core" / "__init__.py").write_text("")
    (commons / "models" / "core" / "User.py").write_text("class User:\n    pass\n")

    for sub in ("support", "jobs", "repositories"):
        (commons / sub).mkdir()
        (commons / sub / "__init__.py").write_text("")
    (commons / "support" / "Gate.py").write_text(
        "def gate():\n    from commons.models import User\n    return User\n"
    )
    (commons / "jobs" / "Envelope.py").write_text(
        "from commons.models import User\n"
    )
    # the once-skipped subpackage — nested, function-local import (the real
    # failure shape: ChannelLifecycleRepository et al.)
    (commons / "repositories" / "billing").mkdir()
    (commons / "repositories" / "billing" / "__init__.py").write_text("")
    (commons / "repositories" / "SomeRepository.py").write_text(
        "def rows():\n    from commons.models import User\n    return User\n"
    )
    (commons / "repositories" / "billing" / "LedgerRepository.py").write_text(
        "import commons.models.core.User as user_mod\n"
    )
    # the framework clone must NOT be rewritten
    (commons / "cara" / "cara").mkdir(parents=True)
    (commons / "cara" / "cara" / "untouched.py").write_text(
        "TEXT = \"from commons.models import User\"\n"
    )


def test_vendor_rewrites_every_shipped_commons_subpackage(tmp_path, monkeypatch):
    _make_tree(tmp_path)
    monkeypatch.chdir(tmp_path)

    assert VendorCommonsCommand(application=None).handle() == 0

    # commons/models is gone; the barrel became relative imports
    assert not (tmp_path / "commons" / "models").exists()
    barrel = (tmp_path / "app" / "models" / "__init__.py").read_text()
    assert "from .User import User" in barrel
    assert "commons.models" not in barrel

    # every shipped subpackage — including repositories — was rewritten
    for rel in (
        "app/jobs/SomeJob.py",
        "commons/support/Gate.py",
        "commons/jobs/Envelope.py",
        "commons/repositories/SomeRepository.py",
        "commons/repositories/billing/LedgerRepository.py",
    ):
        text = (tmp_path / rel).read_text()
        assert "commons.models" not in text, f"{rel} still references commons.models"
        assert "app.models" in text

    # the framework clone was left alone
    assert "commons.models" in (tmp_path / "commons" / "cara" / "cara" / "untouched.py").read_text()


def test_vendor_is_idempotent(tmp_path, monkeypatch):
    _make_tree(tmp_path)
    monkeypatch.chdir(tmp_path)
    assert VendorCommonsCommand(application=None).handle() == 0
    # second run: commons/models already gone -> clean no-op success
    assert VendorCommonsCommand(application=None).handle() == 0
