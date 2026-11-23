"""An audit pointed at a root that does not exist must not pass.

Every tree audit walks ``root`` through one helper, and a root that was not a
directory (a renamed ``app/``, a ``parents[n]`` off by one) yielded no files:
no findings, a green guard, and not one module read. A named SUBDIRECTORY
that is absent stays a no-op on purpose — one declaration serves deployables
that do not all carry the same layers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cara.testing.audits import (
    AsyncDispatchAudit,
    CacheKeyAudit,
    HttpTimeoutAudit,
    MixinOverrideAudit,
    NestedExceptShadowAudit,
    NumericTruthinessAudit,
    SaltedHashKeyAudit,
)

AUDITS = [
    AsyncDispatchAudit(),
    CacheKeyAudit(),
    HttpTimeoutAudit(),
    MixinOverrideAudit(),
    NestedExceptShadowAudit(),
    NumericTruthinessAudit(["price"]),
    SaltedHashKeyAudit(),
]


@pytest.mark.parametrize("audit", AUDITS, ids=lambda audit: type(audit).__name__)
def test_a_root_that_is_not_a_directory_is_refused(audit, tmp_path: Path) -> None:
    with pytest.raises(NotADirectoryError, match="nothing would be scanned"):
        audit.scan_tree(tmp_path / "renamed_app", ["services"])
    (tmp_path / "app.py").write_text("x = 1\n")
    with pytest.raises(NotADirectoryError):
        audit.scan_tree(tmp_path / "app.py", ["services"])


@pytest.mark.parametrize("audit", AUDITS, ids=lambda audit: type(audit).__name__)
def test_an_absent_named_directory_is_still_skipped(audit, tmp_path: Path) -> None:
    assert audit.scan_tree(tmp_path, ["listeners"]) == []
