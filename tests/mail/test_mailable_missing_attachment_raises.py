"""``Mailable.attach()`` of a missing file raises instead of dropping it.

Live defect: ``attach()`` appended the file only ``if os.path.exists(path)``
and otherwise returned silently, so the mail went out without the
attachment it promised (an invoice mail with no invoice) and nothing failed.
"""

from __future__ import annotations

import pytest

from cara.mail import Mailable


def test_attaching_a_missing_file_raises_file_not_found(tmp_path) -> None:
    mailable = Mailable().to("user@example.com")

    with pytest.raises(FileNotFoundError, match="invoice.pdf"):
        mailable.attach("invoice.pdf", str(tmp_path / "missing.pdf"))
    assert mailable.to_dict()["attachments"] == []


def test_attaching_a_directory_raises_file_not_found(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        Mailable().attach("folder", str(tmp_path))


def test_attaching_an_existing_file_keeps_it(tmp_path) -> None:
    invoice = tmp_path / "invoice.pdf"
    invoice.write_bytes(b"%PDF-1.4")

    mailable = Mailable().attach("invoice.pdf", str(invoice))

    assert mailable.to_dict()["attachments"] == [
        {"name": "invoice.pdf", "path": str(invoice)}
    ]
