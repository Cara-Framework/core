"""Gate: the partition a product scopes its models by is the PRODUCT's word.

The engine's whole job here is one AST question — "is this base among the
class's bases?" — and that question is agnostic. The words wrapped around the
answer are not: "tenant", "workspace", "account", "shop" are vocabulary, and a
framework that ships one of them as a DEFAULT has compiled one product's domain
into every other product's documentation.

That is exactly how this subsystem broke before: a constant naming the other
product sat inside each copy, and the copies drifted the day one was fixed. So
the scope is default-free, and a product that declares none gets no column at
all rather than an empty one on every row.
"""

from __future__ import annotations

import pytest

from cara.docs import DocsManifest, ModelScope
from cara.docs.Inventory import gen_models

from ._fixtures import make_checkout, manifest_for, write

SCOPED = """
from app.models import Model
from app.scopes import MarksOwnership


class Widget(Model, MarksOwnership):
    __table__ = "widget"
"""

CENTRAL = """
from app.models import Model


class Region(Model):
    __table__ = "region"
"""


def _checkout_with_models(tmp_path):
    root = make_checkout(tmp_path, "alpha")
    write(root / "commons" / "models" / "Widget.py", SCOPED)
    write(root / "commons" / "models" / "Region.py", CENTRAL)
    return root


def _page(manifest) -> str:
    gen_models(manifest, "2026-01-01 00:00", lambda _line: None)
    return (manifest.reference / "models.md").read_text(encoding="utf-8")


def test_a_manifest_declares_no_scope_by_default():
    """No product's mixin name may be the framework's fallback.

    A default would be silently inherited by every product that never thought
    about scoping — and would start MATCHING the day one of them happened to
    define a class by that name, marking rows with a meaning nobody chose.
    """
    manifest = DocsManifest(product="alpha", root="/tmp", viewer_port=9999)

    assert manifest.model_scope is None


def test_the_column_is_omitted_entirely_when_no_scope_is_declared(tmp_path):
    root = _checkout_with_models(tmp_path)

    page = _page(manifest_for(root, "alpha"))

    assert "| Model | Table | File |" in page
    assert "MarksOwnership" not in page
    assert "_Total: 2 models._" in page


def test_the_declared_label_and_note_are_what_reach_the_page(tmp_path):
    root = _checkout_with_models(tmp_path)
    manifest = manifest_for(
        root,
        "alpha",
        model_scope=ModelScope(
            base="MarksOwnership",
            label="Owner-scoped",
            note="`MarksOwnership` filters every query by `owner_id`.",
        ),
    )

    page = _page(manifest)

    assert "_1 of 2 models are owner-scoped_" in page
    assert "`MarksOwnership` filters every query by `owner_id`." in page
    assert "| Model | Table | Owner-scoped | File |" in page


def test_only_the_models_carrying_the_base_are_ticked(tmp_path):
    root = _checkout_with_models(tmp_path)
    manifest = manifest_for(
        root,
        "alpha",
        model_scope=ModelScope(base="MarksOwnership", label="Scoped", note="n/a"),
    )

    page = _page(manifest)
    rows = {
        line.split("|")[1].strip(): line.split("|")[3].strip()
        for line in page.splitlines()
        if line.startswith("| ") and "`" in line
    }

    assert rows["Widget"] == "✅"
    assert rows["Region"] == "—"


def test_a_declared_scope_no_model_carries_still_omits_the_column(tmp_path):
    """An always-empty column is noise that trains readers to skip the table."""
    root = _checkout_with_models(tmp_path)
    manifest = manifest_for(
        root,
        "alpha",
        model_scope=ModelScope(base="NoSuchMixin", label="Scoped", note="n/a"),
    )

    assert "| Model | Table | File |" in _page(manifest)


def test_a_scope_cannot_be_declared_without_its_words():
    """A base with no label prints an unnamed column; a label with no note
    prints a tick mark nobody can act on. All three travel together."""
    with pytest.raises(TypeError):
        ModelScope(base="MarksOwnership")  # type: ignore[call-arg]
