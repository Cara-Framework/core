"""The product-blind test-support helpers both products' suites share."""

from __future__ import annotations

import ast
import sys
import types

import pytest

from cara.testing import (
    PUBLIC_ID_LENGTH,
    assert_cli_prints,
    dotted_name,
    drop_modules,
    public_id,
    strip_sgr,
    terminal_name,
    unique,
)


# ── minters ──────────────────────────────────────────────────────────────


def test_unique_is_lowercase_prefixed_and_never_repeats() -> None:
    tokens = {unique("Sku-") for _ in range(50)}
    assert len(tokens) == 50
    assert all(token == token.lower() and token.startswith("sku-") for token in tokens)


def test_public_id_reads_the_model_prefix_and_stays_bounded() -> None:
    class Thing:
        __public_id_prefix__ = "THG"

    minted = public_id(Thing)
    assert minted.startswith("THG")
    assert len(minted) == len("THG") + 26

    class Long:
        __public_id_prefix__ = "X" * 10

    assert len(public_id(Long)) == PUBLIC_ID_LENGTH


def test_public_id_refuses_a_restated_literal() -> None:
    # ``public_id("PP")`` is the habit this helper ends: a string is not a
    # model, and silently minting a bare ULID from it would hide the mistake.
    with pytest.raises(TypeError, match="takes a model class"):
        public_id("PP")


def test_public_id_of_an_unprefixed_model_is_a_bare_ulid() -> None:
    class Plain:
        pass

    assert len(public_id(Plain)) == 26


# ── module isolation ─────────────────────────────────────────────────────


@pytest.fixture
def _fake_package():
    package = types.ModuleType("_cara_iso_pkg")
    child = types.ModuleType("_cara_iso_pkg.cfg")
    package.cfg = child
    sys.modules["_cara_iso_pkg"] = package
    sys.modules["_cara_iso_pkg.cfg"] = child
    yield package, child
    sys.modules.pop("_cara_iso_pkg", None)
    sys.modules.pop("_cara_iso_pkg.cfg", None)


def test_drop_modules_restores_both_the_registry_and_the_parent_attribute(
    _fake_package,
) -> None:
    package, original = _fake_package
    patch = pytest.MonkeyPatch()
    drop_modules(patch, "_cara_iso_pkg.cfg")
    assert "_cara_iso_pkg.cfg" not in sys.modules

    fresh = types.ModuleType("_cara_iso_pkg.cfg")
    sys.modules["_cara_iso_pkg.cfg"] = fresh
    package.cfg = fresh

    patch.undo()
    assert sys.modules["_cara_iso_pkg.cfg"] is original
    assert package.cfg is original


def test_drop_modules_tolerates_a_module_never_imported() -> None:
    patch = pytest.MonkeyPatch()
    drop_modules(patch, "_cara_iso_never_loaded.sub")
    patch.undo()
    assert "_cara_iso_never_loaded.sub" not in sys.modules


# ── CLI output ───────────────────────────────────────────────────────────

_SPLIT_OPTION = "\x1b[1;36m-\x1b[0m\x1b[1;36m-port\x1b[0m"


def test_strip_sgr_rejoins_an_option_rich_painted_in_two() -> None:
    assert "--port" not in _SPLIT_OPTION
    assert strip_sgr(_SPLIT_OPTION) == "--port"


def test_assert_cli_prints_returns_plain_text_and_fails_with_the_capture() -> None:
    assert assert_cli_prints(f"Usage: {_SPLIT_OPTION}", "--port", "serve") == (
        "Usage: --port"
    )
    with pytest.raises(AssertionError, match=r"`craft serve` did not print '--host'"):
        assert_cli_prints(_SPLIT_OPTION, "--host", "serve")


# ── AST names ────────────────────────────────────────────────────────────


def _expr(source: str) -> ast.expr:
    return ast.parse(source, mode="eval").body


@pytest.mark.parametrize(
    ("source", "terminal", "dotted"),
    [
        ("bind", "bind", "bind"),
        ("Registry.bind", "bind", "Registry.bind"),
        ("a.b.c", "c", "a.b.c"),
        ("foo().bar.baz", "baz", "bar.baz"),
        ("items[0]", None, ""),
    ],
)
def test_terminal_and_dotted_names(source: str, terminal, dotted: str) -> None:
    node = _expr(source)
    assert terminal_name(node) == terminal
    assert dotted_name(node) == dotted
