"""``cara.support.Console`` must not ship orphan stub classes.

What was here pre-fix
~~~~~~~~~~~~~~~~~~~~~
``AddCommandColors`` lived alongside ``HasColoredOutput`` but every
method on it called ``self.line(text, ...)`` — a method the class
never defined. The class had zero references across commons/, api/,
or services/ (the audit grep confirmed: only the declaration line
matched). Importing and instantiating it would have crashed on the
first ``error()`` / ``warning()`` call with
``AttributeError: 'AddCommandColors' object has no attribute 'line'``.

In a framework, a stub like this is worse than absent code:

* It signals a public extension point to readers (`grep -rn "class "`
  hits it like any other API surface).
* The orphan ``self.line()`` reference fools type-checkers into
  thinking ``line`` exists somewhere they should look for.
* Any caller who imports it crashes at first use, not at import —
  a runtime trap, not a static one.

Why pin this with a test
~~~~~~~~~~~~~~~~~~~~~~~~
Source-shape so the deletion can't quietly come back via a refactor
that copy-pastes the old pattern (e.g. someone resurrects it from
git history under a new name). If a future contributor needs the
class, they have to either (a) implement ``line()`` and call sites,
or (b) edit the allowlist below and document why.

The test does NOT try to import Console — keeping it parse-only
means the same pin works whether or not the bootstrap is reachable
from the test runner's import path.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


CONSOLE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "cara"
    / "support"
    / "Console.py"
)

# Classes that ARE expected to live in Console.py. Everything else
# raises a flag. Keep this list short — Console is meant to host the
# tiny coloured-output helper and nothing more.
EXPECTED_CLASSES: frozenset[str] = frozenset({
    "HasColoredOutput",
})


def _module_classes() -> list[ast.ClassDef]:
    tree = ast.parse(CONSOLE_PATH.read_text(encoding="utf-8"))
    return [n for n in tree.body if isinstance(n, ast.ClassDef)]


def test_console_file_is_readable():
    """Smoke check — pin the discovery path so a directory move
    fires here instead of letting the next test pass vacuously."""
    assert CONSOLE_PATH.is_file(), (
        f"Console.py missing at {CONSOLE_PATH}; did the support/ "
        f"layout change?"
    )


def test_console_exports_only_the_expected_classes():
    classes = _module_classes()
    names = {c.name for c in classes}
    unexpected = names - EXPECTED_CLASSES
    assert not unexpected, (
        f"Console.py declared unexpected class(es): {sorted(unexpected)}. "
        f"The previous AddCommandColors stub called self.line(...) — a "
        f"method the class never defined — so any instantiated user "
        f"crashed at first call. If you genuinely need a new helper "
        f"here, implement every method it references AND add the class "
        f"name to EXPECTED_CLASSES in this test with a one-line why."
    )
    missing = EXPECTED_CLASSES - names
    assert not missing, (
        f"Console.py is missing expected class(es): {sorted(missing)}. "
        f"Did the file get accidentally truncated?"
    )


@pytest.mark.parametrize("cls", _module_classes(), ids=lambda c: c.name)
def test_every_class_method_only_calls_defined_helpers(cls: ast.ClassDef):
    """A class whose methods call ``self.<x>()`` MUST define ``<x>``
    somewhere on the class — either as another method or inherited
    from a documented parent.

    Pre-fix ``AddCommandColors.error`` called ``self.line(...)`` and
    ``line`` did not exist; the test would have caught that at
    parse time. Now we keep the invariant explicitly so the next
    accidental import + call ALSO can't ship orphaned ``self.X``
    references."""
    defined: set[str] = {
        m.name
        for m in cls.body
        if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    # Parent classes the test doesn't try to introspect; methods
    # they declare are assumed to satisfy ``self.X`` references.
    has_external_parent = bool(cls.bases)

    referenced: set[str] = set()
    for node in ast.walk(cls):
        # Match ``self.<name>(...)`` call patterns.
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        ):
            referenced.add(node.func.attr)

    if has_external_parent:
        # We can't statically confirm a parent declared the method;
        # trust it and skip the check for this class.
        return

    missing = sorted(referenced - defined)
    assert not missing, (
        f"{cls.name} calls self.{missing!r} but those methods are not "
        f"defined on the class and the class has no parent the test "
        f"can defer to. Either implement them or remove the references "
        f"— shipping an orphan ``self.X`` call is a runtime crash "
        f"waiting for the first caller (the exact bug the deleted "
        f"AddCommandColors stub had)."
    )
