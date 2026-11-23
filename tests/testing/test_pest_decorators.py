"""``@it`` / ``@describe`` suites must actually be collected by pytest.

pytest collects by the name a module or class BINDS, never by ``__name__``.
The decorators only renamed their target, so the suite the module docstring
shows (``class _Suite`` / ``def _``) was never collected: "no tests ran",
green, whatever the bodies asserted.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import cara
from cara.testing import describe, it

SUITE = """
from cara.testing import describe, it


@it("adds at module level")
def _():
    assert 1 + 1 == 2


@describe("price validation")
class _Suite:
    @it("accepts a positive price")
    def _(self):
        assert True

    @it("rejects a null price")
    def _(self):
        raise AssertionError("this body ran")
"""


def test_pytest_collects_and_runs_a_described_suite(tmp_path: Path) -> None:
    (tmp_path / "test_suite.py").write_text(SUITE)
    environment = dict(os.environ, PYTHONPATH=str(Path(cara.__file__).parents[1]))
    environment.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_suite.py"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    report = result.stdout + result.stderr
    assert "1 failed, 2 passed" in report, report
    assert "test_suite.py::TestPriceValidation::test_rejects_a_null_price" in report
    assert "this body ran" in report


def _define(source: str) -> dict:
    namespace: dict = {"it": it, "describe": describe}
    exec(source, namespace)
    return namespace


def test_the_placeholder_is_bound_under_its_collectable_name() -> None:
    namespace = _define(
        "@it('rejects null prices')\n"
        "def _(): ...\n"
        "@describe('alerts: edge cases')\n"
        "class _Suite:\n"
        "    @it('fires once')\n"
        "    def _(self): ...\n"
    )
    assert namespace["test_rejects_null_prices"].__name__ == "test_rejects_null_prices"
    suite = namespace["TestAlertsEdgeCases"]
    assert suite is namespace["_Suite"]
    assert suite.test_fires_once.__pest_description__ == "fires once"


def test_a_definition_pytest_already_collects_is_not_bound_twice() -> None:
    """``def test_x`` / ``class TestX`` run under the name they were written
    with; a second binding would run them twice."""
    namespace = _define(
        "@it('does the thing')\n"
        "def test_original(): ...\n"
        "@describe('the thing')\n"
        "class TestOriginal: ...\n"
    )
    assert "test_does_the_thing" not in namespace
    assert "TestTheThing" not in namespace
    assert namespace["test_original"].__name__ == "test_does_the_thing"


def test_two_descriptions_with_one_test_name_are_refused() -> None:
    """Rebinding would drop the first test without a word."""
    with pytest.raises(ValueError, match="test_rejects_null"):
        _define(
            "@it('rejects null')\n"
            "def _(): raise AssertionError('never ran')\n"
            "@it('Rejects NULL!')\n"
            "def _(): ...\n"
        )


def test_a_decorator_applied_inside_a_function_binds_nothing() -> None:
    @it("stays local")
    def placeholder():
        return None

    assert placeholder.__name__ == "test_stays_local"
    assert "test_stays_local" not in globals()
