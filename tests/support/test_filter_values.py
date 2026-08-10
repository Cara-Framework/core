"""Pure filter-value canonicalization contracts."""

from __future__ import annotations

import subprocess
import sys

import pytest

from cara.support.FilterValues import csv_filter_values


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("active", ["active"]),
        ("draft,active", ["active", "draft"]),
        ("  draft , active ", ["active", "draft"]),
        ("active,active,draft", ["active", "draft"]),
        ("b,a,c", ["a", "b", "c"]),
    ],
    ids=["single", "sorted", "trimmed", "deduped", "reordered"],
)
def test_tokens_are_trimmed_deduped_and_sorted(raw: str, expected: list[str]) -> None:
    """One intent has one spelling regardless of client token order."""
    assert csv_filter_values(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [None, "", "   ", ",", ", ,,"],
    ids=["none", "empty", "whitespace", "bare-comma", "only-separators"],
)
def test_a_filter_with_no_tokens_is_absent_not_empty(raw: object) -> None:
    assert csv_filter_values(raw) is None


def test_empty_tokens_between_values_are_dropped() -> None:
    assert csv_filter_values("active,,draft,") == ["active", "draft"]


def test_non_string_values_are_stringified_before_splitting() -> None:
    assert csv_filter_values(42) == ["42"]


def test_zero_is_a_token_not_an_absent_filter() -> None:
    assert csv_filter_values("0") == ["0"]


def test_exported_only_from_http_independent_support() -> None:
    import cara.http
    from cara.support import csv_filter_values as exported

    assert exported is csv_filter_values
    assert "csv_filter_values" not in cara.http.__all__
    assert not hasattr(cara.http, "csv_filter_values")


def test_support_import_does_not_load_http() -> None:
    script = """
import sys
from cara.support import csv_filter_values
assert csv_filter_values('draft,active') == ['active', 'draft']
assert not any(name == 'cara.http' or name.startswith('cara.http.') for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", script], check=True)
