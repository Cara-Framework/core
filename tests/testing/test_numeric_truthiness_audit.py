"""The numeric-truthiness audit, including the shapes it must NOT flag.

A guard that fires on correct code gets switched off, so the exemptions are
pinned as hard as the detections: an explicit ``is not None``, a coalesce to
zero, a bare local name, a comment, and a string are all legal.
"""

from __future__ import annotations

import textwrap

import pytest

from cara.testing.audits import NumericTruthinessAudit

FIELDS = frozenset({"price", "quantity", "margin"})


def _scan(source: str) -> list[str]:
    audit = NumericTruthinessAudit(FIELDS)
    return [
        str(finding) for finding in audit.scan_source(textwrap.dedent(source), "m.py")
    ]


class TestTernaryTruthiness:
    def test_a_zero_field_taking_the_else_branch_is_flagged(self):
        findings = _scan("value = row.price if row.price else fallback\n")
        assert len(findings) == 1
        assert "price" in findings[0]

    def test_an_explicit_none_check_is_not_flagged(self):
        assert _scan("value = row.price if row.price is not None else fallback\n") == []

    def test_a_bare_local_name_is_not_flagged(self):
        """``price`` alone may be anything; ``row.price`` names a column."""
        assert _scan("value = price if price else fallback\n") == []

    def test_an_unwatched_attribute_is_not_flagged(self):
        assert _scan("value = row.name if row.name else fallback\n") == []


class TestOrDefault:
    def test_a_non_zero_default_is_flagged(self):
        findings = _scan("total = row.price or 99\n")
        assert len(findings) == 1
        assert "or 99" in findings[0]

    def test_coalescing_to_zero_is_exempt(self):
        """``0 or 0`` is ``0`` — the idiom cannot corrupt what it defaults."""
        assert _scan("total = int(row.quantity or 0)\n") == []
        assert _scan("total = float(row.price or 0.0)\n") == []

    def test_a_none_default_is_flagged(self):
        findings = _scan("total = row.margin or None\n")
        assert len(findings) == 1
        assert "margin" in findings[0]

    def test_each_watched_operand_of_a_chain_is_checked(self):
        findings = _scan("total = row.price or row.margin or 99\n")
        assert len(findings) == 2

    def test_a_chain_ending_in_zero_only_flags_the_corrupted_link(self):
        """``row.price or row.margin or 0``: price is corrupted by margin."""
        findings = _scan("total = row.price or row.margin or 0\n")
        assert len(findings) == 1
        assert "price" in findings[0]


class TestNonCodeIsNotCode:
    def test_a_comment_is_not_a_finding(self):
        assert _scan("# never write row.price or 99 here\nvalue = 1\n") == []

    def test_a_docstring_mentioning_the_pattern_is_not_a_finding(self):
        assert _scan('"""Do not write row.price or 99."""\nvalue = 1\n') == []

    def test_a_multiline_expression_is_still_seen(self):
        """A line scanner misses this; the AST does not."""
        findings = _scan(
            """
            total = (
                row.price
                or 99
            )
            """
        )
        assert len(findings) == 1

    def test_unparseable_source_is_skipped_rather_than_crashing(self):
        assert _scan("def broken(:\n") == []


class TestTreeScan:
    def test_missing_directories_are_skipped(self, tmp_path):
        audit = NumericTruthinessAudit(FIELDS)
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "A.py").write_text("value = row.price or 99\n")
        findings = audit.scan_tree(tmp_path, ["services", "does-not-exist"])
        assert len(findings) == 1
        assert findings[0].path == "services/A.py"

    def test_pycache_is_never_scanned(self, tmp_path):
        audit = NumericTruthinessAudit(FIELDS)
        cache = tmp_path / "services" / "__pycache__"
        cache.mkdir(parents=True)
        (cache / "A.py").write_text("value = row.price or 99\n")
        assert audit.scan_tree(tmp_path, ["services"]) == []

    def test_the_report_names_every_site(self, tmp_path):
        audit = NumericTruthinessAudit(FIELDS)
        (tmp_path / "jobs").mkdir()
        (tmp_path / "jobs" / "A.py").write_text(
            "a = row.price or 99\nb = row.quantity or 5\n"
        )
        findings = audit.scan_tree(tmp_path, ["jobs"])
        report = audit.report(findings)
        assert "jobs/A.py:1" in report
        assert "jobs/A.py:2" in report


class TestBooleanContextIsNotDefaulting:
    """An ``or`` chain READ AS A CONDITION asks a presence question.

    ``if sig.phash or sig.width`` cannot corrupt a zero: it keeps no value, and
    a zero width answers "is anything present" exactly as an absent one does.
    Only a chain whose VALUE survives can substitute the default for the zero.
    """

    def test_an_or_chain_in_an_if_is_not_flagged(self):
        assert _scan("if row.price or row.quantity:\n    pass\n") == []

    def test_the_context_propagates_through_and(self):
        assert _scan("if row and (row.price or row.quantity):\n    pass\n") == []

    def test_the_context_propagates_through_not(self):
        assert _scan("if not (row.price or row.quantity):\n    pass\n") == []

    def test_a_while_test_is_a_condition(self):
        assert _scan("while row.price or row.quantity:\n    pass\n") == []

    def test_a_comprehension_filter_is_a_condition(self):
        assert _scan("kept = [r for r in rows if r.price or r.quantity]\n") == []

    def test_a_ternary_test_is_a_condition(self):
        assert _scan("value = a if row.price or row.quantity else b\n") == []

    def test_bool_of_a_chain_is_a_condition(self):
        assert _scan("flag = bool(row.price or row.quantity)\n") == []

    def test_the_same_chain_as_a_VALUE_is_still_flagged(self):
        """The exemption is about position, not about the expression."""
        findings = _scan("value = row.price or row.quantity\n")
        assert len(findings) == 1
        assert "price" in findings[0]

    def test_a_value_assigned_inside_an_if_body_is_still_flagged(self):
        """Being NEAR a condition is not being one."""
        findings = _scan("if row:\n    value = row.price or 99\n")
        assert len(findings) == 1


class TestFieldSetIsRequired:
    def test_an_empty_field_set_is_refused(self):
        """Silently passing everything is the worst failure mode a guard has."""
        with pytest.raises(ValueError, match="numeric column names"):
            NumericTruthinessAudit(frozenset())
