"""The cache-key audit: a constant key inside a function that takes arguments.

Everything that can vary is excluded, because a key that varies may still be
wrong but is no longer wrong on its face — and a guard that speculates is a
guard someone eventually deletes.
"""

from __future__ import annotations

import textwrap

from cara.testing.audits import CacheKeyAudit


def _scan(source: str) -> list[str]:
    audit = CacheKeyAudit()
    return [str(f) for f in audit.scan_source(textwrap.dedent(source), "m.py")]


class TestConstantKeys:
    def test_a_constant_key_in_a_parameterized_method_is_flagged(self):
        findings = _scan(
            """
            class R:
                def settings(self, tenant_id):
                    return Cache.remember("tenant:settings", 60, load)
            """
        )
        assert len(findings) == 1
        assert "tenant:settings" in findings[0]
        assert "tenant_id" in findings[0]

    def test_a_keyword_key_argument_is_read_too(self):
        findings = _scan(
            """
            class R:
                def settings(self, tenant_id):
                    return Cache.remember(key="tenant:settings", ttl=60)
            """
        )
        assert len(findings) == 1

    def test_a_keyword_only_parameter_still_counts_as_a_dimension(self):
        findings = _scan(
            """
            class R:
                def all(self, *, locale):
                    return Cache.remember("catalog:all", 60, load)
            """
        )
        assert len(findings) == 1

    def test_varargs_count_as_a_dimension(self):
        findings = _scan(
            """
            def load(*ids):
                return Cache.remember("things", 60, fetch)
            """
        )
        assert len(findings) == 1


class TestKeysThatCanVary:
    def test_an_fstring_key_is_not_flagged(self):
        assert (
            _scan(
                """
                class R:
                    def settings(self, tenant_id):
                        return Cache.remember(f"tenant:{tenant_id}:settings", 60, load)
                """
            )
            == []
        )

    def test_a_concatenated_key_is_not_flagged(self):
        assert (
            _scan(
                """
                class R:
                    def settings(self, tenant_id):
                        return Cache.remember("tenant:" + tenant_id, 60, load)
                """
            )
            == []
        )

    def test_a_computed_key_is_not_flagged(self):
        assert (
            _scan(
                """
                class R:
                    def settings(self, tenant_id):
                        return Cache.remember(self._key(tenant_id), 60, load)
                """
            )
            == []
        )


class TestFunctionsWithNothingToVaryBy:
    def test_a_self_only_method_may_use_a_constant_key(self):
        """Nothing about the call can differ, so nothing can bleed."""
        assert (
            _scan(
                """
                class R:
                    def all(self):
                        return Cache.remember("catalog:all", 60, load)
                """
            )
            == []
        )

    def test_a_classmethod_with_only_cls_may_use_a_constant_key(self):
        assert (
            _scan(
                """
                class R:
                    @classmethod
                    def all(cls):
                        return Cache.remember("catalog:all", 60, load)
                """
            )
            == []
        )

    def test_a_module_level_function_with_no_arguments_is_fine(self):
        assert (
            _scan('def all():\n    return Cache.remember("catalog:all", 60, load)\n')
            == []
        )


class TestOtherReceivers:
    def test_another_receivers_remember_is_not_this_rule(self):
        assert (
            _scan(
                """
                class R:
                    def settings(self, tenant_id):
                        return self.store.remember("tenant:settings", 60)
                """
            )
            == []
        )

    def test_a_product_may_point_the_rule_at_its_own_seam(self):
        audit = CacheKeyAudit(receiver="Memo", method="around")
        findings = audit.scan_source(
            'def f(a):\n    return Memo.around("k", 1)\n', "m.py"
        )
        assert len(findings) == 1


class TestTreeScan:
    def test_findings_carry_the_relative_path(self, tmp_path):
        (tmp_path / "repositories").mkdir()
        (tmp_path / "repositories" / "A.py").write_text(
            'def f(a):\n    return Cache.remember("k", 1)\n'
        )
        audit = CacheKeyAudit()
        findings = audit.scan_tree(tmp_path, ["repositories"])
        assert [f.path for f in findings] == ["repositories/A.py"]
        assert "repositories/A.py:2" in audit.report(findings)

    def test_an_absent_directory_is_skipped(self, tmp_path):
        assert CacheKeyAudit().scan_tree(tmp_path, ["nope"]) == []
