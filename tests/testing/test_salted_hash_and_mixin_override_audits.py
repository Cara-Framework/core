"""SaltedHashKeyAudit and MixinOverrideAudit — the two shapes they catch."""

from __future__ import annotations

from cara.testing.audits import MixinOverrideAudit, SaltedHashKeyAudit


def test_hash_in_key_is_found():
    src = 'def f(q):\n    return Cache.remember(f"s:{hash(q)}", 60, lambda: 1)\n'
    (finding,) = SaltedHashKeyAudit().scan_source(src, "a.py")
    assert finding.line == 2 and finding.method == "remember"


def test_hash_through_a_variable_is_found():
    src = "def f(q):\n    k = 's:' + str(hash(q))\n    Cache.forget(k)\n"
    assert len(SaltedHashKeyAudit().scan_source(src, "a.py")) == 1


def test_stable_digest_is_clean():
    src = (
        "import hashlib\n"
        "def f(q):\n"
        "    k = hashlib.sha256(q.encode()).hexdigest()\n"
        "    return Cache.get(k)\n"
    )
    assert SaltedHashKeyAudit().scan_source(src, "a.py") == []


def test_hash_outside_cache_calls_is_ignored():
    src = "def f(q):\n    return hash(q) % 8\n"
    assert SaltedHashKeyAudit().scan_source(src, "a.py") == []


def test_host_shadowing_its_mixin_is_found():
    mixin = "class ThingMixin:\n    def _visible(self, a, b):\n        return a\n"
    host = (
        "class Thing(ThingMixin):\n"
        "    def _visible(self, a):\n"
        "        return a\n"
        "    def __init__(self):\n"
        "        pass\n"
    )
    (finding,) = MixinOverrideAudit().scan_sources([(mixin, "m.py"), (host, "h.py")])
    assert (finding.path, finding.cls, finding.method, finding.mixin) == (
        "h.py",
        "Thing",
        "_visible",
        "ThingMixin",
    )


def test_mixin_of_a_mixin_is_followed():
    inner = "class InnerMixin:\n    def deep(self):\n        pass\n"
    outer = "class OuterMixin(InnerMixin):\n    def shallow(self):\n        pass\n"
    host = "class Host(OuterMixin):\n    def deep(self):\n        pass\n"
    findings = MixinOverrideAudit().scan_sources(
        [(inner, "i.py"), (outer, "o.py"), (host, "h.py")]
    )
    assert [f.mixin for f in findings] == ["InnerMixin"]


def test_non_mixin_base_override_is_allowed():
    base = "class Base:\n    def hook(self):\n        pass\n"
    host = "class Host(Base):\n    def hook(self):\n        pass\n"
    assert MixinOverrideAudit().scan_sources([(base, "b.py"), (host, "h.py")]) == []
