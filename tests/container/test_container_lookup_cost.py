"""Per-request cost of resolving an UNBOUND class through the container.

Stress run 2026-11-12 (synk api, ``stress/api``): every authenticated request
resolves ~20 repositories/services that are autowired, not bound. Each one
fell through ``_find_obj``'s direct/path/name lookups into the strategy-4
scan — ``isinstance``/``issubclass``/``isabstract`` against all ~130
bindings — and then raised ``MissingContainerBindingException`` so ``make``
could autowire it. Same class, same bindings, same answer, every request:
~3,200 ``inspect.isclass`` calls and ~13 ``inspect.signature`` parses per
request, a quarter of a session-authenticated request's CPU in one
single-process API.

The scan answer can only change when the bindings change, so it is memoised
against a version the binding table bumps on every mutation; constructor
signatures are memoised per class (keyed with its current ``__init__`` so a
patched constructor is re-read).
"""

from __future__ import annotations

import importlib
import inspect as real_inspect

import pytest

from cara.container import Container

container_mod = importlib.import_module("cara.container.Container")
resolution_mod = importlib.import_module("cara.container._ContainerResolution")


class _Repo:
    def __init__(self):
        self.ok = True


class _Base:
    pass


class _Impl(_Base):
    pass


def _container_with_bindings(n: int = 60) -> Container:
    c = Container()
    for i in range(n):
        c.bind(f"svc{i}", type(f"Svc{i}", (), {})())
    return c


class _CountingInspect:
    """Stand-in for the ``inspect`` module inside Container.py that counts
    ``isclass`` and ``signature`` calls and forwards everything else."""

    def __init__(self):
        self.isclass_calls = 0
        self.signature_calls = 0

    def isclass(self, obj):
        self.isclass_calls += 1
        return real_inspect.isclass(obj)

    def signature(self, obj, *a, **k):
        self.signature_calls += 1
        return real_inspect.signature(obj, *a, **k)

    def __getattr__(self, name):
        return getattr(real_inspect, name)


@pytest.fixture
def counting(monkeypatch):
    probe = _CountingInspect()
    monkeypatch.setattr(container_mod, "inspect", probe)
    monkeypatch.setattr(resolution_mod, "inspect", probe)
    return probe


def test_repeat_make_of_unbound_class_does_not_rescan_bindings(counting):
    c = _container_with_bindings(60)
    assert isinstance(c.make(_Repo), _Repo)  # first resolve may scan
    counting.isclass_calls = 0
    for _ in range(10):
        assert isinstance(c.make(_Repo), _Repo)
    # Without the memo every make walks all 60 bindings (>= 60 isclass each).
    assert counting.isclass_calls < 10 * 10, counting.isclass_calls


def test_repeat_make_does_not_reparse_constructor_signature(counting):
    c = _container_with_bindings(5)
    c.make(_Repo)
    counting.signature_calls = 0
    for _ in range(10):
        c.make(_Repo)
    assert counting.signature_calls == 0


def test_binding_added_after_a_miss_is_seen():
    c = _container_with_bindings(10)
    with pytest.raises(Exception):
        c._find_obj(_Base)  # memoised miss
    impl = _Impl()
    c.bind("impl", impl)
    assert c._find_obj(_Base) is impl


def test_binding_removed_after_a_hit_is_not_returned():
    c = _container_with_bindings(10)
    impl = _Impl()
    c.bind("impl", impl)
    assert c._find_obj(_Base) is impl  # memoised hit
    c.unbind("impl")
    with pytest.raises(Exception):
        c._find_obj(_Base)


def test_direct_dict_mutation_invalidates_memo():
    c = _container_with_bindings(3)
    with pytest.raises(Exception):
        c._find_obj(_Base)
    impl = _Impl()
    c.objects["raw"] = impl
    assert c._find_obj(_Base) is impl
    c.objects.pop("raw")
    with pytest.raises(Exception):
        c._find_obj(_Base)


def test_patched_constructor_signature_is_re_read():
    c = Container()

    class Svc:
        def __init__(self):
            self.v = 1

    c.make(Svc)

    def patched(self, repo: _Repo):
        self.v = repo

    Svc.__init__ = patched
    assert isinstance(c.make(Svc).v, _Repo)
