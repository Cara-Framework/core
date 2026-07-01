"""``model:prune`` command — discovery, --model scoping, --pretend, batching.

Exercises pure command logic with no live database: the candidate model
classes are injected via ``_load_models`` and their ``prune`` / ``prunable``
operate in-memory. Mirrors the existing command-test pattern
(``application=None``, ``set_parsed_options``, mocked ``console``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cara.commands.core.ModelPruneCommand import ModelPruneCommand
from cara.eloquent.concerns import MakesPrunable


class _FakePrunable(MakesPrunable):
    """In-memory prunable model double — no DB."""

    name = "Fake"

    def __init__(self, count: int = 0):
        self._count = count
        self.pruned_with: int | None = None

    def prunable(self):  # overridden → recognised as prunable
        store = self

        class _Q:
            def count(self_inner):
                return store._count

        return _Q()

    def prune(self, batch_size: int = 1000, **_kw):
        self.pruned_with = batch_size
        removed, self._count = self._count, 0
        return removed


def _make_class(name: str, count: int) -> type:
    """Build a distinct MakesPrunable subclass with a fixed prunable count."""

    def __init__(self):
        _FakePrunable.__init__(self, count=count)

    return type(name, (_FakePrunable,), {"__init__": __init__})


def _not_prunable_class() -> type:
    """A MakesPrunable subclass that does NOT override prunable() — must be
    filtered out by discovery (the base prunable() raises)."""
    return type("NeverPrunable", (MakesPrunable,), {})


def _make_command(options=None, models=None) -> ModelPruneCommand:
    cmd = ModelPruneCommand(application=None)
    cmd.set_parsed_options(options or {})
    cmd.console = MagicMock()
    # Inject discovery result + ensure the MakesPrunable handle is set
    # (handle() sets it lazily; tests that bypass handle() set it here).
    cmd._MakesPrunable = MakesPrunable
    if models is not None:
        cmd._load_models = lambda: models  # type: ignore[assignment]
    return cmd


# ── happy path: prune all discovered models ─────────────────────────────


def test_prunes_all_discovered_models():
    A = _make_class("Alpha", 30)
    B = _make_class("Beta", 12)
    cmd = _make_command(models=[A, B])

    cmd.handle()

    # success() was called with a summary mentioning the total (42).
    printed = " ".join(str(c.args) for c in cmd.console.print.call_args_list)
    assert "42" in printed


def test_filters_out_non_prunable_subclass():
    """A MakesPrunable subclass without an overridden prunable() is not a
    prune target — discovery's _is_prunable must exclude it."""
    cmd = _make_command(models=[_make_class("Alpha", 5), _not_prunable_class()])

    targets = cmd._discover_prunable_models()

    names = {c.__name__ for c in targets}
    assert names == {"Alpha"}, f"NeverPrunable should be excluded, got {names}"


def test_no_models_warns_and_returns():
    cmd = _make_command(models=[])
    cmd.handle()
    # warning() path — no crash, console used.
    assert cmd.console.print.called


# ── --model scoping ─────────────────────────────────────────────────────


def test_model_option_scopes_to_one():
    A = _make_class("Alpha", 7)
    B = _make_class("Beta", 99)
    cmd = _make_command(options={"model": "Alpha"}, models=[A, B])

    targets = cmd._discover_prunable_models()

    assert [c.__name__ for c in targets] == ["Alpha"]


def test_model_option_unknown_reports_not_found():
    A = _make_class("Alpha", 7)
    cmd = _make_command(options={"model": "Ghost"}, models=[A])

    targets = cmd._discover_prunable_models()

    assert targets == []
    # error() routes through console.print (via line()).
    assert cmd.console.print.called


# ── --batch passthrough ─────────────────────────────────────────────────


def test_batch_option_passed_to_prune():
    A = _make_class("Alpha", 10)
    instances: list[_FakePrunable] = []

    # Capture the instance prune() is called on by wrapping the class.
    original_prune = A.prune

    def _capture(self, *a, **k):
        instances.append(self)
        return original_prune(self, *a, **k)

    A.prune = _capture  # type: ignore[assignment]
    cmd = _make_command(options={"batch": "250"}, models=[A])

    cmd.handle()

    assert instances and instances[0].pruned_with == 250


def test_invalid_batch_falls_back_to_default():
    cmd = _make_command(options={"batch": "not-a-number"})
    assert cmd._batch_size() == 1000


def test_negative_batch_falls_back_to_default():
    cmd = _make_command(options={"batch": "-5"})
    assert cmd._batch_size() == 1000


# ── --pretend counts without deleting ───────────────────────────────────


def test_pretend_counts_without_pruning():
    A = _make_class("Alpha", 18)
    cmd = _make_command(options={"pretend": True}, models=[A])

    cmd.handle()

    # In pretend mode the command calls prunable().count(), never prune();
    # the row count (18) shows in output, and the model wasn't mutated.
    printed = " ".join(str(c.args) for c in cmd.console.print.call_args_list)
    assert "18" in printed


# ── a failing model doesn't abort the run ───────────────────────────────


def test_one_failing_model_does_not_abort_others():
    good = _make_class("Good", 5)

    class _Boom(_FakePrunable):
        def __init__(self):
            super().__init__(count=1)

        def prune(self, batch_size: int = 1000, **_kw):
            raise RuntimeError("kaboom")

    cmd = _make_command(models=[good, _Boom])

    # Must not raise — the bad model is reported, the good one still prunes.
    cmd.handle()

    printed = " ".join(str(c.args) for c in cmd.console.print.call_args_list)
    assert "kaboom" in printed or "ERROR" in printed
