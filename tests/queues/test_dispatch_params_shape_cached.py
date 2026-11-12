"""``Bus.get_dispatch_params`` reads a job class's constructor shape ONCE.

Every dispatch serialized its job through ``get_dispatch_params`` three
times (``Bus.dispatch``, the delivery record, the signed payload), and each
call ran ``inspect.signature`` over the whole constructor MRO, ending at
``object.__init__``. Signing a builtin parses its text signature, which
copies ``sys.modules`` (thousands of entries) into a closure that becomes
cyclic garbage. Stress 2026-11-12 (sync area): one 1,000-listing drift plan
dispatched 1,000 ``PushSyncTaskJob`` and grew the worker 356 MB; four such
plans on one sync worker (concurrency 4) put it at 1,674 MB, past its 512 MB
recycle limit, so every plan wave restarted the worker. 2.6 ms of every
dispatch was this parse.

The constructor chain of a class does not change at runtime, so the shape
is a property of the class: computed once, the same answer every time.
"""

from __future__ import annotations

import inspect

from cara.queues import Bus


class _Base:
    def __init__(self, priority: int = 0, **kwargs):
        self.priority = priority
        self.kwargs = kwargs


class _Job(_Base):
    def __init__(self, sync_task_id: int, **kwargs):
        self.sync_task_id = sync_task_id
        self.derived = sync_task_id * 2  # not constructor input
        super().__init__(**kwargs)


def test_dispatch_params_keep_the_constructor_contract():
    job = _Job(7, priority=3, trace="t-1")
    assert Bus.get_dispatch_params(job) == {
        "sync_task_id": 7,
        "priority": 3,
        "trace": "t-1",
    }


def test_a_class_constructor_shape_is_read_once(monkeypatch):
    Bus.get_dispatch_params(_Job(1))  # the class's first dispatch may read it
    calls: list[object] = []
    real = inspect.signature

    def counting(obj, *args, **kwargs):
        calls.append(obj)
        return real(obj, *args, **kwargs)

    monkeypatch.setattr(inspect, "signature", counting)
    for task_id in range(50):
        assert Bus.get_dispatch_params(_Job(task_id))["sync_task_id"] == task_id
    assert calls == [], (
        f"{len(calls)} constructor signatures parsed for 50 dispatches of one class"
    )
