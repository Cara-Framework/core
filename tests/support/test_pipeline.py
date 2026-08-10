"""``cara.support.Pipeline`` has exactly ONE dispatch implementation.

Pre-fix the module advertised "two API surfaces" and ``__call__`` was a
second, hand-written run loop kept "for middleware capsule callers". It had
drifted from the canonical ``then()`` path in three visible ways, each pinned
below: it hardcoded ``pipe.handle`` so ``via()`` was silently discarded, it
could not run a plain-callable pipe at all, and it tracked executed instances
itself rather than through ``_resolve_pipe`` — so every change to instance
tracking had to be made twice, and the copy that was not maintained rotted.
"""

from __future__ import annotations

import asyncio

import pytest

from cara.support.Pipeline import Pipeline


class _MarkingPipe:
    """A pipe exposing two async entry points so ``via()`` is observable."""

    def __init__(self, application: object | None = None) -> None:
        self.application = application

    async def handle(self, request, next_call):
        return await next_call([*request, "handle"])

    async def process(self, request, next_call):
        return await next_call([*request, "process"])


async def _identity(payload):
    return payload


# --- via() is honoured on every entry point ---


def test_call_honours_via():
    """Pinned wrong behaviour: ``__call__`` ran ``handle`` and dropped ``via``.

    A caller who set ``.via("process")`` got no error and the wrong method.
    """
    pipeline = Pipeline([], application=None).through([_MarkingPipe]).via("process")

    assert asyncio.run(pipeline(_identity)) == ["process"]


def test_call_defaults_to_handle():
    pipeline = Pipeline([], application=None).through([_MarkingPipe])

    assert asyncio.run(pipeline(_identity)) == ["handle"]


# --- plain-callable pipes work on every entry point ---


def test_call_supports_plain_callable_pipes():
    """Pinned wrong behaviour: ``AttributeError: 'function' object has no
    attribute 'handle'`` — legal on ``then()``, fatal on ``__call__``."""

    async def shout(request, next_call):
        return await next_call(request.upper())

    assert asyncio.run(Pipeline("hi").through([shout])(_identity)) == "HI"


# --- one dispatch implementation ---


def test_call_delegates_to_then():
    """``__call__`` owns no dispatch logic of its own."""
    pipeline = Pipeline("payload")
    seen: list = []

    def fake_then(destination=None):
        seen.append(destination)
        return "delegated"

    pipeline.then = fake_then

    assert asyncio.run(pipeline(None)) == "delegated"
    assert seen == [None]


def test_call_enforces_the_depth_ceiling():
    pipeline = Pipeline("x").through([_MarkingPipe] * (Pipeline.MAX_DEPTH + 1))

    with pytest.raises(RuntimeError, match="exceeds maximum"):
        asyncio.run(pipeline(None))


def test_call_of_a_sync_chain_returns_the_value():
    """``then()`` returns a plain value for a fully sync chain; the ``async
    def`` alias must not blind-await it."""

    def increment(request, next_call):
        return next_call(request + 1)

    result = asyncio.run(Pipeline(1).through([increment])(lambda payload: payload * 10))

    assert result == 20


def test_call_without_a_handler_returns_the_payload():
    pipeline = Pipeline([], application=None).through([_MarkingPipe])

    assert asyncio.run(pipeline()) == ["handle"]


# --- terminate() sees every pipe the chain walked ---


def test_executed_instances_records_pre_instantiated_pipes():
    """Pinned wrong behaviour: ``_resolve_pipe`` appended ONLY the pipes it
    instantiated, so a pre-built middleware handed to ``through()`` never
    reached the conductors' ``terminate()`` sweep."""
    pipe = _MarkingPipe()
    pipeline = Pipeline([], application=None).through([pipe])

    asyncio.run(pipeline.then(_identity))

    assert pipeline.executed_instances == [pipe]


def test_executed_instances_records_class_pipes_once():
    pipeline = Pipeline([], application=None).through([_MarkingPipe])

    asyncio.run(pipeline(_identity))

    assert len(pipeline.executed_instances) == 1
    assert isinstance(pipeline.executed_instances[0], _MarkingPipe)
