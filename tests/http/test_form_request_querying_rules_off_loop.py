"""A FormRequest whose rules query the database validates on a worker thread.

``exists`` / ``unique`` / ``batch_exists`` run a synchronous query inside
``Validation.make``; inline, every such request held the event loop for a
database round trip. ``FormRequest._run_rules`` now sends the whole run
(after-callbacks and the verdict included) through
``ExecutionContext.run_in_thread`` — except while this context holds an open
transaction, whose uncommitted rows a worker thread's connection could not
see, and except when no rule queries at all.
"""

from __future__ import annotations

import asyncio
import importlib
import threading
from types import SimpleNamespace

import pytest

from cara.http.requests.FormRequest import FormRequest

_form_request = importlib.import_module("cara.http.requests.FormRequest")


class _Validator:
    """Records the thread each phase of the run happened on."""

    def __init__(self, data, threads: dict[str, int]) -> None:
        self._data = data
        self._threads = threads

    def after(self, _callback) -> None:
        self._threads.setdefault("after", threading.get_ident())

    def fails(self) -> bool:
        self._threads.setdefault("fails", threading.get_ident())
        return False

    def validated(self):
        return dict(self._data)


class _Request:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.params: dict = {}

    async def all(self):
        return dict(self.payload)


def _form(rules: dict) -> FormRequest:
    class _Form(FormRequest):
        refuse_unknown_keys = False

        def rules(self):
            return rules

    return _Form()


@pytest.fixture
def threads(monkeypatch) -> dict[str, int]:
    seen: dict[str, int] = {}

    def make(data, _rules, _messages=None):
        seen["make"] = threading.get_ident()
        return _Validator(data, seen)

    monkeypatch.setattr(_form_request, "Validation", SimpleNamespace(make=make))
    return seen


def _transaction_level(monkeypatch, level) -> None:
    def transaction_level():
        if isinstance(level, Exception):
            raise level
        return level

    monkeypatch.setattr(
        _form_request, "DB", SimpleNamespace(transaction_level=transaction_level)
    )


def _validate(rules: dict) -> tuple[dict, int]:
    async def run():
        loop_thread = threading.get_ident()
        validated = await _form(rules).validate_request(_Request({"sku": "A-1"}))
        return validated, loop_thread

    return asyncio.run(run())


@pytest.mark.parametrize(
    "rule",
    ["required|exists:products,sku", "unique:products,sku", "batch_exists:products"],
)
def test_a_querying_rule_validates_on_a_worker_thread(monkeypatch, threads, rule):
    _transaction_level(monkeypatch, 0)

    validated, loop_thread = _validate({"sku": rule})

    assert validated == {"sku": "A-1"}
    assert threads["make"] != loop_thread
    # The verdict is settled where the rules ran, not back on the loop.
    assert threads["after"] == threads["fails"] == threads["make"]


def test_an_open_transaction_keeps_the_run_inline(monkeypatch, threads):
    _transaction_level(monkeypatch, 1)

    _validated, loop_thread = _validate({"sku": "exists:products,sku"})

    assert threads["make"] == loop_thread


def test_an_unknown_transaction_depth_keeps_the_run_inline(monkeypatch, threads):
    _transaction_level(monkeypatch, RuntimeError("no connection configured"))

    _validated, loop_thread = _validate({"sku": "exists:products,sku"})

    assert threads["make"] == loop_thread


def test_rules_that_query_nothing_stay_inline(monkeypatch, threads):
    _transaction_level(monkeypatch, 0)

    _validated, loop_thread = _validate({"sku": "required|string|max:40"})

    assert threads["make"] == loop_thread
