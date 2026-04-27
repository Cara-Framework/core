"""Fluent mock builder for Cara contracts.

Two layers:

- :class:`Mock` — strict mock. Attribute access for an undeclared method
  raises ``AssertionError`` ("unexpected call"). Useful when you want
  to assert *exactly* which methods are called.

- :class:`Spy` — permissive mock. Any attribute returns a recording
  callable that returns ``None`` by default. Records all calls.

Both expose ``.expects("method").returns(value)`` and
``.expects("method").raises(Exception(...))`` plus call assertions.

Example
-------

    data = Mock(PriceValidationDataContract)
    data.expects("get_latest_price_min").returns(10.0)

    service = PriceValidationService(data)
    service.validate(1, 12.0)

    data.assert_called("get_latest_price_min", times=1)
    data.assert_called_with("get_latest_price_min", 1, None)
"""

from __future__ import annotations

import inspect
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Type


class _Call:
    __slots__ = ("args", "kwargs")

    def __init__(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
        self.args = args
        self.kwargs = kwargs

    def __repr__(self) -> str:
        parts = [repr(a) for a in self.args]
        parts.extend(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"({', '.join(parts)})"


class _Behavior:
    """How a mocked method should respond when called."""

    def __init__(self) -> None:
        # Either a single ``return_value`` *or* a queue of ``returns``.
        self._return_value: Any = None
        self._return_queue: List[Any] = []
        self._raises: Optional[BaseException] = None
        self._side_effect: Optional[Callable[..., Any]] = None
        self._matchers: List[Tuple[Tuple[Any, ...], Dict[str, Any], Any]] = []

    def returns(self, value: Any) -> "_Behavior":
        self._return_value = value
        return self

    def returns_in_order(self, *values: Any) -> "_Behavior":
        self._return_queue.extend(values)
        return self

    def raises(self, exc: BaseException) -> "_Behavior":
        self._raises = exc
        return self

    def then(self, fn: Callable[..., Any]) -> "_Behavior":
        """Custom callable; receives the same ``*args, **kwargs``."""
        self._side_effect = fn
        return self

    def with_args(self, *args: Any, **kwargs: Any) -> "_ArgMatcher":
        """Return-value branch: ``.with_args(1).returns(10)``."""
        return _ArgMatcher(self, args, kwargs)

    def _resolve(self, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        for ma, mk, mv in self._matchers:
            if ma == args and mk == kwargs:
                if isinstance(mv, BaseException):
                    raise mv
                return mv
        if self._side_effect is not None:
            return self._side_effect(*args, **kwargs)
        if self._raises is not None:
            raise self._raises
        if self._return_queue:
            return self._return_queue.pop(0)
        return self._return_value


class _ArgMatcher:
    def __init__(
        self,
        behavior: _Behavior,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> None:
        self._behavior = behavior
        self._args = args
        self._kwargs = kwargs

    def returns(self, value: Any) -> _Behavior:
        self._behavior._matchers.append((self._args, self._kwargs, value))
        return self._behavior

    def raises(self, exc: BaseException) -> _Behavior:
        self._behavior._matchers.append((self._args, self._kwargs, exc))
        return self._behavior


class _MockBase:
    """Shared machinery for :class:`Mock` and :class:`Spy`."""

    def __init__(self, contract: Optional[Type[Any]] = None) -> None:
        self._contract = contract
        self._behaviors: Dict[str, _Behavior] = {}
        self._calls: Dict[str, List[_Call]] = defaultdict(list)
        self._strict = False  # Subclasses set this.

    # ── Public API ───────────────────────────────────────────────────

    def expects(self, name: str) -> _Behavior:
        """Declare a method and return a :class:`_Behavior` to configure."""
        self._validate_method(name)
        beh = self._behaviors.setdefault(name, _Behavior())
        return beh

    def returning(self, **mapping: Any) -> "_MockBase":
        """Sugar: ``Mock(C).returning(method_a=1, method_b="x")``."""
        for name, value in mapping.items():
            self.expects(name).returns(value)
        return self

    # ── Call assertions ──────────────────────────────────────────────

    def calls_to(self, name: str) -> List[_Call]:
        return list(self._calls.get(name, []))

    def call_count(self, name: str) -> int:
        return len(self._calls.get(name, []))

    def was_called(self, name: str) -> bool:
        return self.call_count(name) > 0

    def assert_called(self, name: str, times: Optional[int] = None) -> None:
        n = self.call_count(name)
        if times is None and n == 0:
            raise AssertionError(f"Expected {name}() to be called at least once; was 0x")
        if times is not None and n != times:
            raise AssertionError(f"Expected {name}() called {times}x, got {n}x")

    def assert_not_called(self, name: str) -> None:
        n = self.call_count(name)
        if n != 0:
            raise AssertionError(f"Expected {name}() to NOT be called, got {n}x")

    def assert_called_with(self, name: str, *args: Any, **kwargs: Any) -> None:
        for call in self._calls.get(name, []):
            if call.args == args and call.kwargs == kwargs:
                return
        history = self.calls_to(name)
        if not history:
            raise AssertionError(f"{name}() was never called")
        rendered = "\n".join(f"  {name}{c}" for c in history)
        raise AssertionError(
            f"No call to {name}({', '.join(repr(a) for a in args)}) found.\n"
            f"Calls were:\n{rendered}"
        )

    # ── Internal hooks ───────────────────────────────────────────────

    def _record_and_resolve(
        self, name: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        self._calls[name].append(_Call(args, kwargs))
        beh = self._behaviors.get(name)
        if beh is None:
            if self._strict:
                raise AssertionError(
                    f"Unexpected call to {name}() on strict Mock"
                    + (f"({self._contract.__name__})" if self._contract else "")
                )
            return None
        return beh._resolve(args, kwargs)

    def _validate_method(self, name: str) -> None:
        if self._contract is None:
            return
        if not hasattr(self._contract, name):
            members = [
                n
                for n, _ in inspect.getmembers(self._contract, predicate=callable)
                if not n.startswith("_")
            ]
            raise AttributeError(
                f"{self._contract.__name__} has no method {name!r}. Available: {members}"
            )

    # ── Attribute interception ───────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        # Methods starting with ``_`` or already on the instance go
        # through normal lookup (handled by ``__getattribute__``); this
        # method only fires for misses.
        if name.startswith("_"):
            raise AttributeError(name)

        if self._strict and name not in self._behaviors:
            # Dispatch a clear failure on the actual call so the
            # traceback points at the call site.
            def _fail(*args: Any, **kwargs: Any) -> Any:
                raise AssertionError(
                    f"Unexpected call to {name}() on strict Mock"
                    + (f"({self._contract.__name__})" if self._contract else "")
                )

            return _fail

        def _dispatch(*args: Any, **kwargs: Any) -> Any:
            return self._record_and_resolve(name, args, kwargs)

        return _dispatch


class Mock(_MockBase):
    """Strict mock — undeclared methods raise."""

    def __init__(self, contract: Optional[Type[Any]] = None) -> None:
        super().__init__(contract)
        self._strict = True


class Spy(_MockBase):
    """Permissive mock — any attribute is a no-op recorder."""

    def __init__(self, contract: Optional[Type[Any]] = None) -> None:
        super().__init__(contract)
        self._strict = False


# ── Module-level helpers ─────────────────────────────────────────────


def when(mock: _MockBase, method: str) -> _Behavior:
    """``when(mock, "x").returns(1)`` reads as ``when(...) returns ...``."""
    return mock.expects(method)


def returning(mock: _MockBase, **mapping: Any) -> _MockBase:
    """Functional alias for ``mock.returning(...)``."""
    return mock.returning(**mapping)
