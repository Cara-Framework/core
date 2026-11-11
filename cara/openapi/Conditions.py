"""Read what the source pins about a call: argument values and branch outcomes.

The fact half of ``ReachableErrors``. A function runs with FACTS — parameter
values the source fixes: a literal argument, a default the call leaves in
place, a value its caller already had a fact for, and the request a route
hands its action (``_Routed``: the route fixes its HTTP method). A branch whose
test those facts decide runs on one side only, so a raise behind
``if billing:`` called with ``billing=False``, or behind an unsafe-method
check on a GET, is not reachable there.

Every reading is exact or absent: a value this module cannot read is
``_UNKNOWN`` and its branch runs both ways, as before. A fact is an immutable
value (a list or set display is not: the callee can change it in place). It
holds only for a parameter the function never rebinds (a ``match`` capture
rebinds too), and only where it is consulted — in a test, or in what is handed
to another call — so an unread literal adds no variant. A function behind a
decorator that may change its arguments gets none.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .Sources import _FUNCTION_NODES, _simple_name

_UNKNOWN = object()

_Value = Callable[[ast.expr], object]
# A function's facts as a hashable key: each value with its type, so ``True``
# and ``1`` (equal, and equal in hash) stay two facts.
_Facts = frozenset[tuple[str, type, object]]
# Decorators that call the function with the arguments its caller passed.
_TRANSPARENT_DECORATORS = frozenset(
    {
        "abstractmethod",
        "asynccontextmanager",
        "cache",
        "cached_property",
        "classmethod",
        "contextmanager",
        "lru_cache",
        "override",
        "property",
        "staticmethod",
    }
)


def _pin(values: Mapping[str, object]) -> _Facts:
    return frozenset((name, type(value), value) for name, value in values.items())


def _unpin(facts: _Facts) -> dict[str, object]:
    return {name: value for name, _, value in facts}


def _transparent(function: Any) -> bool:
    """Whether every decorator of ``function`` hands it its caller's
    arguments unchanged (one that may inject or drop them pins nothing)."""
    return all(
        _simple_name(decorator.func if isinstance(decorator, ast.Call) else decorator)
        in _TRANSPARENT_DECORATORS
        for decorator in function.decorator_list
    )


_COMPARISONS: dict[type, Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
    ast.In: lambda left, right: left in right,
    ast.NotIn: lambda left, right: left not in right,
}
# ``str`` methods a test normalises a value with before it compares it.
_STRING_METHODS = frozenset({"casefold", "lower", "strip", "upper"})
_CONVERSIONS: dict[str, Callable[[Any], Any]] = {"bool": bool, "str": str}


@dataclass(frozen=True)
class _Routed:
    """The request a route hands its action: its HTTP method is the route's."""

    method: str


def _truthy(value: object) -> bool:
    return True if isinstance(value, _Routed) else bool(value)


# Values no callee can change in place. A list or set display is a new object
# the callee may ``append`` to or ``discard`` from without rebinding its name,
# so it is read inside a test but never handed on as a fact.
_SCALARS = (str, bytes, int, float, complex, bool, type(None), type(Ellipsis))


def _immutable(value: object) -> bool:
    if isinstance(value, tuple):
        return all(_immutable(item) for item in value)
    return isinstance(value, (*_SCALARS, _Routed))


class _Evaluator:
    """Evaluate an expression against one function's facts.

    ``builtin(name)`` says whether ``name`` there still means the Python
    builtin (nothing in the function or its module rebinds it).
    """

    def __init__(self, facts: Mapping[str, object], builtin: Callable[[str], bool]):
        self._facts = facts
        self._builtin = builtin

    def truth(self, node: ast.expr) -> bool | None:
        """Whether ``node`` is truthy, ``None`` where the facts do not decide.
        ``a and b`` is falsy once either side is, whatever the other is."""
        if isinstance(node, ast.BoolOp):
            truths = [self.truth(value) for value in node.values]
            decisive = isinstance(node.op, ast.Or)
            if decisive in truths:
                return decisive
            return None if None in truths else not decisive
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            truth = self.truth(node.operand)
            return None if truth is None else not truth
        value = self.value(node)
        return None if value is _UNKNOWN else _truthy(value)

    def value(self, node: ast.expr) -> object:
        """What ``node`` evaluates to, or ``_UNKNOWN``."""
        try:
            return self._value(node)
        except TypeError, ValueError:
            return _UNKNOWN

    def _value(self, node: ast.expr) -> object:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return self._facts.get(node.id, _UNKNOWN)
        if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
            items = [self._value(item) for item in node.elts]
            if any(item is _UNKNOWN for item in items):
                return _UNKNOWN
            # Each display as the type it builds: ``["a"] != ("a",)``.
            if isinstance(node, ast.Set):
                return frozenset(items)
            return list(items) if isinstance(node, ast.List) else tuple(items)
        if isinstance(node, ast.Attribute):
            base = self._value(node.value)
            if isinstance(base, _Routed) and node.attr == "method":
                return base.method
            return _UNKNOWN
        if isinstance(node, ast.Call):
            return self._call(node)
        if isinstance(node, ast.BoolOp):
            return self._bool_op(node)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            truth = self.truth(node.operand)
            return _UNKNOWN if truth is None else not truth
        if isinstance(node, ast.Compare):
            return self._compare(node)
        if isinstance(node, ast.IfExp):
            truth = self.truth(node.test)
            if truth is None:
                return _UNKNOWN
            return self._value(node.body if truth else node.orelse)
        return _UNKNOWN

    def _call(self, node: ast.Call) -> object:
        func, arguments = node.func, node.args
        if node.keywords or any(isinstance(a, ast.Starred) for a in arguments):
            return _UNKNOWN
        if isinstance(func, ast.Name) and self._builtin(func.id):
            if func.id == "getattr" and len(arguments) in (2, 3):
                target, name = self._value(arguments[0]), self._value(arguments[1])
                if isinstance(target, _Routed) and name == "method":
                    return target.method
                return _UNKNOWN
            convert = _CONVERSIONS.get(func.id)
            if convert is not None and len(arguments) == 1:
                value = self._value(arguments[0])
                if value is _UNKNOWN or isinstance(value, _Routed):
                    return _UNKNOWN
                return convert(value)
            return _UNKNOWN
        if isinstance(func, ast.Attribute) and func.attr in _STRING_METHODS:
            base = self._value(func.value)
            if isinstance(base, str) and not arguments:
                return getattr(base, func.attr)()
        return _UNKNOWN

    def _bool_op(self, node: ast.BoolOp) -> object:
        """Python's own result: ``and`` the first falsy operand, ``or`` the
        first truthy one, else the last."""
        stop_on = isinstance(node.op, ast.Or)
        value: object = _UNKNOWN
        for operand in node.values:
            value = self._value(operand)
            if value is _UNKNOWN:
                return _UNKNOWN
            if _truthy(value) is stop_on:
                return value
        return value

    def _compare(self, node: ast.Compare) -> object:
        left = self._value(node.left)
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            right = self._value(comparator)
            compare = _COMPARISONS.get(type(op))
            if left is _UNKNOWN or right is _UNKNOWN or compare is None:
                return _UNKNOWN
            if not compare(left, right):
                return False
            left = right
        return True


class _Binder:
    """Which facts a called function receives (cached per function)."""

    def __init__(self) -> None:
        self._kept: dict[ast.AST, frozenset[str]] = {}
        self._forwards: dict[tuple[ast.AST, int], bool] = {}

    def bind(
        self,
        function: Any,
        arguments: list[ast.expr],
        keywords: list[ast.keyword],
        implicit: int,
        value: _Value,
    ) -> dict[str, object]:
        """The parameter values a call pins. ``implicit`` leading parameters
        are supplied by the call form itself (a method's ``self``). An
        argument after ``*args``, or any default under ``**kwargs``, is not
        pinned: the expansion may fill it. Only an immutable value is pinned
        (``_immutable``)."""
        if not _transparent(function):
            return {}
        spec = function.args
        positional = [*spec.posonlyargs, *spec.args]
        open_positional = positional[implicit:]
        found: dict[str, object] = {}
        supplied: set[str] = set()
        expanded = False
        for parameter, argument in zip(open_positional, arguments, strict=False):
            if isinstance(argument, ast.Starred):
                expanded = True
                break
            supplied.add(parameter.arg)
            found[parameter.arg] = value(argument)
        expanded = expanded or any(isinstance(a, ast.Starred) for a in arguments)
        keyword_names = {p.arg for p in (*spec.args, *spec.kwonlyargs)}
        mapping = any(keyword.arg is None for keyword in keywords)
        for keyword in keywords:
            if keyword.arg in keyword_names:
                supplied.add(keyword.arg)
                found[keyword.arg] = value(keyword.value)
        if not mapping:
            defaults = dict(
                zip(
                    [p.arg for p in positional][len(positional) - len(spec.defaults) :],
                    spec.defaults,
                    strict=True,
                )
            )
            if not expanded:
                for name, default in defaults.items():
                    if name not in supplied and isinstance(default, ast.Constant):
                        found[name] = default.value
            for parameter, default in zip(spec.kwonlyargs, spec.kw_defaults, strict=True):
                if parameter.arg not in supplied and isinstance(default, ast.Constant):
                    found[parameter.arg] = default.value
        return {
            name: fact
            for name, fact in found.items()
            if fact is not _UNKNOWN and _immutable(fact)
        }

    def kept(self, function: Any) -> frozenset[str]:
        """The names a fact may hold for in ``function``: consulted (read in
        a test, or handed to a call) and never rebound or shadowed in it."""
        if function not in self._kept:
            self._kept[function] = _consulted(function) - _unsettled(function)
        return self._kept[function]

    def forwards(self, function: Any, implicit: int) -> bool:
        """Whether ``function(fn, *args, **kwargs)`` runs ``fn`` with exactly
        the arguments after it — it hands ``fn, *args, **kwargs`` on in one
        call, as ``run_in_thread`` does — so what the caller passes after the
        callable is what the callable receives."""
        key = (function, implicit)
        if key not in self._forwards:
            self._forwards[key] = _forwards(function, implicit)
        return self._forwards[key]


def _forwards(function: Any, implicit: int) -> bool:
    spec = function.args
    positional = [*spec.posonlyargs, *spec.args][implicit:]
    if len(positional) != 1 or spec.vararg is None or spec.kwarg is None:
        return False
    if spec.kwonlyargs or not _transparent(function):
        return False
    callable_name, star, double = positional[0].arg, spec.vararg.arg, spec.kwarg.arg
    if {callable_name, star, double} & _unsettled(function):
        return False
    # ``kwargs.pop(...)`` / ``args[0]``: what it hands on is no longer what
    # it was given.
    if any(
        isinstance(node, (ast.Attribute, ast.Subscript))
        and isinstance(node.value, ast.Name)
        and node.value.id in {star, double}
        for node in ast.walk(function)
    ):
        return False
    for node in ast.walk(function):
        if not isinstance(node, ast.Call) or not _passes_mapping(node, double):
            continue
        arguments = node.args
        if _is_name(node.func, callable_name) and _is_star(arguments, star, 1):
            return True
        if (
            len(arguments) >= 2
            and _is_name(arguments[-2], callable_name)
            and _is_star(arguments[-1:], star, 1)
        ):
            return True
    return False


def _passes_mapping(call: ast.Call, name: str) -> bool:
    return (
        len(call.keywords) == 1
        and call.keywords[0].arg is None
        and _is_name(call.keywords[0].value, name)
    )


def _is_star(arguments: list[ast.expr], name: str, count: int) -> bool:
    return (
        len(arguments) == count
        and isinstance(arguments[-1], ast.Starred)
        and _is_name(arguments[-1].value, name)
    )


def _is_name(node: ast.AST, name: str) -> bool:
    return isinstance(node, ast.Name) and node.id == name


def _consulted(function: Any) -> frozenset[str]:
    """Names ``function`` reads in a branch test or in what it hands to a
    call — as is, or inside the expression it passes (``billing=billing and
    other``, ``"a" if write else "b"``)."""
    found: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, (ast.If, ast.While, ast.IfExp, ast.Assert)):
            found.update(_loaded(node.test))
        elif isinstance(node, ast.Call):
            for argument in (*node.args, *(k.value for k in node.keywords)):
                found.update(_loaded(argument))
    return frozenset(found)


def _loaded(expression: ast.expr) -> set[str]:
    return {
        name.id
        for name in ast.walk(expression)
        if isinstance(name, ast.Name) and isinstance(name.ctx, ast.Load)
    }


def _captures(node: ast.AST) -> tuple[str, ...]:
    """The name a ``match`` pattern node binds — ``case {"k": name}``,
    ``case [*name]``, ``case {**name}`` — a store with no ``Name`` node."""
    if isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name:
        return (node.name,)
    if isinstance(node, ast.MatchMapping) and node.rest:
        return (node.rest,)
    return ()


def _unsettled(function: Any) -> frozenset[str]:
    """Names whose value inside ``function`` is not the one it was called
    with: assigned, deleted, imported, an ``except ... as`` target, captured
    by a ``match`` pattern, a nested definition's name, or a parameter of a
    nested function or lambda."""
    found: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            found.add(node.id)
        elif isinstance(node, ast.pattern):
            found.update(_captures(node))
        elif isinstance(node, ast.ExceptHandler) and node.name:
            found.add(node.name)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            found.update(node.names)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            found.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ClassDef):
            found.add(node.name)
        elif isinstance(node, (*_FUNCTION_NODES, ast.Lambda)) and node is not function:
            if not isinstance(node, ast.Lambda):
                found.add(node.name)
            spec = node.args
            found.update(
                parameter.arg
                for parameter in (
                    *spec.posonlyargs,
                    *spec.args,
                    *spec.kwonlyargs,
                    *(p for p in (spec.vararg, spec.kwarg) if p is not None),
                )
            )
    return frozenset(found)
