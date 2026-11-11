"""Canonical definition of ``ReachableErrors``."""

from __future__ import annotations

import ast
from collections import deque
from collections.abc import Mapping

from .Calls import _escapes, _FunctionScan, _Node, _node_for, _Raised, _Scan
from .Conditions import _Binder, _pin, _Routed, _transparent
from .SourceClass import SourceClass
from .SourceIndex import SourceIndex
from .SourceModule import SourceModule
from .Sources import _simple_name

# The constructor parameter a raise site sets its own status through.
_STATUS_PARAMETER = "status_code"
# The framework's request: the parameter an action declares with it (or a
# subclass) is the request its route hands over, whose method the route fixes.
_REQUEST_CLASS = ("cara.http.request.Request", "Request")
# 500 is the fault every operation can answer; a typed one tells a client
# nothing it can act on, so it is not published per operation.
_GENERIC_FAULT = 500
_UNPINNED = object()


def _published(status: int) -> bool:
    """An error status an operation documents: a 4xx/5xx other than the
    generic fault."""
    return status >= 400 and status != _GENERIC_FAULT


class ReachableErrors:
    """Which typed exceptions can escape a routed action, below the controller too.

    A controller is thin: the ``InvalidInput`` a content edit answers with is
    raised by the service it calls, and a 404 by the lookup helper that
    service uses. Documenting only what the action's own methods raise left
    those answers out of the published contract (live 2026-11-11: ``PATCH
    /listings/{id}/content`` answered 400 and the spec did not say so).

    This follows the calls the source pins, through every layer
    (``Calls._FunctionScan``):

    * ``self.method()`` through the concrete class's MRO (a mixin's ``self``
      is the controller or service that mixes it in);
    * ``self.<attribute>.method()`` through the class the attribute holds —
      the constructor parameter's annotation the container injects
      (``SourceIndex.attribute_class``);
    * imported functions and classes (a constructor runs ``__init__``), class
      and module attributes, ``super()``, typed parameters and locals;
    * a function or method handed to another call (``run_in_thread(fn,
      ...)``, ``find_or_fail(self.repo.find, ...)``, a callback) — it is
      handed over to be called;
    * a nested ``def`` where it is referenced, a ``lambda`` where it is
      written.

    A raise counts wherever the source pins the class it throws: named, the
    class a classmethod runs on (``raise cls(...)``), a typed local, or what a
    factory's return annotation names (``raise self.not_found(...)``). A
    class with no status of its own answers its nearest base's, as its
    inherited ``status_code`` does at runtime.

    An exception stops where an enclosing ``try`` catches it (its class or any
    base, builtins included) without re-raising, or inside
    ``contextlib.suppress``. A call the source does not pin — a value of
    unknown type, a callable read from a dict — is not guessed; what flows
    through it is declared on the action with ``@raises(...)``. An abstract
    method is read as written (it raises nothing), not as the implementation
    a container binds to its class.

    A branch runs only where it can (``Conditions``): given the HTTP method
    of the route, the action's request parameter (declared with the
    framework's ``Request``) carries it, and every call hands its callee the
    values the source pins — literal arguments, the defaults it leaves, a
    value the caller already knows, and what a forwarding call
    (``run_in_thread(fn, *args, **kwargs)``) passes on to ``fn``. So the
    billing write gate's 402 is not listed on a GET, nor behind
    ``billing=False``, nor for a permission key its set does not hold (review
    2026-11-11: 175 reads and 64 billing-exempt writes published it).

    The answer is what CAN escape, not what does on every path: a condition
    the facts do not decide runs both ways. ``statuses`` maps an exception
    class name to its HTTP status
    (``ErrorContractExtractor.exception_statuses``).
    """

    def __init__(self, index: SourceIndex, statuses: Mapping[str, int]):
        self._index = index
        self._statuses = dict(statuses)
        self._binder = _Binder()
        self._scans: dict[_Node, _Scan] = {}
        self._final: dict[_Node, frozenset[_Raised]] = {}
        self._request: SourceClass | None | bool = False

    def method_errors(
        self, cls: SourceClass, method: str, *, http_method: str | None = None
    ) -> frozenset[tuple[str, int]]:
        """``(exception name, status)`` for every typed error
        ``cls().method(...)`` can raise to its caller; with ``http_method``,
        as the route answering that method calls it."""
        function = self._index.method(cls, method)
        if function is None:
            return frozenset()
        root = _node_for(function, cls)
        if http_method is not None:
            root = self._routed(root, http_method.upper())
        return frozenset((name, status) for name, status, _ in self._escaping(root))

    def _routed(self, root: _Node, http_method: str) -> _Node:
        """``root`` with its request parameter fixed to the routed request."""
        request = self._request_class()
        if request is None:
            return root
        function = root.function
        if not _transparent(function.node):
            return root
        spec = function.node.args
        parameters = [*spec.posonlyargs, *spec.args, *spec.kwonlyargs]
        if root.receiver is not None:
            parameters = parameters[1:]
        kept = self._binder.kept(function.node)
        facts = {
            parameter.arg: _Routed(http_method)
            for parameter in parameters
            if parameter.arg in kept
            and (
                found := self._index.annotation_class(
                    function.module, parameter.annotation
                )
            )
            is not None
            and request in self._index.mro(found)
        }
        return _Node(root.function, root.context, root.receiver, _pin(facts))

    def _request_class(self) -> SourceClass | None:
        if self._request is False:
            module_name, name = _REQUEST_CLASS
            module = self._index.module(module_name)
            found = self._index.resolve(module, name) if module is not None else None
            self._request = found if isinstance(found, SourceClass) else None
        return self._request if isinstance(self._request, SourceClass) else None

    def _escaping(self, root: _Node) -> frozenset[_Raised]:
        """Propagate raises up the call edges to a fixpoint over every
        function ``root`` reaches; each result is final once computed."""
        if root in self._final:
            return self._final[root]
        order: list[_Node] = []
        seen = {root}
        stack = [root]
        while stack:
            node = stack.pop()
            order.append(node)
            for callee, _ in self._scan(node).edges:
                if callee not in seen and callee not in self._final:
                    seen.add(callee)
                    stack.append(callee)
        current = {node: set(self._scan(node).direct) for node in order}
        callers: dict[_Node, list[_Node]] = {}
        for node in order:
            for callee, _ in self._scan(node).edges:
                callers.setdefault(callee, []).append(node)
        pending = deque(order)
        queued = set(order)
        while pending:
            node = pending.popleft()
            queued.discard(node)
            found = set(self._scan(node).direct)
            for callee, context in self._scan(node).edges:
                source = self._final.get(callee)
                for raised in current[callee] if source is None else source:
                    if _escapes(raised[2], context):
                        found.add(raised)
            if found != current[node]:
                current[node] = found
                for caller in callers.get(node, ()):
                    if caller not in queued:
                        queued.add(caller)
                        pending.append(caller)
        for node in order:
            self._final[node] = frozenset(current[node])
        return self._final[root]

    def _scan(self, node: _Node) -> _Scan:
        if node in self._scans:
            return self._scans[node]
        scan = _Scan()
        self._scans[node] = scan
        _FunctionScan(
            self._index, self._binder, self._raised, self._typed, node, scan
        ).run()
        return scan

    def _raised(self, module: SourceModule, exc: ast.expr) -> _Raised | None:
        """The typed exception ``raise exc`` throws when ``exc`` names its
        class (``raise X(...)``, ``raise X``, ``raise pkg.X(...)``): resolved
        through the imports, so an ``as`` name still counts; by its name
        where the class cannot be read."""
        call = exc if isinstance(exc, ast.Call) else None
        target = call.func if call is not None else exc
        found = self._index.resolve_expr(module, target)
        if isinstance(found, SourceClass):
            return self._typed(found, call)
        name = _simple_name(target)
        status = self._statuses.get(name or "")
        if name is None or status is None or not _published(status):
            return None
        # A class with an HTTP status is an ``Exception`` even where its own
        # bases cannot be read, so ``except Exception`` still stops it.
        return name, status, frozenset({name, "Exception", "BaseException"})

    def _typed(self, cls: SourceClass, call: ast.Call | None) -> _Raised | None:
        """``cls`` as a raised exception: its name, the status it answers —
        its own, else the nearest base's that has one, as its inherited
        ``status_code`` answers — and its lineage. A status handed to the
        constructor (``HttpException(msg, status_code=404)``) is the one it
        answers; one handed in as a variable (``abort``'s) is not pinned, so
        not counted."""
        status = next(
            (
                self._statuses[klass.node.name]
                for klass in self._index.mro(cls)
                if klass.node.name in self._statuses
            ),
            None,
        )
        if status is None:
            return None
        if call is not None:
            argument = self._status_argument(cls, call)
            if argument is _UNPINNED:
                return None
            status = argument if isinstance(argument, int) else status
        if not _published(status):
            return None
        name = cls.node.name
        lineage = self._index.lineage(cls)
        return name, status, lineage | {name, "Exception", "BaseException"}

    def _status_argument(self, cls: SourceClass, call: ast.Call) -> object:
        """The ``status_code`` a constructor call passes: an int, ``None``
        when it passes none (the class's own applies), else ``_UNPINNED``."""
        value = next((k.value for k in call.keywords if k.arg == _STATUS_PARAMETER), None)
        init = self._index.method(cls, "__init__")
        if value is None and init is not None:
            arguments = init.node.args
            names = [p.arg for p in (*arguments.posonlyargs, *arguments.args)][1:]
            if _STATUS_PARAMETER in names:
                position = names.index(_STATUS_PARAMETER)
                if any(isinstance(a, ast.Starred) for a in call.args[: position + 1]):
                    return _UNPINNED
                if position < len(call.args):
                    value = call.args[position]
        if value is None or (isinstance(value, ast.Constant) and value.value is None):
            return None
        if (
            isinstance(value, ast.Constant)
            and isinstance(value.value, int)
            and not isinstance(value.value, bool)
        ):
            return value.value
        return _UNPINNED
