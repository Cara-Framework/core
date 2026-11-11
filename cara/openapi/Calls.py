"""Walk one function body: its typed raises and the calls the source pins.

The per-function half of ``ReachableErrors``. Each raise and each call is
recorded with the ``try`` handlers that enclose it, so propagation can stop an
exception exactly where the code catches it. A call target is resolved only
where the source pins it (``SourceIndex``); anything else is not guessed.

A function runs with the facts its caller pins (``Conditions``): a branch
those facts decide is walked on its taken side only, a block stops at the
statement that ends it, and each call hands the callee the values it pins.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

from .Conditions import _Binder, _captures, _Evaluator, _Facts, _pin, _unpin
from .SourceClass import SourceClass
from .SourceFunction import SourceFunction
from .SourceIndex import SourceIndex
from .SourceModule import SourceModule
from .Sources import (
    _FUNCTION_NODES,
    _first_parameter,
    _parameter_annotation,
    _simple_name,
)

# An exception travels as (class name, the HTTP status it answers, every class
# name it is an instance of).
_Raised = tuple[str, int, frozenset[str]]
# One ``try``: its handlers in order, each (caught names or None for all,
# whether it re-raises what it caught).
_Clause = tuple[tuple[frozenset[str] | None, bool], ...]
_Context = tuple[_Clause, ...]


@dataclass(frozen=True)
class _Node:
    """One function as it runs: on an instance of ``context`` (a mixin's
    ``self`` is the class that mixes it in), with ``receiver`` naming it, and
    the parameter values its caller pins (``facts``, ``Conditions._pin``)."""

    function: SourceFunction
    context: SourceClass | None
    receiver: str | None
    facts: _Facts = frozenset()


# A call target and how many of its leading parameters the call form supplies
# itself (a bound method's ``self``); ``None`` where the arguments are not the
# ones it receives (a callable handed over to be called later).
_Target = tuple[_Node, int | None]


def _node_for(function: SourceFunction, context: SourceClass | None) -> _Node:
    receiver = _first_parameter(function.node) if context is not None else None
    return _Node(function, context, receiver)


@dataclass
class _Scan:
    direct: set[_Raised] = field(default_factory=set)
    edges: list[tuple[_Node, _Context]] = field(default_factory=list)


class _FunctionScan:
    """Record one function's escaping raises and its call edges."""

    def __init__(
        self,
        index: SourceIndex,
        binder: _Binder,
        raised: Callable[[SourceModule, ast.expr], _Raised | None],
        typed: Callable[[SourceClass, ast.Call | None], _Raised | None],
        node: _Node,
        scan: _Scan,
    ):
        self._index = index
        self._binder = binder
        self._raised = raised
        self._typed = typed
        self._node = node
        self._scan = scan
        self._module = node.function.module
        function = node.function.node
        self._nested = {
            statement.name: statement
            for statement in ast.walk(function)
            if isinstance(statement, _FUNCTION_NODES) and statement is not function
        }
        self._locals = _bound_names(function)
        self._types = self._local_types(function)
        self._evaluator = _Evaluator(_unpin(node.facts), self._builtin)

    def run(self) -> None:
        self._block(self._node.function.node.body, ())

    # -- statements ------------------------------------------------------

    def _block(self, statements: list[ast.stmt], context: _Context) -> bool:
        """Walk ``statements`` up to the one that ends the block; whether
        one does (nothing after it runs)."""
        return any(self._statement(statement, context) for statement in statements)

    def _statement(self, statement: ast.stmt, context: _Context) -> bool:
        if isinstance(statement, _FUNCTION_NODES):
            # Decorators and defaults run here; the body runs where the
            # function is referenced (``_targets``).
            for expression in (
                *statement.decorator_list,
                *statement.args.defaults,
                *(d for d in statement.args.kw_defaults if d is not None),
            ):
                self._expression(expression, context)
            return False
        if isinstance(statement, ast.ClassDef):
            return False
        if isinstance(statement, ast.If):
            truth = self._test(statement.test, context)
            ends = [
                self._block(block, context)
                for block, runs in (
                    (statement.body, truth is not False),
                    (statement.orelse, truth is not True),
                )
                if runs
            ]
            return all(ends)
        if isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            if isinstance(statement, ast.While):
                runs = self._test(statement.test, context) is not False
            else:
                self._expression(statement.iter, context)
                runs = True
            if runs:
                self._block(statement.body, context)
            self._block(statement.orelse, context)
            return False
        if isinstance(statement, (ast.Try, ast.TryStar)):
            clause = tuple(
                (_handler_names(handler.type), _reraises(handler))
                for handler in statement.handlers
            )
            self._block(statement.body, (*context, clause))
            for handler in statement.handlers:
                self._block(handler.body, context)
            self._block(statement.orelse, context)
            self._block(statement.finalbody, context)
            return False
        if isinstance(statement, (ast.With, ast.AsyncWith)):
            inner = context
            for item in statement.items:
                self._expression(item.context_expr, context)
                suppressed = _suppressed(item.context_expr)
                if suppressed is not None:
                    inner = (*inner, ((suppressed, False),))
            self._block(statement.body, inner)
            return False
        if isinstance(statement, ast.Raise):
            if statement.exc is not None:
                for raised in self._raised_by(statement.exc):
                    if _escapes(raised[2], context):
                        self._scan.direct.add(raised)
            for expression in (statement.exc, statement.cause):
                if expression is not None:
                    self._expression(expression, context)
            return True
        self._children(statement, context)
        return isinstance(statement, (ast.Return, ast.Break, ast.Continue))

    def _test(self, test: ast.expr, context: _Context) -> bool | None:
        """Walk a branch test's calls as Python runs them — an ``and`` stops
        at its first falsy operand, an ``or`` at its first truthy one — and
        say whether the facts decide it."""
        if isinstance(test, ast.BoolOp):
            stop = isinstance(test.op, ast.Or)
            for operand in test.values:
                if self._test(operand, context) is stop:
                    break
        else:
            self._expression(test, context)
        return self._evaluator.truth(test)

    def _builtin(self, name: str) -> bool:
        """Whether ``name`` here is the Python builtin of that name."""
        return (
            name not in self._locals and self._index.resolve(self._module, name) is None
        )

    def _children(self, node: ast.AST, context: _Context) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.stmt):
                self._statement(child, context)
            elif isinstance(child, ast.expr):
                self._expression(child, context)
            else:
                self._children(child, context)

    def _raised_by(self, exc: ast.expr) -> list[_Raised]:
        """What ``raise exc`` throws, wherever the source pins its class:
        named (``raise X(...)``), the class a classmethod runs on (``raise
        cls(...)``), a local every assignment types (``error = X(...)``;
        ``raise error``), or each class a factory's return annotation names
        (``raise self.not_found(...)``, ``raise X.field(...)``). A re-raised
        ``except ... as`` name is the handler's business (``_reraises``)."""
        raised = self._raised(self._module, exc)
        if raised is not None:
            return [raised]
        call = exc if isinstance(exc, ast.Call) else None
        target = call.func if call is not None else exc
        node = self._node
        if isinstance(target, ast.Name) and target.id == node.receiver:
            typed = self._typed(node.context, call) if node.context else None
            return [typed] if typed is not None else []
        if call is None:
            instance = self._instance_class(target)
            typed = self._typed(instance, None) if instance is not None else None
            return [typed] if typed is not None else []
        found: list[_Raised] = []
        for factory, _ in self._targets(target, construct=False):
            function = factory.function
            for cls in self._index.annotation_classes(
                function.module, function.node.returns
            ):
                typed = self._typed(cls, None)
                if typed is not None:
                    found.append(typed)
        return found

    # -- calls -----------------------------------------------------------

    def _expression(self, expression: ast.expr, context: _Context) -> None:
        for node in ast.walk(expression):
            if not isinstance(node, ast.Call):
                continue
            targets = self._targets(node.func, construct=True)
            for target, implicit in targets:
                entered = self._enter(target, implicit, node.args, node.keywords)
                self._scan.edges.append((entered, context))
            # ``run_in_thread(fn, *args, **kwargs)`` runs ``fn`` with the
            # arguments after it; any other receiver's are not ``fn``'s.
            forwarded = bool(targets) and all(
                implicit is not None
                and self._binder.forwards(target.function.node, implicit)
                for target, implicit in targets
            )
            head = (
                node.args[0]
                if forwarded and node.args and not isinstance(node.args[0], ast.Starred)
                else None
            )
            for argument in (*node.args, *(k.value for k in node.keywords)):
                handed_on = argument is head
                if isinstance(argument, ast.Starred):
                    argument = argument.value
                if not isinstance(argument, (ast.Name, ast.Attribute)):
                    continue
                for target, implicit in self._targets(argument, construct=False):
                    entered = (
                        self._enter(target, implicit, node.args[1:], node.keywords)
                        if handed_on
                        else self._enter(target, None, [], [])
                    )
                    self._scan.edges.append((entered, context))

    def _enter(
        self,
        target: _Node,
        implicit: int | None,
        arguments: list[ast.expr],
        keywords: list[ast.keyword],
    ) -> _Node:
        """``target`` with the facts this call pins on it: the arguments it
        passes (``implicit`` leading parameters come from the call form), the
        defaults it leaves, and a nested function's closure. Only what the
        callee consults and never rebinds is kept, so an unread literal adds
        no variant."""
        function = target.function.node
        facts = _unpin(target.facts)
        if implicit is not None:
            facts.update(
                self._binder.bind(
                    function, arguments, keywords, implicit, self._evaluator.value
                )
            )
        kept = self._binder.kept(function)
        pinned = _pin({name: fact for name, fact in facts.items() if name in kept})
        return target if pinned == target.facts else replace(target, facts=pinned)

    def _targets(self, func: ast.expr, *, construct: bool) -> list[_Target]:
        """The functions a call (or a handed-over reference) of ``func`` runs."""
        node = self._node
        if isinstance(func, ast.Name):
            if func.id in self._nested:
                nested = SourceFunction(
                    self._module, self._nested[func.id], node.function.owner
                )
                return [(_Node(nested, node.context, node.receiver, node.facts), 0)]
            if func.id in self._locals:
                return []
            return self._symbol_targets(
                self._index.resolve(self._module, func.id), construct
            )
        if not isinstance(func, ast.Attribute):
            return []
        receiver = func.value
        if (
            isinstance(receiver, ast.Call)
            and isinstance(receiver.func, ast.Name)
            and receiver.func.id == "super"
            and node.context is not None
        ):
            return self._method(node.context, func.attr, after=node.function.owner)
        if isinstance(receiver, ast.Name) and receiver.id == node.receiver:
            if node.context is None:
                return []
            # A classmethod's ``cls.method(...)`` goes through the class: the
            # call passes an instance method's ``self`` itself.
            bound = not _classmethod(node.function.node)
            return self._method(node.context, func.attr, bound=bound)
        instance = self._instance_class(receiver)
        if instance is not None:
            return self._method(instance, func.attr)
        if _root_name(receiver) in self._locals:
            return []
        holder = self._index.resolve_expr(self._module, receiver)
        if isinstance(holder, SourceClass):
            return self._method(holder, func.attr, bound=False)
        if isinstance(holder, SourceModule):
            return self._symbol_targets(self._index.resolve(holder, func.attr), construct)
        return []

    def _symbol_targets(self, symbol: Any, construct: bool) -> list[_Target]:
        if isinstance(symbol, SourceFunction):
            return [(_node_for(symbol, None), 0)]
        if isinstance(symbol, SourceClass) and construct:
            return self._method(symbol, "__init__")
        return []

    def _method(
        self,
        cls: SourceClass,
        name: str,
        *,
        after: SourceClass | None = None,
        bound: bool = True,
    ) -> list[_Target]:
        """``cls``'s ``name`` method. Reached through an instance (``bound``)
        its receiver is supplied by the call; through the class only a
        classmethod's is (``Cls.method(instance, ...)`` passes its own)."""
        function = self._index.method(cls, name, after=after)
        if function is None:
            return []
        target = _node_for(function, cls)
        if bound:
            implicit = 0 if target.receiver is None else 1
        else:
            implicit = int(_classmethod(function.node))
        return [(target, implicit)]

    def _instance_class(self, expression: ast.expr) -> SourceClass | None:
        """The class of the instance ``expression`` evaluates to, where the
        source pins it."""
        node = self._node
        if isinstance(expression, ast.Name):
            return self._types.get(expression.id)
        if isinstance(expression, ast.Attribute):
            if (
                isinstance(expression.value, ast.Name)
                and expression.value.id == node.receiver
            ):
                return (
                    self._index.attribute_class(node.context, expression.attr)
                    if node.context
                    else None
                )
            holder = self._instance_class(expression.value)
            return (
                self._index.attribute_class(holder, expression.attr) if holder else None
            )
        if (
            isinstance(expression, ast.Call)
            and _root_name(expression.func) not in self._locals
        ):
            built = self._index.resolve_expr(self._module, expression.func)
            return built if isinstance(built, SourceClass) else None
        return None

    def _local_types(self, function: Any) -> dict[str, SourceClass]:
        """Parameters and locals whose class the source pins: an annotation,
        or every assignment constructing the same class."""
        found: dict[str, set] = {}
        receiver = self._node.receiver
        for parameter in (
            *function.args.posonlyargs,
            *function.args.args,
            *function.args.kwonlyargs,
        ):
            if parameter.arg != receiver:
                found.setdefault(parameter.arg, set()).add(
                    self._index.annotation_class(self._module, parameter.annotation)
                )
        for statement in ast.walk(function):
            if isinstance(statement, ast.AnnAssign) and isinstance(
                statement.target, ast.Name
            ):
                found.setdefault(statement.target.id, set()).add(
                    self._index.annotation_class(self._module, statement.annotation)
                )
            elif isinstance(statement, ast.Assign):
                for target in statement.targets:
                    if (
                        isinstance(target, ast.Name)
                        and _parameter_annotation(function, target.id) is None
                    ):
                        found.setdefault(target.id, set()).update(
                            self._index.value_classes(
                                self._module, function, statement.value
                            )
                        )
        return {
            name: next(iter(classes))
            for name, classes in found.items()
            if len(classes) == 1 and None not in classes
        }


def _classmethod(function: Any) -> bool:
    return any(
        _simple_name(decorator) == "classmethod" for decorator in function.decorator_list
    )


def _escapes(lineage: frozenset[str], context: _Context) -> bool:
    """Whether an exception of ``lineage`` leaves every enclosing ``try``:
    the first handler matching it in each either re-raises or is absent."""
    for clause in context:
        for names, reraises in clause:
            if names is None or names & lineage:
                if not reraises:
                    return False
                break
    return True


def _handler_names(node: ast.expr | None) -> frozenset[str] | None:
    if node is None:
        return None
    items = node.elts if isinstance(node, ast.Tuple) else [node]
    return frozenset(
        item.id if isinstance(item, ast.Name) else item.attr
        for item in items
        if isinstance(item, (ast.Name, ast.Attribute))
    )


def _reraises(handler: ast.ExceptHandler) -> bool:
    """A handler that can raise what it caught again (bare ``raise`` or
    ``raise <as-name>``, on any path) lets it through."""
    for node in _walk_local(handler.body):
        if isinstance(node, ast.Raise) and (
            node.exc is None
            or (
                handler.name is not None
                and isinstance(node.exc, ast.Name)
                and node.exc.id == handler.name
            )
        ):
            return True
    return False


def _suppressed(expression: ast.expr) -> frozenset[str] | None:
    """``contextlib.suppress(A, B)`` as a context manager catches A and B."""
    if not isinstance(expression, ast.Call):
        return None
    func = expression.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    if name != "suppress":
        return None
    return _handler_names(ast.Tuple(elts=list(expression.args)))


def _walk_local(statements: list[ast.stmt]):
    """Every node of ``statements``, not entering nested functions/classes."""
    pending: list[ast.AST] = list(statements)
    while pending:
        node = pending.pop()
        yield node
        for child in ast.iter_child_nodes(node):
            if not isinstance(child, (*_FUNCTION_NODES, ast.ClassDef, ast.Lambda)):
                pending.append(child)


def _bound_names(function: Any) -> frozenset[str]:
    """Names the function binds itself — parameters and assignment targets —
    which therefore do not mean the module-level symbol of that name."""
    arguments = function.args
    names = {
        parameter.arg
        for parameter in (
            *arguments.posonlyargs,
            *arguments.args,
            *arguments.kwonlyargs,
            *(p for p in (arguments.vararg, arguments.kwarg) if p is not None),
        )
    }
    for node in ast.walk(function):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, ast.pattern):
            names.update(_captures(node))
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
    return frozenset(names)


def _root_name(node: ast.expr) -> str | None:
    while isinstance(node, (ast.Attribute, ast.Call, ast.Subscript)):
        node = node.func if isinstance(node, ast.Call) else node.value
    return node.id if isinstance(node, ast.Name) else None
