"""Canonical definition of ``SourceIndex``."""

from __future__ import annotations

import ast
import builtins
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .SourceClass import SourceClass
from .SourceFunction import SourceFunction
from .SourceModule import SourceModule
from .Sources import (
    _FUNCTION_NODES,
    _c3,
    _first_parameter,
    _imports,
    _lazy_exports,
    _parameter_annotation,
    _simple_name,
    _subscript_base,
    _top_level,
)

_UNION_FORMS = frozenset({"Optional", "Union"})


class SourceIndex:
    """Resolve names in Python source to their definitions, without importing it.

    The error contract follows an action's calls below the controller
    (``ReachableErrors``). That needs what an import would give — which module
    a name comes from, which class an attribute holds, which method a call
    lands on — read from source only, so the generator keeps its no-boot
    promise.

    A resolution here is EXACT or absent, never a guess: a name resolves
    through the imports, the generated barrels' ``_LAZY_EXPORTS`` and the
    kernel aliases the way Python would; an attribute's class is the one every
    assignment and annotation agrees on (the type a constructor parameter
    names is the one the container injects — for an abstract contract the
    container binds to an implementation, it is the contract, whose methods
    this index cannot see past); a union, a conflict or an unknown value
    resolves to nothing.

    ``roots`` are the import roots (``sys.path`` entries); ``aliases`` maps a
    package name to the package it mirrors at runtime — a deployable's kernel
    barrels register ``commons.gates`` under ``app.gates`` — and applies only
    where no file of that name exists, as the mirroring itself does.
    """

    def __init__(self, roots: Sequence[Path], aliases: Mapping[str, str] | None = None):
        self._roots = tuple(roots)
        self._aliases = sorted((aliases or {}).items(), key=lambda item: -len(item[0]))
        self._modules: dict[str, SourceModule | None] = {}
        self._by_path: dict[Path, SourceModule] = {}
        self._mro: dict[SourceClass, tuple[SourceClass, ...]] = {}
        self._attributes: dict[tuple[SourceClass, str], SourceClass | None] = {}

    # -- modules ---------------------------------------------------------

    def module(self, name: str) -> SourceModule | None:
        """The module ``name`` imports, or ``None`` outside the roots."""
        if name in self._modules:
            return self._modules[name]
        self._modules[name] = None  # an alias cycle resolves to nothing
        found = self._load(name)
        if found is None:
            for alias, target in self._aliases:
                if name == alias or name.startswith(f"{alias}."):
                    found = self.module(target + name[len(alias) :])
                    break
        self._modules[name] = found
        return found

    def _load(self, name: str) -> SourceModule | None:
        if not name:
            return None
        for root in self._roots:
            base = root.joinpath(*name.split("."))
            for path in (base.with_name(f"{base.name}.py"), base / "__init__.py"):
                if path.is_file():
                    return self._parse(name, path)
        return None

    def _parse(self, name: str, path: Path) -> SourceModule:
        resolved = path.resolve()
        if resolved in self._by_path:
            return self._by_path[resolved]
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        package = name if path.name == "__init__.py" else name.rpartition(".")[0]
        functions: dict = {}
        classes: dict = {}
        for statement in _top_level(tree.body):
            if isinstance(statement, _FUNCTION_NODES):
                functions[statement.name] = statement
            elif isinstance(statement, ast.ClassDef):
                classes[statement.name] = statement
        module = SourceModule(
            name=name,
            package=package,
            path=path,
            functions=functions,
            classes=classes,
            imports=_imports(tree, package),
            lazy=_lazy_exports(tree, package),
        )
        self._by_path[resolved] = module
        return module

    # -- names -----------------------------------------------------------

    def resolve(
        self, module: SourceModule, name: str, _seen: frozenset = frozenset()
    ) -> Any:
        """What ``name`` is bound to at ``module``'s top level: a
        ``SourceModule``, ``SourceClass`` or ``SourceFunction``, or ``None``."""
        if (module.name, name) in _seen:
            return None
        seen = _seen | {(module.name, name)}
        if name in module.classes:
            return SourceClass(module, module.classes[name])
        if name in module.functions:
            return SourceFunction(module, module.functions[name])
        imported = module.imports.get(name)
        if imported is not None:
            source_name, attribute = imported
            if attribute is None:
                found = self.module(source_name)
            else:
                source = self.module(source_name)
                found = source and self.resolve(source, attribute, seen)
                found = found or self.module(f"{source_name}.{attribute}")
            # A generated barrel's export replaces a same-named child module.
            if not isinstance(found, SourceModule) or name not in module.lazy:
                return found
        if name in module.lazy:
            target, attribute = module.lazy[name]
            source = self.module(target)
            exported = source and self.resolve(source, attribute, seen)
            return exported or self.module(f"{target}.{attribute}")
        if module.path.name == "__init__.py":
            return self.module(f"{module.name}.{name}")
        return None

    def resolve_expr(self, module: SourceModule, node: ast.AST) -> Any:
        """A ``name`` or ``package.module.name`` expression's definition."""
        if isinstance(node, ast.Name):
            return self.resolve(module, node.id)
        if isinstance(node, ast.Attribute):
            base = self.resolve_expr(module, node.value)
            if isinstance(base, SourceModule):
                return self.resolve(base, node.attr)
        return None

    def annotation_class(
        self, module: SourceModule, node: ast.AST | None
    ) -> SourceClass | None:
        """The one class an annotation names: ``X``, ``X | None``,
        ``Optional[X]``, ``"X"``. A union of two classes, or of a class and a
        name this index cannot read, names none — it would be a guess."""
        members = self._annotation_members(module, node) if node is not None else []
        if len(members) == 1 and isinstance(members[0], SourceClass):
            return members[0]
        return None

    def annotation_classes(
        self, module: SourceModule, node: ast.AST | None
    ) -> tuple[SourceClass, ...]:
        """Every class an annotation names, each member of a union: what a
        function annotated ``-> InvalidInput | ValidationFailed`` can return.
        A member this index cannot read adds nothing."""
        members = self._annotation_members(module, node) if node is not None else []
        return tuple(member for member in members if isinstance(member, SourceClass))

    def _annotation_members(self, module: SourceModule, node: ast.AST) -> list:
        if isinstance(node, ast.Constant):
            if node.value is None:
                return []
            if isinstance(node.value, str):
                try:
                    parsed = ast.parse(node.value, mode="eval").body
                except SyntaxError:
                    return [None]
                return self._annotation_members(module, parsed)
            return [None]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            left = self._annotation_members(module, node.left)
            return left + self._annotation_members(module, node.right)
        if isinstance(node, ast.Subscript):
            form = _simple_name(node.value)
            if form in _UNION_FORMS:
                items = (
                    node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
                )
                return [
                    member
                    for item in items
                    for member in self._annotation_members(module, item)
                ]
            if form == "Annotated" and isinstance(node.slice, ast.Tuple):
                return self._annotation_members(module, node.slice.elts[0])
            node = node.value  # ``Repository[T]`` is still a Repository
        found = self.resolve_expr(module, node)
        return [found if isinstance(found, SourceClass) else None]

    # -- classes ---------------------------------------------------------

    def mro(self, cls: SourceClass) -> tuple[SourceClass, ...]:
        """The class's resolution order over the bases this index can read
        (C3, as Python computes it)."""
        if cls in self._mro:
            return self._mro[cls]
        self._mro[cls] = (cls,)  # a base cycle stops here
        bases = [
            base
            for base in (
                self.resolve_expr(cls.module, _subscript_base(node))
                for node in cls.node.bases
            )
            if isinstance(base, SourceClass)
        ]
        order = (cls, *_c3([list(self.mro(base)) for base in bases] + [bases]))
        self._mro[cls] = order
        return order

    def lineage(self, cls: SourceClass) -> frozenset[str]:
        """Every class name ``cls`` is an instance of, the builtin bases
        included — what an ``except`` clause naming any of them catches."""
        names: set[str] = set()
        for klass in self.mro(cls):
            names.add(klass.node.name)
            for base in klass.node.bases:
                name = _simple_name(_subscript_base(base))
                builtin = getattr(builtins, name or "", None)
                if isinstance(builtin, type) and issubclass(builtin, BaseException):
                    names.update(entry.__name__ for entry in builtin.__mro__)
                elif name:
                    names.add(name)
        return frozenset(names)

    def method(
        self, cls: SourceClass, name: str, *, after: SourceClass | None = None
    ) -> SourceFunction | None:
        """The ``name`` method an instance of ``cls`` calls; with ``after``,
        the one ``super()`` reaches from a method defined on ``after``."""
        order = self.mro(cls)
        if after is not None:
            order = order[order.index(after) + 1 :] if after in order else ()
        for klass in order:
            for statement in klass.node.body:
                if isinstance(statement, _FUNCTION_NODES) and statement.name == name:
                    return SourceFunction(klass.module, statement, klass)
        return None

    def attribute_class(self, cls: SourceClass, attribute: str) -> SourceClass | None:
        """The class ``self.<attribute>`` holds on an instance of ``cls``:
        what every ``self.<attribute> = ...`` and annotation in its MRO agrees
        on — a constructor parameter's annotation, a constructed class, a
        ``@property``'s return annotation."""
        key = (cls, attribute)
        if key in self._attributes:
            return self._attributes[key]
        self._attributes[key] = None
        found: list[SourceClass | None] = []
        for klass in self.mro(cls):
            for statement in klass.node.body:
                if (
                    isinstance(statement, ast.AnnAssign)
                    and isinstance(statement.target, ast.Name)
                    and statement.target.id == attribute
                ):
                    found.append(
                        self.annotation_class(klass.module, statement.annotation)
                    )
                if not isinstance(statement, _FUNCTION_NODES):
                    continue
                if statement.name == attribute and any(
                    _simple_name(decorator) == "property"
                    for decorator in statement.decorator_list
                ):
                    found.append(self.annotation_class(klass.module, statement.returns))
                found.extend(self._self_assignments(klass.module, statement, attribute))
        classes = set(found)
        resolved = classes.pop() if len(classes) == 1 else None
        self._attributes[key] = resolved
        return resolved

    def _self_assignments(
        self, module: SourceModule, function: Any, attribute: str
    ) -> list[SourceClass | None]:
        receiver = _first_parameter(function)
        if receiver is None:
            return []
        values: list[SourceClass | None] = []
        for node in ast.walk(function):
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
                if isinstance(node, ast.AnnAssign)
                else []
            )
            for target in targets:
                if not (
                    isinstance(target, ast.Attribute)
                    and target.attr == attribute
                    and isinstance(target.value, ast.Name)
                    and target.value.id == receiver
                ):
                    continue
                if isinstance(node, ast.AnnAssign):
                    values.append(self.annotation_class(module, node.annotation))
                else:
                    values.extend(self.value_classes(module, function, node.value))
        return values

    def value_classes(self, module: SourceModule, function: Any, value: ast.AST) -> list:
        """The classes an assigned value can be: a parameter's annotation, a
        constructed class, either side of ``a or B()`` / ``a if c else B()``.
        ``None`` stands for a value this index cannot type."""
        if isinstance(value, ast.Constant) and value.value is None:
            return []
        if isinstance(value, ast.BoolOp):
            return [
                found
                for item in value.values
                for found in self.value_classes(module, function, item)
            ]
        if isinstance(value, ast.IfExp):
            body = self.value_classes(module, function, value.body)
            return body + self.value_classes(module, function, value.orelse)
        if isinstance(value, ast.Name):
            annotation = _parameter_annotation(function, value.id)
            return [self.annotation_class(module, annotation)]
        if isinstance(value, ast.Call):
            built = self.resolve_expr(module, value.func)
            return [built if isinstance(built, SourceClass) else None]
        return [None]
