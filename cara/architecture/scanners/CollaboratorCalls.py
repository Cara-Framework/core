"""CollaboratorCalls: a call into an injected collaborator matches its
declared shape.

The codebase's universal DI convention is constructor injection with a type
annotation: ``def __init__(self, foo_service: FooService): self.foo_service =
foo_service``. Nothing in the existing Guard Pack, the unit suite (which mocks
the collaborator) or the static route harness (which only proves a controller
ACTION exists) verifies that a later ``self.foo_service.bar(...)`` call
actually matches ``FooService.bar``'s real signature — a typo'd method name or
a mismatched positional/keyword shape surfaces only as a live 500.

This scanner is pure AST and answers exactly that question, for the narrow
case where it can be fully certain:

1. Build a class repository from every ``ClassDef`` in the scanned trees:
   its own (undecorated) methods, and its base names — resolved by simple
   name across the whole scanned tree, mixins included (``_FooServiceMixin``
   is heavily used here, so a class's usable method set is itself plus every
   resolvable ancestor's).
2. For every class that declares ``__init__`` with a plain
   ``self.<attr> = <param>`` assignment where ``<param>`` carries a plain,
   resolvable type annotation, record ``<attr> -> <TypeName>``. That map
   travels with the WHOLE composed instance, not just the class that wrote
   it — a mixin's methods run with ``self`` bound to the concrete class that
   mixes it in, so a mixin's own call sites are checked against the
   composing class's attribute map too.
3. Walk every method reachable from that composing class (itself plus its
   resolvable bases) for ``self.<attr>.<method>(...)`` calls — both the
   direct shape and the ``ExecutionContext.run_in_thread(self.<attr>.<method>,
   *args, **kwargs)`` / ``asyncio.to_thread(...)`` thread-offload shape
   (``manifest.collaborator_call_forwarders``), since that is how nearly
   every async controller reaches a sync service in this codebase. Each call
   is checked against the resolved collaborator type's real signature:
   the method must exist, the call must not pass more positional arguments
   than the method accepts (unless it takes ``*args``), and the call must
   not omit a required parameter — positional-or-keyword or keyword-only —
   that has no default (unless the call passes ``**kwargs``, which already
   makes the call unauditable and is skipped, see below).

Zero false positives beats catching more bugs. A call is SKIPPED — never
flagged — whenever certainty is not available:

* the attribute has no annotation, or the annotation is a string, ``Any``,
  a union, a generic, or a class whose own bases include ``Protocol``;
* the constructor assigns anything other than the bare parameter name, the
  parameter itself carries a default, or ``self.<attr>`` is assigned again
  anywhere else reachable from the composing class (a possible later
  rebind — the initial annotation can no longer be trusted);
* the collaborator type is not defined in the scanned trees, its name is
  ambiguous (multiple classes share it), or ANY base in its resolution
  chain cannot be resolved the same way (an unresolved base may supply the
  method) or defines ``__getattr__``/``__getattribute__``;
* the resolved method carries any decorator at all (``@property``,
  ``@staticmethod``, ``@classmethod``, ``functools.wraps``, or anything
  else — a decorator can change the effective call shape and this scanner
  does not attempt to reason about which ones do);
* the call site (or the forwarded call) unpacks ``*args``/``**kwargs``.

Existing pre-rule mismatches are exact, shrink-only, dated
``manifest.collaborator_call_exemptions`` pins (``"<path>:<line>:self.<attr>.
<method>"``) — sunset debt per DOCTRINE §11, not a standing exception.
"""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from cara.architecture._ast_utils import parse, python_files, relpath
from cara.architecture.Finding import Finding
from cara.architecture.Manifest import Manifest

COLLABORATOR_CALLS_KEY = "collaborator_calls"

_DYNAMIC_ATTR_METHODS = frozenset({"__getattr__", "__getattribute__"})

_UNKNOWN = object()  # tri-state sentinel: certainty is unavailable, skip


@dataclass(frozen=True, slots=True)
class _Param:
    name: str
    has_default: bool


@dataclass(frozen=True, slots=True)
class _Signature:
    positional: tuple[_Param, ...]
    has_vararg: bool
    kwonly: tuple[_Param, ...]
    has_kwarg: bool


@dataclass(frozen=True, slots=True)
class _ClassEntry:
    rel: str
    bases: tuple[str, ...]
    bases_fully_resolvable: bool
    methods: dict[str, _Signature]
    decorated: frozenset[str]
    dynamic_getattr: bool
    init_node: ast.FunctionDef | ast.AsyncFunctionDef | None
    method_nodes: tuple[ast.FunctionDef | ast.AsyncFunctionDef, ...]


@dataclass(frozen=True, slots=True)
class _CallSite:
    attr: str
    method: str
    args: tuple[ast.expr, ...]
    keywords: tuple[ast.keyword, ...]
    lineno: int


def _simple_name(node: ast.expr) -> str | None:
    """A base/annotation name this scanner can reason about, or ``None``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> _Signature:
    args = node.args
    combined = [*args.posonlyargs, *args.args]
    num_defaults = len(args.defaults)
    positional = tuple(
        _Param(arg.arg, index >= len(combined) - num_defaults)
        for index, arg in enumerate(combined)
        if index != 0  # drop the implicit self/cls
    )
    kwonly = tuple(
        _Param(arg.arg, default is not None)
        for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
    )
    return _Signature(
        positional=positional,
        has_vararg=args.vararg is not None,
        kwonly=kwonly,
        has_kwarg=args.kwarg is not None,
    )


def _build_entry(node: ast.ClassDef, rel: str) -> _ClassEntry:
    bases: list[str] = []
    bases_fully_resolvable = True
    for base in node.bases:
        name = _simple_name(base)
        if name is None:
            bases_fully_resolvable = False
            continue
        bases.append(name)

    methods: dict[str, _Signature] = {}
    decorated: set[str] = set()
    dynamic_getattr = False
    init_node: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    method_nodes: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for stmt in node.body:
        if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        method_nodes.append(stmt)
        if stmt.name == "__init__":
            init_node = stmt
        if stmt.name in _DYNAMIC_ATTR_METHODS:
            dynamic_getattr = True
        if stmt.decorator_list:
            decorated.add(stmt.name)
            methods.pop(stmt.name, None)
        else:
            decorated.discard(stmt.name)
            methods[stmt.name] = _signature(stmt)

    return _ClassEntry(
        rel=rel,
        bases=tuple(bases),
        bases_fully_resolvable=bases_fully_resolvable,
        methods=methods,
        decorated=frozenset(decorated),
        dynamic_getattr=dynamic_getattr,
        init_node=init_node,
        method_nodes=tuple(method_nodes),
    )


def _collect_classes(manifest: Manifest) -> dict[str, list[_ClassEntry]]:
    classes: dict[str, list[_ClassEntry]] = {}
    seen: set[Path] = set()
    for root in manifest.roots.scan_dirs(COLLABORATOR_CALLS_KEY):
        for path in python_files(root):
            resolved = path.resolve()
            if resolved in seen or path.name == "__init__.py":
                continue
            seen.add(resolved)
            tree = parse(path)
            if tree is None:
                continue
            rel = relpath(path, manifest.roots.deployable)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    classes.setdefault(node.name, []).append(_build_entry(node, rel))
    return classes


def _unique(name: str, classes: dict[str, list[_ClassEntry]]) -> _ClassEntry | None:
    """The single scanned-tree class named ``name`` — ``None`` if it is not
    defined here at all, or the name is ambiguous (defined more than once)."""
    entries = classes.get(name)
    if entries is None or len(entries) != 1:
        return None
    return entries[0]


def _find_method(
    name: str,
    method: str,
    classes: dict[str, list[_ClassEntry]],
    seen: frozenset[str],
) -> _Signature | None | object:
    """``_Signature`` if resolvable, ``None`` if definitively absent,
    ``_UNKNOWN`` when certainty is unavailable anywhere along the chain."""
    if name in seen:
        return _UNKNOWN
    entry = _unique(name, classes)
    if entry is None:
        return _UNKNOWN
    if method in entry.decorated:
        return _UNKNOWN
    if method in entry.methods:
        return entry.methods[method]
    if entry.dynamic_getattr:
        return _UNKNOWN
    if not entry.bases_fully_resolvable:
        return _UNKNOWN
    nested = seen | {name}
    for base in entry.bases:
        result = _find_method(base, method, classes, nested)
        if result is _UNKNOWN:
            # Python would search this (unresolvable) base before any base
            # listed after it — no later branch can rescue certainty.
            return _UNKNOWN
        if result is not None:
            return result
    return None


def _collaborator_type(
    annotation: ast.expr | None, classes: dict[str, list[_ClassEntry]]
) -> str | None:
    name = _simple_name(annotation) if annotation is not None else None
    if name is None or name in {"Any", "None"}:
        return None
    entry = _unique(name, classes)
    if entry is None:
        return None
    if "Protocol" in entry.bases:
        return None
    return name


def _init_attr_map(
    init_node: ast.FunctionDef | ast.AsyncFunctionDef | None,
    classes: dict[str, list[_ClassEntry]],
) -> dict[str, str]:
    if init_node is None:
        return {}
    args = init_node.args
    combined_pos = [*args.posonlyargs, *args.args]
    num_defaults = len(args.defaults)
    defaulted = {
        arg.arg
        for index, arg in enumerate(combined_pos)
        if index >= len(combined_pos) - num_defaults
    }
    defaulted.update(
        arg.arg
        for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
        if default is not None
    )
    param_types: dict[str, str] = {}
    for arg in [*combined_pos, *args.kwonlyargs]:
        if arg.arg in defaulted:
            continue  # a default (often `= None`) makes the annotation unreliable
        type_name = _collaborator_type(arg.annotation, classes)
        if type_name is not None:
            param_types[arg.arg] = type_name

    attr_map: dict[str, str] = {}
    for stmt in init_node.body:
        if isinstance(stmt, ast.Assign):
            targets, value = stmt.targets, stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            targets, value = [stmt.target], stmt.value
        else:
            continue
        if not (isinstance(value, ast.Name) and value.id in param_types):
            continue  # not a bare pass-through of an annotated parameter
        for target in targets:
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
            ):
                attr_map[target.attr] = param_types[value.id]
    return attr_map


def _self_attr_assignment_counts(nodes: Iterable[ast.AST]) -> Counter[str]:
    """How many times ``self.<attr> = ...`` appears across ``nodes`` — used
    to detect a later rebind that would make the constructor's annotation
    unreliable for the rest of the composed instance's lifetime."""
    counts: Counter[str] = Counter()
    for fn in nodes:
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets = [node.target]
            else:
                continue
            for target in targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                ):
                    counts[target.attr] += 1
    return counts


def _reachable(
    name: str, classes: dict[str, list[_ClassEntry]], seen: set[str]
) -> list[tuple[str, _ClassEntry]]:
    """``name`` plus every resolvable ancestor — the classes whose methods
    execute with ``self`` bound to a ``name`` instance."""
    if name in seen:
        return []
    seen.add(name)
    entry = _unique(name, classes)
    if entry is None:
        return []
    out = [(name, entry)]
    for base in entry.bases:
        out.extend(_reachable(base, classes, seen))
    return out


def _self_attr_ref(node: ast.expr) -> tuple[str, str] | None:
    """``(attr, method)`` for a bare ``self.<attr>.<method>`` reference."""
    if not isinstance(node, ast.Attribute):
        return None
    owner = node.value
    if not isinstance(owner, ast.Attribute):
        return None
    base = owner.value
    if isinstance(base, ast.Name) and base.id == "self":
        return owner.attr, node.attr
    return None


def _dotted_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _call_sites(
    nodes: Iterable[ast.FunctionDef | ast.AsyncFunctionDef], forwarders: frozenset[str]
) -> Iterator[_CallSite]:
    for fn in nodes:
        for node in ast.walk(fn):
            if not isinstance(node, ast.Call):
                continue
            direct = _self_attr_ref(node.func)
            if direct is not None:
                yield _CallSite(
                    direct[0],
                    direct[1],
                    tuple(node.args),
                    tuple(node.keywords),
                    node.lineno,
                )
                continue
            # `ExecutionContext.run_in_thread(self.attr.method, *args, **kwargs)`
            # — the codebase's thread-offload idiom — forwards to
            # `self.attr.method(*args, **kwargs)` exactly (asyncio.to_thread
            # drop-in). Treat it as the equivalent direct call.
            if not node.args or _dotted_name(node.func) not in forwarders:
                continue
            forwarded = _self_attr_ref(node.args[0])
            if forwarded is not None:
                yield _CallSite(
                    forwarded[0],
                    forwarded[1],
                    tuple(node.args[1:]),
                    tuple(node.keywords),
                    node.args[0].lineno,
                )


def _call_issue(
    type_name: str,
    method: str,
    call: _CallSite,
    classes: dict[str, list[_ClassEntry]],
) -> str | None:
    if any(isinstance(a, ast.Starred) for a in call.args):
        return None  # `*args` unpacking — true arity is not visible here
    if any(kw.arg is None for kw in call.keywords):
        return None  # `**kwargs` unpacking — same reasoning

    result = _find_method(type_name, method, classes, frozenset())
    if result is _UNKNOWN:
        return None
    if result is None:
        return "has no such method"

    sig = result
    passed = len(call.args)
    kw_names = {kw.arg for kw in call.keywords}
    problems: list[str] = []

    if passed > len(sig.positional) and not sig.has_vararg:
        problems.append(
            f"accepts {len(sig.positional)} positional argument(s) but {passed} were passed"
        )
    for index, param in enumerate(sig.positional):
        if param.has_default or index < passed or param.name in kw_names:
            continue
        problems.append(
            f"requires {param.name!r} (not passed positionally or by keyword)"
        )
    for param in sig.kwonly:
        if param.has_default or param.name in kw_names:
            continue
        problems.append(f"requires keyword-only {param.name!r} but the call omits it")

    if not problems:
        return None
    return "; ".join(problems)


class CollaboratorCalls:
    """A call into an injected collaborator matches its declared shape."""

    @staticmethod
    def scan(manifest: Manifest) -> list[Finding]:
        classes = _collect_classes(manifest)
        forwarders = manifest.collaborator_call_forwarders
        exemptions = manifest.collaborator_call_exemptions

        raw: dict[str, Finding] = {}
        for name, entries in classes.items():
            if len(entries) != 1:
                continue  # ambiguous class name — cannot safely be a DI root
            entry = entries[0]
            attr_map = _init_attr_map(entry.init_node, classes)
            if not attr_map:
                continue

            member_classes = _reachable(name, classes, set())
            all_nodes = [n for _, member in member_classes for n in member.method_nodes]
            counts = _self_attr_assignment_counts(all_nodes)
            attr_map = {
                attr: type_name
                for attr, type_name in attr_map.items()
                if counts[attr] == 1
            }
            if not attr_map:
                continue

            for _, member in member_classes:
                for site in _call_sites(member.method_nodes, forwarders):
                    type_name = attr_map.get(site.attr)
                    if type_name is None:
                        continue
                    issue = _call_issue(type_name, site.method, site, classes)
                    if issue is None:
                        continue
                    key = f"{member.rel}:{site.lineno}:self.{site.attr}.{site.method}"
                    message = (
                        f"self.{site.attr}.{site.method}(...) -> {type_name}.{site.method}: "
                        f"{issue}"
                    )
                    raw.setdefault(key, Finding(member.rel, site.lineno, message))

        findings: list[Finding] = []
        for key in sorted(raw):
            if key not in exemptions:
                findings.append(raw[key])
        for key in sorted(exemptions):
            if key not in raw:
                findings.append(
                    Finding(
                        "collaborator_call_exemptions",
                        0,
                        f"{key}: no matching finding remains — delete the pin",
                    )
                )
        return findings
