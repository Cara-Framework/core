"""
Validation Core for the Cara framework.

This module provides the core validation logic, managing validation rules and executing validation
checks.
"""

from __future__ import annotations

import importlib
import inspect
import os
import re as _re
from collections.abc import Iterable
from typing import Any

from cara.exceptions import (
    InvalidRuleFormatException,
    RuleNotFoundException,
)
from cara.validation.contracts import (
    Rule,
    ValidationContract,
)
from cara.validation.ValidationErrors import ValidationErrors


class Validation(ValidationContract):
    """
    Core validator that applies string‐based rules to a data payload.

    Rule strings use pipe delimiters (e.g. "required|email|min:5").

    Supports Laravel-style wildcard paths for validating array / nested
    elements, e.g. ``"slugs.*": "required|string"`` or
    ``"users.*.email": "required|email"``. Errors for wildcard-expanded
    entries are reported with concrete paths (``slugs.0``, ``slugs.1``,
    ``users.2.email`` …).
    """

    _WILDCARD = "*"

    # Laravel's "implicit" rules — the ones that still run when the key is
    # ABSENT from the payload, because absence is exactly what they judge
    # (Laravel: ``Validator::$implicitRules`` consulted by
    # ``presentOrRuleIsImplicit``). Every other rule describes the SHAPE of a
    # value, so it has nothing to say about a key that was never sent and is
    # skipped — absence is an error only when one of these asks for it.
    #
    _IMPLICIT_RULES = frozenset(
        {
            "required",
            "required_if",
            "required_unless",
            "required_with",
            "required_without",
            "present",
            "filled",
            "accepted",
            "missing",
            "prohibited",
        }
    )

    # Rules that answer their question by BINDING THE RAW VALUE into a typed
    # column comparison. They are skipped once an earlier rule in the same
    # chain has already rejected the value: the lookup is meaningless (the
    # shape rule already said "invalid"), and on a strict engine it is a
    # crash. Postgres raises ``invalid input syntax for type bigint`` for
    # ``exists:product,id`` with ``PRD01K…``, and that QueryException escapes
    # the validator as a 500 where validation owes the caller a 422 — the
    # rule chain ``required|integer|exists:product,id`` was already correct,
    # so the fix belongs here rather than in every FormRequest.
    _DB_BACKED_RULES = frozenset({"exists", "unique"})

    # Class-level registry of user-supplied custom rules (Laravel parity).
    # Mapping: rule_name (lowercase) → Rule class.
    _custom_rules: dict[str, type[Rule]] = {}

    def __init__(self) -> None:
        self._errors: dict[str, list[str]] = {}
        self._validated: dict[str, Any] = {}
        # Dynamically load all rule classes from cara/validation/rules/
        self.__rule_classes = self._discover_rules()
        # Merge user-supplied rules registered via Validation.extend(...).
        self.__rule_classes.update(self._custom_rules)
        # Post-validation hook callbacks (Laravel: $validator->after(cb)).
        self._after_callbacks: list = []

    def _discover_rules(
        self,
    ) -> dict[str, type[Rule]]:
        """
        Inspect the 'cara.validation.rules' package for any class whose name ends with 'Rule' and
        implements Rule.

        Returns a mapping: rule_name (lowercase) → RuleClass.
        E.g. "required" → RequiredRule
        """
        rules_pkg = "cara.validation.rules"
        pkg_dir = os.path.dirname(__file__) + "/rules"
        classes: dict[str, type[Rule]] = {}

        for filename in os.listdir(pkg_dir):
            if filename.endswith("Rule.py") and not filename.startswith("__"):
                module_name = filename[:-3]  # strip ".py"
                qualified = f"{rules_pkg}.{module_name}"
                module = importlib.import_module(qualified)
                for _, obj in inspect.getmembers(module, inspect.isclass):
                    # Check if this class inherits Rule and is defined in this module
                    if (
                        issubclass(obj, Rule)
                        and obj is not Rule
                        and obj.__module__ == qualified
                    ):
                        base = module_name[:-4]  # strip "Rule" suffix
                        canonical = (
                            "alpha_num"
                            if base == "Alphanum"
                            else self._camel_to_snake(base)
                        )
                        classes[canonical] = obj
        return classes

    @staticmethod
    def _camel_to_snake(name: str) -> str:
        """CamelCase → snake_case (e.g. ``RequiredIf`` → ``required_if``)."""

        s1 = _re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return _re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    # ------------------------------------------------------------------ #
    # Public extension API                                               #
    # ------------------------------------------------------------------ #
    @classmethod
    def extend(cls, name: str, rule_class: type[Rule]) -> None:
        """Register a custom rule globally (Laravel ``Validator::extend``).

        Args:
            name: Rule identifier used in rule strings (e.g. ``"adult"``).
            rule_class: A class implementing the ``Rule`` contract.
        """
        if not issubclass(rule_class, Rule):
            raise TypeError(
                f"{rule_class.__name__} must inherit from cara.validation.contracts.Rule"
            )
        cls._custom_rules[name.lower()] = rule_class

    @classmethod
    def extend_many(cls, mapping: dict[str, type[Rule]]) -> None:
        """Register multiple custom rules at once."""
        for name, rule_class in mapping.items():
            cls.extend(name, rule_class)

    def after(self, callback) -> Validation:
        """Register an after-validation callback (Laravel parity).

        The callback receives the Validation instance and can inspect
        errors or call ``errors().add(field, msg)`` to add new errors.
        """
        self._after_callbacks.append(callback)
        return self

    @staticmethod
    def make(
        data: dict[str, Any],
        rules: dict[str, str],
        messages: dict[str, str] | None = None,
    ) -> Validation:
        """
        Laravel-style validation method with custom message support.

        Returns a new Validation instance so you can chain .fails() or .passes() checks.
        Usage:
        - validator = Validation.make(data, rules)
        - if validator.fails():
        - if validator.passes():
        """
        if not isinstance(rules, dict):
            raise InvalidRuleFormatException("Rules must be a dict of field→rule_string.")

        # Create new instance for this validation
        instance = Validation()
        instance._errors.clear()
        instance._validated.clear()

        custom_messages = messages or {}
        for field in instance._closed_object_violations(data, rules):
            instance._errors.setdefault(field, []).append(
                f"The {field} field is not allowed."
            )

        # Expand wildcard rule keys (e.g. "slugs.*") into concrete paths
        # against the incoming data. Non-wildcard keys pass through
        # unchanged so existing semantics (including data.get(field)
        # returning None for missing fields) are preserved.
        rule_plan: list[tuple[str, str, str, Any, bool]] = []
        for field, rule_string in rules.items():
            if instance._WILDCARD in field.split("."):
                any_expansion = False
                for concrete_field, value, provided in instance._expand_wildcard_field(
                    field, data
                ):
                    rule_plan.append(
                        (field, concrete_field, rule_string, value, provided)
                    )
                    any_expansion = True
                # If the wildcard produced no concrete paths (e.g. the
                # source array is missing/empty), we skip — the parent
                # field's own rule (array|required|min:N) is responsible
                # for catching that case. This mirrors Laravel.
                if not any_expansion:
                    continue
            else:
                resolved = next(
                    instance._walk_segments(field.split("."), data, []),
                    (field, None, False),
                )
                concrete_field, value, provided = resolved
                rule_plan.append((field, concrete_field, rule_string, value, provided))

        for (
            original_field,
            concrete_field,
            rule_string,
            value,
            was_provided,
        ) in rule_plan:
            field_passed = True

            # Precompute rule names for this field so individual rules can
            # consult the chain (Laravel parity: `min`/`max`/`between` treat
            # numeric-looking strings as numbers when `integer`/`numeric` is
            # present in the chain, and as length otherwise).
            _chain = tuple(
                instance._split_token(tok)[0] for tok in rule_string.split("|")
            )

            # Laravel ``sometimes``: validate only when the key is present.
            # Unlike ``nullable``, an explicitly sent null/blank still runs
            # the remaining rules and may fail.
            if "sometimes" in _chain and not was_provided:
                continue

            # Handle nullable logic: if 'nullable' is one of the RULE tokens
            # and the value is None/empty, skip validation. Check the
            # tokenized chain — NOT a substring of rule_string — so a value
            # inside an ``in:nullable,active`` parameter can't accidentally
            # make the field skip ``required``.
            #
            # Store ``None`` — NOT the raw blank — so "blank means null" is
            # true for CONSUMERS too, but only when the caller actually sent
            # the field. An omitted optional key must stay omitted from
            # ``validated()``; otherwise PATCH requests turn every absent
            # nullable field into an explicit clear. Pre-fix a whitespace-only query param
            # (``?offset=%20``; parse_qs keeps blank-only values) skipped the
            # ``integer``/``numeric`` rules here yet landed in ``validated()``
            # as ``" "``, so every downstream ``int(v.get("offset") or 0)``
            # crashed with ValueError → an unauthenticated 500 on endpoints
            # whose contract is "validation errors are 422". With ``None``
            # stored, both the ``or default`` and the ``is not None`` guard
            # idioms behave.
            if "nullable" in _chain and (
                value is None or (isinstance(value, str) and value.strip() == "")
            ):
                if was_provided:
                    instance._validated[concrete_field] = None
                continue

            # Laravel ``bail`` modifier: stop running further rules for the
            # SAME field after the first failure. Detected via the chain.
            bail = "bail" in _chain

            for token in rule_string.split("|"):
                rule_name, params = instance._split_token(token)
                # ``bail`` and ``sometimes`` are modifiers, not real rules.
                if rule_name in {"bail", "sometimes"}:
                    continue

                # Laravel parity: an ABSENT key is judged only by the implicit
                # (presence-family) rules. Shape rules — string, integer, in,
                # max … — describe a value, so they have nothing to say about a
                # key that was never sent.
                #
                # Pre-fix, absence was fed to every rule as ``None`` and any
                # non-nullable type rule rejected it, so a bare
                # ``"title": "string|max:512"`` made ``title`` *de facto
                # required*: `PATCH /products/{id}` with `{"description": "x"}`
                # answered 422 "The title field must be a string." for a title
                # the caller never mentioned — no partial update could reach a
                # controller. `required` is what makes absence an error;
                # `nullable` governs an explicitly-sent null, not absence.
                #
                # Scoped deliberately to ABSENCE. Laravel additionally routes
                # whitespace-only strings down this same path; cara does not,
                # and that is left alone — it is a separate semantic with a far
                # wider blast radius (``in:``/enum chains would start accepting
                # ``""``), not part of this defect.
                if not was_provided and rule_name not in instance._IMPLICIT_RULES:
                    continue

                # A shape rule already rejected this value — do not hand it to
                # a rule that binds it into a typed column comparison (§
                # ``_DB_BACKED_RULES``). Without this, ``integer`` failing and
                # ``exists`` still querying turns a 422 into a 500 on Postgres.
                if not field_passed and rule_name in instance._DB_BACKED_RULES:
                    continue

                params["_rules"] = _chain
                rule_cls = instance._Validation__rule_classes.get(rule_name)
                if not rule_cls:
                    raise RuleNotFoundException(f"Rule '{rule_name}' is not registered.")
                rule_instance = rule_cls()
                # Pass the full data for rules that need access to other fields (like confirmed)
                params["_data"] = data

                # Custom message resolution. For wildcard-expanded rules
                # we accept both the concrete path (``slugs.0.required``)
                # and the original wildcard pattern (``slugs.*.required``
                # / ``slugs.*``).
                if custom_messages:
                    custom_message = instance._resolve_custom_message(
                        custom_messages,
                        original_field,
                        concrete_field,
                        rule_name,
                    )
                    if custom_message:
                        params["_custom_message"] = custom_message
                        params["_field"] = concrete_field
                        params["_rule"] = rule_name
                        params["_value"] = value

                if not rule_instance.validate(concrete_field, value, params):
                    if concrete_field not in instance._errors:
                        instance._errors[concrete_field] = []
                    instance._errors[concrete_field].append(
                        rule_instance.message(concrete_field, params)
                    )
                    field_passed = False
                    if bail:
                        break

            if field_passed and was_provided:
                # Keep concrete paths until every rule has run. The final
                # projection below nests them and gives a declared parent
                # sole ownership of its subtree. Omitted optional fields stay
                # omitted rather than becoming synthetic ``None`` values.
                instance._validated[concrete_field] = value

        instance._validated = instance._nest_validated(instance._validated, data)

        # After-callbacks (registered via .after()) run lazily on the first
        # call to fails()/passes() so callers can chain registration after
        # make().
        return instance

    def _run_after_callbacks(self) -> None:
        """Run any registered after-validation callbacks exactly once."""
        if getattr(self, "_after_ran", False):
            return
        self._after_ran = True
        for cb in self._after_callbacks:
            cb(self)

    def fails(self) -> bool:
        """Returns True if validation failed."""
        self._run_after_callbacks()
        return bool(self._errors)

    def passes(self) -> bool:
        """Returns True if validation passed."""
        self._run_after_callbacks()
        return not bool(self._errors)

    def errors(self) -> ValidationErrors:
        """Returns ValidationErrors object with all errors."""
        return ValidationErrors(self._errors)

    def first_error(self, field: str | None = None) -> str:
        """Get the first error message for a field, or the first error overall."""
        if field:
            field_errors = self._errors.get(field, [])
            return field_errors[0] if field_errors else ""

        for field_errors in self._errors.values():
            if field_errors:
                return field_errors[0]
        return ""

    def all_errors(self) -> list[str]:
        """Get all error messages as a flat list."""
        all_messages = []
        for field_errors in self._errors.values():
            all_messages.extend(field_errors)
        return all_messages

    def validated(self) -> dict[str, Any]:
        """Return validated input in its original nested shape."""

        return self._validated.copy()

    @staticmethod
    def _nest_validated(flat: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
        """Project concrete rule paths into one nested payload.

        A declared parent owns the subtree's output location. Child results
        are overlaid inside that subtree so validator-owned normalization
        (for example, ``nullable`` blank-to-``None``) is preserved without
        leaking duplicate dotted top-level keys. When no parent rule exists,
        concrete dotted and wildcard paths are rebuilt using the source
        container types, including list indices.
        """

        nested: dict[str, Any] = {}
        for path, value in sorted(
            flat.items(),
            key=lambda item: (item[0].count("."), item[0]),
        ):
            Validation._set_validated_path(nested, source, path.split("."), value)
        return nested

    @staticmethod
    def _set_validated_path(
        target: dict[str, Any],
        source: Any,
        segments: list[str],
        value: Any,
    ) -> None:
        current_target: dict[str, Any] | list[Any] = target
        current_source = source
        for index, segment in enumerate(segments):
            leaf = index == len(segments) - 1
            if isinstance(current_source, list):
                try:
                    position = int(segment)
                except ValueError as exc:
                    raise ValueError(
                        f"validated list path segment is not an index: {segment}"
                    ) from exc
                if position < 0 or position >= len(current_source):
                    raise ValueError(f"validated list path index is invalid: {position}")
                if not isinstance(current_target, list):
                    raise ValueError("validated path container differs from source")
                while len(current_target) <= position:
                    current_target.append(None)
                if leaf:
                    current_target[position] = Validation._copy_validated_value(value)
                    return
                next_source = current_source[position]
                child: dict[str, Any] | list[Any] = (
                    [] if isinstance(next_source, list) else {}
                )
                if current_target[position] is None:
                    current_target[position] = child
                elif not isinstance(current_target[position], type(child)):
                    raise ValueError(
                        "validated path container conflicts with another rule"
                    )
                current_target = current_target[position]
                current_source = next_source
                continue

            if not isinstance(current_source, dict) or not isinstance(
                current_target, dict
            ):
                raise ValueError("validated path traverses a non-container value")
            if segment not in current_source:
                raise ValueError(f"validated path is absent from source: {segment}")
            if leaf:
                current_target[segment] = Validation._copy_validated_value(value)
                return
            next_source = current_source[segment]
            child = [] if isinstance(next_source, list) else {}
            existing = current_target.setdefault(segment, child)
            if not isinstance(existing, type(child)):
                raise ValueError("validated path container conflicts with another rule")
            current_target = existing
            current_source = next_source

    @staticmethod
    def _copy_validated_value(value: Any) -> Any:
        """Copy JSON containers while preserving opaque validated leaf objects."""

        if isinstance(value, dict):
            return {
                key: Validation._copy_validated_value(child)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [Validation._copy_validated_value(child) for child in value]
        return value

    def _split_token(self, token: str) -> tuple[str, dict[str, Any]]:
        """Given "min:5" or "required", returns ("min", {"min": "5"}) or ("required", {})."""
        if ":" in token:
            name, raw_param = token.split(":", 1)
            return name, {name: raw_param}
        return token, {}

    # ------------------------------------------------------------------ #
    # Wildcard helpers                                                   #
    # ------------------------------------------------------------------ #

    def _expand_wildcard_field(
        self, field: str, data: Any
    ) -> Iterable[tuple[str, Any, bool]]:
        """
        Expand a wildcard field pattern against ``data``.

        Yields ``(concrete_field, value, provided)`` tuples. The concrete field is a
        dot-delimited path with numeric indices in place of each ``*``,
        e.g. ``"slugs.*"`` with ``{"slugs": ["a","b"]}`` yields
        ``("slugs.0","a")`` and ``("slugs.1","b")``.

        If the data does not contain an array where a wildcard expects
        one, no pairs are yielded (validation for that pattern is
        skipped — the parent ``array`` rule handles shape errors).
        """
        segments = field.split(".")
        yield from self._walk_segments(segments, data, [])

    def _closed_object_violations(
        self, data: dict[str, Any], rules: dict[str, str]
    ) -> list[str]:
        """Return undeclared keys inside dicts with named child rules."""
        fields = {tuple(field.split(".")): rule for field, rule in rules.items()}
        parents: dict[tuple[str, ...], set[str] | None] = {}
        for path, rule in fields.items():
            names = {token.partition(":")[0] for token in rule.split("|")}
            if "dict" in names:
                parents[path] = set()

        for parent in tuple(parents):
            for child in fields:
                if len(child) != len(parent) + 1:
                    continue
                if all(
                    expected == actual
                    for expected, actual in zip(parent, child[: len(parent)], strict=True)
                ):
                    if child[-1] == "*":
                        parents[parent] = None
                        break
                    assert parents[parent] is not None
                    parents[parent].add(child[-1])

        violations: list[str] = []
        for parent, allowed in parents.items():
            if not allowed:
                continue
            pattern = ".".join(parent)
            if "*" in parent:
                values = self._expand_wildcard_field(pattern, data)
            else:
                values = self._walk_segments(list(parent), data, [])
            for concrete, value, _provided in values:
                if not isinstance(value, dict):
                    continue
                violations.extend(
                    f"{concrete}.{key}" for key in sorted(set(value) - allowed)
                )
        return violations

    def _walk_segments(
        self,
        segments: list[str],
        current: Any,
        path_so_far: list[str],
        provided: bool = True,
    ) -> Iterable[tuple[str, Any, bool]]:
        if not segments:
            yield ".".join(path_so_far), current, provided
            return

        head, rest = segments[0], segments[1:]

        if head == self._WILDCARD:
            if not provided:
                return
            if isinstance(current, list):
                for index, item in enumerate(current):
                    yield from self._walk_segments(rest, item, path_so_far + [str(index)])
            elif isinstance(current, dict):
                # Support dict-as-collection (Laravel also walks dicts).
                for key, item in current.items():
                    yield from self._walk_segments(rest, item, path_so_far + [str(key)])
            # Non-collection under a wildcard: nothing to emit.
            return

        # Regular (non-wildcard) path segment.
        if isinstance(current, dict):
            child_provided = provided and head in current
            child = current.get(head)
        elif isinstance(current, list):
            try:
                child = current[int(head)]
            except ValueError, IndexError:
                child = None
                child_provided = False
            else:
                child_provided = provided
        else:
            child = None
            child_provided = False

        yield from self._walk_segments(rest, child, path_so_far + [head], child_provided)

    def _resolve_custom_message(
        self,
        messages: dict[str, str],
        original_field: str,
        concrete_field: str,
        rule_name: str,
    ) -> str | None:
        """
        Pick the most specific custom message available.

        Priority (highest → lowest):
          1. concrete field + rule            (``slugs.0.required``)
          2. wildcard field + rule            (``slugs.*.required``)
          3. concrete field                   (``slugs.0``)
          4. wildcard field                   (``slugs.*``)
          5. rule name alone                  (``required``)
        """
        candidates = []
        candidates.append(f"{concrete_field}.{rule_name}")
        if original_field != concrete_field:
            candidates.append(f"{original_field}.{rule_name}")
        candidates.append(concrete_field)
        if original_field != concrete_field:
            candidates.append(original_field)
        candidates.append(rule_name)

        for key in candidates:
            if key in messages:
                return messages[key]
        return None
