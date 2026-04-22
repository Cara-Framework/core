"""
Validation Core for the Cara framework.

This module provides the core validation logic, managing validation rules and executing validation
checks.
"""

import importlib
import inspect
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type

from cara.exceptions import (
    InvalidRuleFormatException,
    RuleNotFoundException,
)
from cara.validation import ValidationErrors
from cara.validation.contracts import (
    Rule,
    Validation as ValidationContract,
)


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

    # Class-level registry of user-supplied custom rules (Laravel parity).
    # Mapping: rule_name (lowercase) → Rule class.
    _custom_rules: Dict[str, Type[Rule]] = {}

    def __init__(self) -> None:
        self._errors: Dict[str, list[str]] = {}
        self._validated: Dict[str, Any] = {}
        # Dynamically load all rule classes from cara/validation/rules/
        self.__rule_classes = self._discover_rules()
        # Merge user-supplied rules registered via Validation.extend(...).
        self.__rule_classes.update(self._custom_rules)
        # Post-validation hook callbacks (Laravel: $validator->after(cb)).
        self._after_callbacks: list = []

    def _discover_rules(
        self,
    ) -> Dict[str, Type[Rule]]:
        """
        Inspect the 'cara.validation.rules' package for any class whose name ends with 'Rule' and
        implements Rule.

        Returns a mapping: rule_name (lowercase) → RuleClass.
        E.g. "required" → RequiredRule
        """
        rules_pkg = "cara.validation.rules"
        pkg_dir = os.path.dirname(__file__) + "/rules"
        classes: Dict[str, Type[Rule]] = {}

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
                        # Register under both camel-concat form (backwards
                        # compatibility: "dateformat") and the Laravel
                        # snake_case form ("date_format") so both strings
                        # resolve to the same rule class.
                        base = module_name[:-4]  # strip "Rule" suffix
                        classes[base.lower()] = obj
                        snake = self._camel_to_snake(base)
                        if snake != base.lower():
                            classes[snake] = obj
        return classes

    @staticmethod
    def _camel_to_snake(name: str) -> str:
        """CamelCase → snake_case (e.g. ``RequiredIf`` → ``required_if``)."""
        import re as _re

        s1 = _re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        return _re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    # ------------------------------------------------------------------ #
    # Public extension API                                               #
    # ------------------------------------------------------------------ #
    @classmethod
    def extend(cls, name: str, rule_class: Type[Rule]) -> None:
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
    def extend_many(cls, mapping: Dict[str, Type[Rule]]) -> None:
        """Register multiple custom rules at once."""
        for name, rule_class in mapping.items():
            cls.extend(name, rule_class)

    def after(self, callback) -> "Validation":
        """Register an after-validation callback (Laravel parity).

        The callback receives the Validation instance and can inspect
        errors or call ``errors().add(field, msg)`` to add new errors.
        """
        self._after_callbacks.append(callback)
        return self

    @staticmethod
    def make(
        data: Dict[str, Any],
        rules: Dict[str, str],
        messages: Optional[Dict[str, str]] = None,
    ) -> "Validation":
        """
        Laravel-style validation method with custom message support.

        Returns a new Validation instance so you can chain .fails() or .passes() checks.
        Usage:
        - validator = Validation.make(data, rules)
        - if validator.fails():
        - if validator.passes():
        """
        if not isinstance(rules, dict):
            raise InvalidRuleFormatException(
                "Rules must be a dict of field→rule_string."
            )

        # Create new instance for this validation
        instance = Validation()
        instance._errors.clear()
        instance._validated.clear()

        custom_messages = messages or {}

        # Expand wildcard rule keys (e.g. "slugs.*") into concrete paths
        # against the incoming data. Non-wildcard keys pass through
        # unchanged so existing semantics (including data.get(field)
        # returning None for missing fields) are preserved.
        rule_plan: List[Tuple[str, str, str, Any, bool]] = []
        for field, rule_string in rules.items():
            if instance._WILDCARD in field.split("."):
                any_expansion = False
                for concrete_field, value in instance._expand_wildcard_field(field, data):
                    rule_plan.append((field, concrete_field, rule_string, value, True))
                    any_expansion = True
                # If the wildcard produced no concrete paths (e.g. the
                # source array is missing/empty), we skip — the parent
                # field's own rule (array|required|min:N) is responsible
                # for catching that case. This mirrors Laravel.
                if not any_expansion:
                    continue
            else:
                rule_plan.append((field, field, rule_string, data.get(field), False))

        for original_field, concrete_field, rule_string, value, is_wildcard in rule_plan:
            field_passed = True

            # Handle nullable logic: if field is nullable and value is None/empty, skip validation
            if "nullable" in rule_string and (
                value is None or (isinstance(value, str) and value.strip() == "")
            ):
                if not is_wildcard:
                    instance._validated[concrete_field] = value
                continue

            # Precompute rule names for this field so individual rules can
            # consult the chain (Laravel parity: `min`/`max`/`between` treat
            # numeric-looking strings as numbers when `integer`/`numeric` is
            # present in the chain, and as length otherwise).
            _chain = tuple(
                instance._split_token(tok)[0]
                for tok in rule_string.split("|")
            )

            # Laravel ``bail`` modifier: stop running further rules for the
            # SAME field after the first failure. Detected via the chain.
            bail = "bail" in _chain

            for token in rule_string.split("|"):
                rule_name, params = instance._split_token(token)
                # ``bail`` itself is a modifier, not a real rule.
                if rule_name == "bail":
                    continue
                params["_rules"] = _chain
                rule_cls = instance._Validation__rule_classes.get(rule_name)
                if not rule_cls:
                    raise RuleNotFoundException(
                        f"Rule '{rule_name}' is not registered."
                    )
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

            if field_passed and not is_wildcard:
                # All rules for this field passed. Wildcard-expanded
                # entries are not added to validated() individually; the
                # parent field (if declared) already carries the full
                # structure.
                instance._validated[concrete_field] = value

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
            try:
                cb(self)
            except Exception:
                from cara.facades import Log

                Log.error("Validation after-hook raised", exc_info=True)

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

    def first_error(self, field: Optional[str] = None) -> str:
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

    def validated(self) -> Dict[str, Any]:
        return self._validated.copy()

    def _split_token(self, token: str) -> (str, Dict[str, Any]):
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
    ) -> Iterable[Tuple[str, Any]]:
        """
        Expand a wildcard field pattern against ``data``.

        Yields ``(concrete_field, value)`` pairs. The concrete field is a
        dot-delimited path with numeric indices in place of each ``*``,
        e.g. ``"slugs.*"`` with ``{"slugs": ["a","b"]}`` yields
        ``("slugs.0","a")`` and ``("slugs.1","b")``.

        If the data does not contain an array where a wildcard expects
        one, no pairs are yielded (validation for that pattern is
        skipped — the parent ``array`` rule handles shape errors).
        """
        segments = field.split(".")
        yield from self._walk_segments(segments, data, [])

    def _walk_segments(
        self,
        segments: List[str],
        current: Any,
        path_so_far: List[str],
    ) -> Iterable[Tuple[str, Any]]:
        if not segments:
            yield ".".join(path_so_far), current
            return

        head, rest = segments[0], segments[1:]

        if head == self._WILDCARD:
            if isinstance(current, list):
                for index, item in enumerate(current):
                    yield from self._walk_segments(
                        rest, item, path_so_far + [str(index)]
                    )
            elif isinstance(current, dict):
                # Support dict-as-collection (Laravel also walks dicts).
                for key, item in current.items():
                    yield from self._walk_segments(
                        rest, item, path_so_far + [str(key)]
                    )
            # Non-collection under a wildcard: nothing to emit.
            return

        # Regular (non-wildcard) path segment.
        if isinstance(current, dict):
            child = current.get(head)
        elif isinstance(current, list):
            try:
                child = current[int(head)]
            except (ValueError, IndexError):
                return
        else:
            return

        yield from self._walk_segments(rest, child, path_so_far + [head])

    def _resolve_custom_message(
        self,
        messages: Dict[str, str],
        original_field: str,
        concrete_field: str,
        rule_name: str,
    ) -> Optional[str]:
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
