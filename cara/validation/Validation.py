"""
Validation Core for the Cara framework.

This module provides the core validation logic, managing validation rules and executing validation
checks.
"""

import importlib
import inspect
import os
from typing import Any, Dict, Type

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
    """

    def __init__(self) -> None:
        self._errors: Dict[str, list[str]] = {}
        self._validated: Dict[str, Any] = {}
        # Dynamically load all rule classes from cara/validation/rules/
        self.__rule_classes = self._discover_rules()

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
                        # rule key is the filename without "Rule" (lowercased)
                        key = module_name[:-4].lower()  # remove "Rule" suffix
                        classes[key] = obj
        return classes

    @staticmethod
    def make(
        data: Dict[str, Any],
        rules: Dict[str, str],
        messages: Dict[str, str] = None,
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

        # Prepare custom messages if provided
        custom_messages = messages or {}

        for field, rule_string in rules.items():
            value = data.get(field)
            field_passed = True

            # Handle nullable logic: if field is nullable and value is None/empty, skip validation
            if "nullable" in rule_string and (
                value is None or (isinstance(value, str) and value.strip() == "")
            ):
                instance._validated[field] = value
                continue

            for token in rule_string.split("|"):
                rule_name, params = instance._split_token(token)
                rule_cls = instance._Validation__rule_classes.get(rule_name)
                if not rule_cls:
                    raise RuleNotFoundException(
                        f"Rule '{rule_name}' is not registered."
                    )
                rule_instance = rule_cls()
                # Pass the full data for rules that need access to other fields (like confirmed)
                params["_data"] = data

                # Add custom messages to params with priority order
                if custom_messages:
                    custom_message = None

                    # Priority 1: field.rule specific message (highest priority)
                    field_rule_key = f"{field}.{rule_name}"
                    if field_rule_key in custom_messages:
                        custom_message = custom_messages[field_rule_key]

                    # Priority 2: field specific message (medium priority)
                    elif field in custom_messages:
                        custom_message = custom_messages[field]

                    # Priority 3: rule specific message (lowest priority)
                    elif rule_name in custom_messages:
                        custom_message = custom_messages[rule_name]

                    # If we found a custom message, add it to params
                    if custom_message:
                        params["_custom_message"] = custom_message
                        params["_field"] = field
                        params["_rule"] = rule_name
                        params["_value"] = value

                if not rule_instance.validate(field, value, params):
                    if field not in instance._errors:
                        instance._errors[field] = []
                    instance._errors[field].append(rule_instance.message(field, params))
                    field_passed = False

            if field_passed:
                # All rules for this field passed
                instance._validated[field] = value

        return instance

    def fails(self) -> bool:
        """Returns True if validation failed."""
        return bool(self._errors)

    def passes(self) -> bool:
        """Returns True if validation passed."""
        return not bool(self._errors)

    def errors(self) -> ValidationErrors:
        """Returns ValidationErrors object with all errors."""
        return ValidationErrors(self._errors)

    def first_error(self, field: str = None) -> str:
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
