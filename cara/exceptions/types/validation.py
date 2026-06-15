"""
Validation Exception Type for the Cara framework.

This module defines exception types related to validation operations.
"""

from __future__ import annotations

__all__ = [
    "ValidationException",
    "RuleNotFoundException",
    "InvalidRuleFormatException",
]

from typing import Any

from .base import CaraException


class ValidationException(CaraException):
    """
    Exception raised when validation fails.

    Smart validation exception that analyzes and extracts validation content.
    """

    is_http_exception = True
    status_code = 422

    def __init__(
        self,
        validation_errors=None,
        message: str | None = None,
        errors: dict[str, Any] | None = None,
        status_code: int | None = None,
    ):
        self.validation_errors = validation_errors
        self._extracted_errors = {}
        self._extracted_message = "Validation failed"

        # Analyze and extract validation content
        self._analyze_validation_content()

        # Use provided values or extracted values
        if message is None:
            message = self._extracted_message
        if errors is None:
            errors = self._extracted_errors

        super().__init__(message)
        self.errors = errors

        if status_code:
            self.status_code = status_code

    def _analyze_validation_content(self) -> None:
        """Analyze validation_errors and extract meaningful content."""
        if self.validation_errors is None:
            return

        # Case 1: RouteParameterValidator dict structure
        if isinstance(self.validation_errors, dict):
            self._analyze_dict_structure()

        # Case 2: ValidationErrors object with methods
        elif hasattr(self.validation_errors, "errors"):
            self._analyze_validation_object()

    def _analyze_dict_structure(self) -> None:
        """Analyze dict-based validation errors (like RouteParameterValidator)."""
        validation_dict = self.validation_errors

        if "route_parameter_validation_failed" in validation_dict:
            # RouteParameterValidator format
            if "first_error" in validation_dict and validation_dict["first_error"]:
                self._extracted_message = validation_dict["first_error"]
            if "errors" in validation_dict and validation_dict["errors"]:
                self._extracted_errors = validation_dict["errors"]
        else:
            # Regular dict of errors
            self._extracted_errors = validation_dict
            self._extract_first_error_from_dict(validation_dict)

    def _analyze_validation_object(self) -> None:
        """Analyze ValidationErrors object with methods.

        Both branches below talk to a third-party ValidationErrors
        contract that we don't fully own (the rule-engine plugs in
        custom error classes). Failures inside ``errors()`` /
        ``first_error()`` should never crash the request — the worst
        case is that ``self._extracted_message`` stays unset and the
        global handler falls back to the generic 422 envelope. But
        the failures ARE bugs in the rule's error class (raising
        instead of returning), so we surface them at ``debug`` level
        rather than the previous bare ``pass`` that masked them
        completely. The HTTP path is unaffected; only ops + the
        rule's author see the breadcrumb.
        """
        # Lazy import — exceptions module loads early in bootstrap, the
        # Log facade may not be wired yet on the very first call. The
        # import is cheap (already cached by the loader for every
        # subsequent call after framework warm-up).
        try:
            from cara.facades import Log
        except Exception:
            Log = None  # type: ignore[assignment]

        # Try errors() method to get all errors
        if hasattr(self.validation_errors, "errors"):
            try:
                all_errors = self.validation_errors.errors()
                if isinstance(all_errors, dict) and all_errors:
                    self._extracted_errors = all_errors
                    self._extract_first_error_from_dict(all_errors)
            except Exception as exc:
                if Log is not None:
                    try:
                        Log.debug("ValidationException: errors() method on %s raised %s: %s — falling back to generic envelope", type(self.validation_errors).__name__, type(exc).__name__, exc, category='validation')
                    except (OSError, RuntimeError, AttributeError, ConnectionError):
                        pass

        # Try first_error() method for main message
        if hasattr(self.validation_errors, "first_error"):
            try:
                first_error = self.validation_errors.first_error()
                if first_error:
                    self._extracted_message = first_error
            except Exception as exc:
                if Log is not None:
                    try:
                        Log.debug("ValidationException: first_error() method on %s raised %s: %s — falling back to generic envelope", type(self.validation_errors).__name__, type(exc).__name__, exc, category='validation')
                    except (OSError, RuntimeError, AttributeError, ConnectionError):
                        pass

    def _extract_first_error_from_dict(self, errors_dict: dict[str, Any]) -> None:
        """Extract first error message from errors dictionary."""
        for field_name, field_errors in errors_dict.items():
            if field_errors:
                if isinstance(field_errors, list) and field_errors:
                    self._extracted_message = field_errors[0]
                elif isinstance(field_errors, str):
                    self._extracted_message = field_errors
                break

    def get_all_errors(self) -> dict[str, list[str]]:
        """Get all validation errors in normalized format."""
        return self.errors

    def get_first_error(self) -> str:
        """Get the first validation error message."""
        return str(self)

    def get_errors_for_field(self, field: str) -> list[str]:
        """Get all errors for a specific field."""
        return self.errors.get(field, [])

    def has_errors_for_field(self, field: str) -> bool:
        """Check if there are errors for a specific field."""
        return field in self.errors and bool(self.errors[field])

    def get_error_count(self) -> int:
        """Get total number of validation errors."""
        return sum(len(errors) for errors in self.errors.values())

    def get_failed_fields(self) -> list[str]:
        """Get list of field names that failed validation."""
        return [field for field, errors in self.errors.items() if errors]

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for JSON response with all errors."""
        response = {
            "error": self.get_first_error(),
            "type": "validation_error",
        }

        # Add all errors if available
        if self.errors:
            response["errors"] = self.get_all_errors()
            response["meta"] = {
                "total_errors": self.get_error_count(),
                "failed_fields": self.get_failed_fields(),
            }

        return response


class RuleNotFoundException(CaraException):
    """Thrown if a named rule does not exist in the rules map."""

    pass


class InvalidRuleFormatException(CaraException):
    """Thrown if the rules dict is not in the expected format (e.g., not a dict of
    field→rule_string)."""

    pass
