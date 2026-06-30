"""Laravel-style FormRequest for Cara Framework.

Encapsulates validation, authorization, and custom validation hooks for a single endpoint.
"""

from __future__ import annotations

from typing import Any

from cara.exceptions import AuthorizationFailedException, ValidationException
from cara.validation import Validation


class FormRequest:
    """Laravel-style form request — encapsulates validation + authorization for one endpoint.

    Subclasses override rules(), messages(), authorize(), and after() to customize behavior.
    """

    def rules(self) -> dict[str, str]:
        """Override — return {field: rule_string, ...}."""
        return {}

    def messages(self) -> dict[str, str]:
        """Override — return custom error messages {field.rule: message}."""
        return {}

    def authorize(self, request: Any) -> bool:
        """Override — return True if user allowed to make this request."""
        return True

    def after(self, validator: Any) -> None:
        """Override — run after built-in rules. Add errors via validator.errors().add(...)."""
        pass

    def prepare_for_validation(self, data: dict[str, Any]) -> dict[str, Any]:
        """Override — normalize the RAW input dict BEFORE the rules run
        (Laravel's ``prepareForValidation``). The returned dict is what
        ``rules()`` validates and what ``validated()`` is drawn from, so any
        mutation here is itself re-validated — unlike normalizing by overriding
        ``validate_request()`` and editing the result, which silently skips
        re-validation of the mutated shape. Use for input shaping: aliasing
        (``per_page`` → ``limit``), bool/number → string coercion, trimming,
        nested-blob flattening. Default: pass the data through unchanged (zero
        behaviour change for requests that don't override)."""
        return data

    async def validate_request(self, request: Any) -> dict[str, Any]:
        """Main entry point. Returns validated dict.

        Raises:
            AuthorizationFailedException: When authorize() returns False.
            ValidationException: When validation rules fail.
        """
        if not self.authorize(request):
            raise AuthorizationFailedException("This action is unauthorized.")

        data = (
            await request.all()
            if hasattr(request, "all") and callable(request.all)
            else {}
        )

        # Laravel parity: normalize the raw payload BEFORE validation so the
        # mutated shape is what the rules see (and what ``validated()`` returns).
        data = self.prepare_for_validation(data)

        validator = Validation.make(data, self.rules(), self.messages())
        validator.after(self.after)

        if validator.fails():
            raise ValidationException(validation_errors=validator.errors())

        validated = validator.validated()

        try:
            request.validated = validated
        except AttributeError:
            pass

        return validated
