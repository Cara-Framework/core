"""Laravel-style FormRequest for Cara Framework.

Encapsulates validation, authorization, and custom validation hooks for a single endpoint.
"""

from __future__ import annotations

import contextlib
from typing import Any

from cara.configuration import config
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

        sent = set(data) if isinstance(data, dict) else set()
        self._refuse_path_overrides(request, data)

        # Laravel parity: normalize the raw payload BEFORE validation so the
        # mutated shape is what the rules see (and what ``validated()`` returns).
        data = self.prepare_for_validation(data)

        rules = self.rules()
        self._refuse_undeclared_keys(request, sent, data, rules)

        validator = Validation.make(data, rules, self.messages())
        validator.after(self.after)

        if validator.fails():
            raise ValidationException(validation_errors=validator.errors())

        validated = validator.validated()

        with contextlib.suppress(AttributeError):
            request.validated = validated

        return validated

    #: Per-request override of the product setting: ``True``/``False`` pins
    #: this door, ``None`` follows ``server.refuse_unknown_request_keys``.
    refuse_unknown_keys: bool | None = None

    @staticmethod
    def _refuse_path_overrides(request: Any, data: Any) -> None:
        """Refuse a body or query key that names a route path parameter with
        another value. The path value is the resource the route addresses
        (``validate_path_param`` puts it in the input), but the body wins the
        merge (``all()``): ``DELETE /notifications/NTF_A`` with a body naming
        ``NTF_B`` acted on B."""
        if not isinstance(data, dict):
            return
        params = getattr(request, "params", None) or {}
        clashing = sorted(
            str(name)
            for name, value in params.items()
            if name in data and str(data[name]) != str(value)
        )
        if clashing:
            raise ValidationException(
                message=f"Parameter {clashing[0]} contradicts the request path.",
                errors={
                    name: [f"{name} contradicts the request path."] for name in clashing
                },
            )

    def _refuses_unknown_keys(self) -> bool:
        if self.refuse_unknown_keys is not None:
            return bool(self.refuse_unknown_keys)
        return bool(config("server.refuse_unknown_request_keys", False))

    def _refuse_undeclared_keys(
        self,
        request: Any,
        sent: set[str],
        data: Any,
        rules: dict[str, Any],
    ) -> None:
        """Refuse a key the client sent that ``rules()`` does not declare.

        Dropping it silently answered 200 for a request that did not do what
        its sender meant: ``GET /orders?status=unshipped`` listed refunded
        orders too, and ``PATCH /products/{id} {"price": ...}`` changed nothing
        (journey 10, 2026-11-13). The market norm is a loud 422 naming the key
        (Stripe's "Received unknown parameter"). ``rules()`` is the door's
        declaration — and the published OpenAPI is drawn from it — so a key it
        does not name is unknown by definition. Nested rules (``items.*.sku``)
        declare their top-level key. A key ``prepare_for_validation`` consumed
        (an alias it renamed or dropped) was handled; a route path parameter is
        addressing, not input.

        A product opts in (``config("server.refuse_unknown_request_keys")``);
        the default stays Laravel's (unknown keys ignored), because flipping
        it framework-wide turned another product's undeclared-but-sent keys
        (an unsubscribe link's ``user``, a list's ``limit``) into 422s.
        """
        if not isinstance(data, dict) or not self._refuses_unknown_keys():
            return
        declared = {str(key).split(".", 1)[0] for key in rules}
        if "*" in declared:
            return
        path_params = set(getattr(request, "params", None) or {})
        unknown = sorted(
            key
            for key in sent
            if key in data and key not in declared and key not in path_params
        )
        if unknown:
            message = f"Received unknown parameter: {unknown[0]}"
            if len(unknown) > 1:
                message = "Received unknown parameters: " + ", ".join(unknown)
            raise ValidationException(
                message=message,
                errors={key: [f"Received unknown parameter: {key}"] for key in unknown},
            )
