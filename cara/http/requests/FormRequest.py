"""
Laravel-style FormRequest for Cara Framework.

Encapsulates validation, authorization, and custom validation hooks for a single endpoint.
"""


class FormRequest:
    """
    Laravel-style form request — encapsulates validation + authorization for one endpoint.

    Subclasses override rules(), messages(), authorize(), and after() to customize behavior.
    """

    def rules(self) -> dict:
        """
        Override — return {field: rule_string, ...}.

        Example:
            return {
                "email": "required|email|unique:users,email",
                "password": "required|min:8|confirmed",
            }
        """
        return {}

    def messages(self) -> dict:
        """
        Override — return custom error messages {field.rule: message}.

        Example:
            return {
                "email.unique": "This email is already registered.",
                "password.min": "Password must be at least 8 characters.",
            }
        """
        return {}

    def authorize(self, request) -> bool:
        """
        Override — return True if user allowed to make this request.

        Example:
            return bool(request.user() and request.user().is_admin)
        """
        return True

    def after(self, validator) -> None:
        """
        Override — run after built-in rules. Add errors via validator.errors().add(...).

        Example:
            def after(self, validator):
                if validator.validated().get("password") != validator.validated().get("password_confirm"):
                    validator.errors().add("password", "Passwords do not match.")
        """
        pass

    async def validate_request(self, request):
        """
        Main entry point. Returns validated dict. Raises AuthorizationFailedException or ValidationException.

        Usage:
            validated = await RegisterUserRequest().validate_request(request)
        """
        # Check authorization first
        if not self.authorize(request):
            from cara.exceptions import AuthorizationFailedException
            raise AuthorizationFailedException("This action is unauthorized.")

        # Get request data
        data = await request.all() if hasattr(request, "all") and callable(request.all) else {}

        # Create validator
        from cara.validation import Validation
        validator = Validation.make(data, self.rules(), self.messages())

        # Register after hook
        validator.after(self.after)

        # Run validation
        if validator.fails():
            from cara.exceptions import ValidationException
            raise ValidationException(validation_errors=validator.errors())

        # Extract validated data
        validated = validator.validated()

        # Expose on request for downstream use
        try:
            request.validated = validated
        except Exception:
            pass

        return validated
