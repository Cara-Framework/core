"""
Validation Helpers Mixin for HTTP Request.

This mixin provides validation functionality and convenience methods for handling
validated data and errors.
"""

from typing import Any, Dict

from cara.validation import Validation


class ValidationHelpersMixin:
    """
    Mixin providing validation functionality for HTTP requests.

    Integrates with the Cara validation system and provides helper methods for
    working with validated data.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validation_instance = None

    async def validate(self, rules: Dict[str, str]) -> None:
        """
        Validate request data against a dict of field→rule_string.

        Populates self.validated and stores validation instance for later checking.
        """
        data = await self.all()
        validation = Validation()
        validation.make(data, rules)

        # Store validation instance for fails() and errors() methods
        self._validation_instance = validation
        self.validated = validation.validated()

    def fails(self) -> bool:
        """Return True if last validate() had errors."""
        if self._validation_instance is None:
            return False
        return self._validation_instance.fails()

    async def errors(self) -> Dict[str, Any]:
        """Return validation error messages if validation failed."""
        if self._validation_instance is None:
            return {}
        return self._validation_instance.errors()

    async def only(self, *args) -> Dict[str, Any]:
        """Return a dict containing only the specified keys from request data."""
        data = await self.all()
        # Handle Laravel-style usage: both list and variadic arguments
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            # Case: request.only(["email", "name"])
            key_list = args[0]
        else:
            # Case: request.only("email", "name") or request.only("email")
            key_list = args
        return {k: data[k] for k in key_list if k in data}

    async def except_(self, *args) -> Dict[str, Any]:
        """Return a dict excluding the specified keys from request data."""
        data = await self.all()
        # Handle Laravel-style usage: both list and variadic arguments
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            # Case: request.except_(["password", "token"])
            key_list = args[0]
        else:
            # Case: request.except_("password", "token") or request.except_("password")
            key_list = args
        return {k: v for k, v in data.items() if k not in key_list}

    async def has(self, key: str) -> bool:
        """Return True if the given key is present in any input source."""
        data = await self.all()
        return key in data

    async def filled(self, key: str) -> bool:
        """Return True if the given key exists and is not an empty string."""
        data = await self.all()
        val = data.get(key)
        return val is not None and val != ""
