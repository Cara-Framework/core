"""
Mutator Decorator for Laravel-style attribute modification.

Allows defining custom setter methods for model attributes that transform
data when setting model properties.

Example:
    class User(Model):
        @mutator('first_name')
        def set_first_name(self, value):
            return value.lower() if value else None

        @mutator('email')
        def set_email(self, value):
            return value.strip().lower() if value else None

    user = User()
    user.first_name = "JOHN"  # Stored as "john" (lowercased by mutator)
    user.email = "  JOHN@EXAMPLE.COM  "  # Stored as "john@example.com"
"""


def mutator(attribute_name: str):
    """
    Decorator to define a mutator method for a model attribute.

    A mutator transforms the attribute value when it's set/assigned.

    Args:
        attribute_name: The name of the attribute this mutator handles

    Returns:
        Decorated function that will be called when the attribute is set

    Example:
        @mutator('password')
        def set_password(self, value):
            return bcrypt.hash(value) if value else None
    """

    def decorator(func):
        # Store mutator metadata on the function
        func._is_mutator = True
        func._mutator_attribute = attribute_name
        func._mutator_method = func.__name__

        # Store the original function for potential debugging
        func._original_func = func

        return func

    return decorator
