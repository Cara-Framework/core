"""
Accessor Decorator for Laravel-style attribute modification.

Allows defining custom getter methods for model attributes that transform
data when accessing model properties.

Example:
    class User(Model):
        @accessor('first_name')
        def get_first_name(self, value):
            return value.capitalize() if value else None

        @accessor('full_name')  # Virtual attribute
        def get_full_name(self, value):
            return f"{self.first_name} {self.last_name}"

    user = User()
    user.first_name = "john"
    print(user.first_name)  # "John" (capitalized by accessor)
    print(user.full_name)   # "John Doe" (virtual attribute)
"""


def accessor(attribute_name: str):
    """
    Decorator to define an accessor method for a model attribute.

    An accessor transforms the attribute value when it's accessed/retrieved.

    Args:
        attribute_name: The name of the attribute this accessor handles

    Returns:
        Decorated function that will be called when the attribute is accessed

    Example:
        @accessor('email')
        def get_email(self, value):
            return value.lower() if value else None
    """

    def decorator(func):
        # Store accessor metadata on the function
        func._is_accessor = True
        func._accessor_attribute = attribute_name
        func._accessor_method = func.__name__

        # Store the original function for potential debugging
        func._original_func = func

        return func

    return decorator
