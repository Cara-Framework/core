"""
ScopeProxy - Simple scope proxy for model scopes
"""


class ScopeProxy:
    """Simple proxy for model scopes."""

    def __init__(self, model_class, scope_name):
        self.model_class = model_class
        self.scope_name = scope_name

    def __call__(self, *args, **kwargs):
        """Call the scope method."""
        # Get a fresh query builder
        query = self.model_class.query()

        # Apply the scope
        scope_method = getattr(self.model_class, f"scope_{self.scope_name}", None)
        if scope_method:
            return scope_method(query, *args, **kwargs)

        return query

    def __getattr__(self, name):
        """Forward attribute access to a new query."""
        query = self()
        return getattr(query, name)


def scope(func):
    """Decorator to register a model scope."""

    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)

    # Mark this as a scope method
    wrapper._is_scope = True
    wrapper._scope_name = func.__name__.replace("scope_", "")
    return wrapper


def enhance_model_with_scopes(model_class):
    """Enhance a model class with scope functionality."""
    # This is a placeholder for scope enhancement
    # In practice, you might register scopes here
    if not hasattr(model_class, "_scopes"):
        model_class._scopes = {}

    # Scan for scope methods and register them
    for attr_name in dir(model_class):
        attr = getattr(model_class, attr_name)
        if hasattr(attr, "_is_scope"):
            scope_name = attr._scope_name
            model_class._scopes[scope_name] = attr

    return model_class
