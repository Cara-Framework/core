"""
Middleware Registry for Cara Framework

Fluent middleware registration helper for clean, organized middleware configuration.
Users can easily register global middleware, groups, and aliases.
"""

from typing import Any, Dict, List, Type, Union

from .Middleware import Middleware


class MiddlewareRegistry:
    """
    Fluent middleware registration helper for Cara framework.

    Provides a clean API for organizing middleware with separation of concerns.

    Example usage:
        registry = MiddlewareRegistry()
        registry.global_middleware(CorsMiddleware, LoggingMiddleware)
        registry.group("auth", ShouldAuthenticate)
        registry.group("admin", ShouldAuthenticate, CanPerform)
        registry.alias("cors", CorsMiddleware)
        config = registry.build()
    """

    def __init__(self):
        self.config = {
            "global": [],
            "groups": {},
            "aliases": {},
        }

    def global_middleware(self, *middlewares: Type[Middleware]) -> "MiddlewareRegistry":
        """
        Register global middleware (applied to all requests).

        Args:
            *middlewares: Middleware classes to register globally

        Returns:
            Self for method chaining

        Example:
            registry.global_middleware(CorsMiddleware, LoggingMiddleware)
        """
        self.config["global"].extend(middlewares)
        return self

    def group(self, name: str, *middlewares: Type[Middleware]) -> "MiddlewareRegistry":
        """
        Register middleware group for easy route usage.

        Args:
            name: Group name (e.g., 'protected', 'admin')
            *middlewares: Middleware classes for this group

        Returns:
            Self for method chaining

        Example:
            registry.group("protected", ShouldAuthenticate)
            registry.group("admin", ShouldAuthenticate, CanPerform)
        """
        if name not in self.config["groups"]:
            self.config["groups"][name] = []
        self.config["groups"][name].extend(middlewares)
        return self

    def alias(self, name: str, middleware: Type[Middleware]) -> "MiddlewareRegistry":
        """
        Register middleware alias for convenient route usage.

        Args:
            name: Alias name (e.g., 'auth', 'can', 'cors')
            middleware: Middleware class

        Returns:
            Self for method chaining

        Example:
            registry.alias("auth", ShouldAuthenticate)
            registry.alias("can", CanPerform)
        """
        self.config["aliases"][name] = middleware
        return self

    def extend_group(
        self, name: str, *middlewares: Type[Middleware]
    ) -> "MiddlewareRegistry":
        """
        Add more middleware to an existing group.

        Args:
            name: Existing group name
            *middlewares: Additional middleware classes

        Returns:
            Self for method chaining
        """
        return self.group(name, *middlewares)

    def remove_from_group(
        self, group_name: str, middleware: Type[Middleware]
    ) -> "MiddlewareRegistry":
        """
        Remove specific middleware from a group.

        Args:
            group_name: Name of the group
            middleware: Middleware class to remove

        Returns:
            Self for method chaining
        """
        if group_name in self.config["groups"]:
            try:
                self.config["groups"][group_name].remove(middleware)
            except ValueError:
                pass  # Middleware not in group
        return self

    def remove_alias(self, alias_name: str) -> "MiddlewareRegistry":
        """
        Remove a middleware alias.

        Args:
            alias_name: Alias to remove

        Returns:
            Self for method chaining
        """
        self.config["aliases"].pop(alias_name, None)
        return self

    def clear_group(self, group_name: str) -> "MiddlewareRegistry":
        """
        Clear all middleware from a group.

        Args:
            group_name: Group to clear

        Returns:
            Self for method chaining
        """
        if group_name in self.config["groups"]:
            self.config["groups"][group_name] = []
        return self

    def has_group(self, group_name: str) -> bool:
        """
        Check if a group exists.

        Args:
            group_name: Group name to check

        Returns:
            True if group exists, False otherwise
        """
        return group_name in self.config["groups"]

    def has_alias(self, alias_name: str) -> bool:
        """
        Check if an alias exists.

        Args:
            alias_name: Alias name to check

        Returns:
            True if alias exists, False otherwise
        """
        return alias_name in self.config["aliases"]

    def get_group_middleware(self, group_name: str) -> List[Type[Middleware]]:
        """
        Get middleware for a specific group.

        Args:
            group_name: Group name

        Returns:
            List of middleware classes in the group
        """
        return self.config["groups"].get(group_name, []).copy()

    def get_alias_middleware(self, alias_name: str) -> Union[Type[Middleware], None]:
        """
        Get middleware for a specific alias.

        Args:
            alias_name: Alias name

        Returns:
            Middleware class or None if not found
        """
        return self.config["aliases"].get(alias_name)

    def merge(self, other: "MiddlewareRegistry") -> "MiddlewareRegistry":
        """
        Merge another registry into this one.

        Args:
            other: Another MiddlewareRegistry to merge

        Returns:
            Self for method chaining
        """
        other_config = other.build()

        # Merge global middleware
        self.config["global"].extend(other_config.get("global", []))

        # Merge groups
        for group_name, middlewares in other_config.get("groups", {}).items():
            if group_name in self.config["groups"]:
                self.config["groups"][group_name].extend(middlewares)
            else:
                self.config["groups"][group_name] = middlewares.copy()

        # Merge aliases (other takes precedence)
        self.config["aliases"].update(other_config.get("aliases", {}))

        return self

    def validate(self) -> List[str]:
        """
        Validate the configuration and return any errors.

        Returns:
            List of error messages (empty if valid)
        """
        errors = []

        # Check for duplicate middleware in global
        global_set = set()
        for mw in self.config["global"]:
            if mw in global_set:
                errors.append(f"Duplicate global middleware: {mw.__name__}")
            global_set.add(mw)

        # Check group validity
        for group_name, middlewares in self.config["groups"].items():
            if not group_name:
                errors.append("Empty group name found")
            if not middlewares:
                errors.append(f"Empty middleware group: {group_name}")

        # Check alias validity
        for alias_name, middleware in self.config["aliases"].items():
            if not alias_name:
                errors.append("Empty alias name found")
            if not middleware:
                errors.append(f"Empty middleware for alias: {alias_name}")

        return errors

    def build(self) -> Dict[str, Any]:
        """
        Build final configuration dictionary.

        Returns:
            Configuration dictionary for the framework

        Raises:
            ValueError: If configuration is invalid
        """
        errors = self.validate()
        if errors:
            raise ValueError(f"Invalid middleware configuration: {', '.join(errors)}")

        return {
            "global": self.config["global"].copy(),
            "groups": {name: mws.copy() for name, mws in self.config["groups"].items()},
            "aliases": self.config["aliases"].copy(),
        }

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"MiddlewareRegistry("
            f"global={len(self.config['global'])}, "
            f"groups={len(self.config['groups'])}, "
            f"aliases={len(self.config['aliases'])})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"MiddlewareRegistry(\n"
            f"  global={[mw.__name__ for mw in self.config['global']]},\n"
            f"  groups={list(self.config['groups'].keys())},\n"
            f"  aliases={list(self.config['aliases'].keys())}\n"
            f")"
        )
