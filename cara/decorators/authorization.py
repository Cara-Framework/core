"""
🛡️ Authorization decorators inspired by Laravel and Masonite.

Usage Examples:
    @can("create-posts")
    @can_any(["admin", "moderator"])
    @admin_only
    @authenticated
    @policy("Post", "update")
"""

from functools import wraps
from typing import Callable, List

from cara.exceptions import AuthorizationFailedException
from cara.facades import Auth, Gate


def can(ability: str, *args) -> Callable:
    """
    ✅ Check single permission - Laravel/Masonite style

    @can("create-posts")
    @can("edit-settings")
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*func_args, **func_kwargs):
            # Get request from first argument (controller method)
            request = func_args[1] if len(func_args) > 1 else func_args[0]

            try:
                # Build parameters for gate check
                parameters = list(args)
                if hasattr(request, "route_params"):
                    for key, value in request.route_params.items():
                        if key.endswith("_id") or key == "id":
                            parameters.append(value)

                # Check authorization
                if not Gate.allows(ability, *parameters):
                    raise AuthorizationFailedException(
                        message=f"Permission denied: {ability}",
                        ability=ability,
                        status_code=403,
                    )

            except Exception as e:
                if "AuthorizationFailedException" in str(type(e)):
                    raise e
                # Fallback for any other errors
                raise AuthorizationFailedException(
                    message=f"Authorization check failed: {ability}",
                    ability=ability,
                    status_code=403,
                )

            return func(*func_args, **func_kwargs)

        return wrapper

    return decorator


def can_any(abilities: List[str], *args) -> Callable:
    """
    🎯 Check multiple permissions (OR logic) - Laravel style

    @can_any(["admin", "moderator"])
    @can_any(["create-posts", "edit-posts"])
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*func_args, **func_kwargs):
            request = func_args[1] if len(func_args) > 1 else func_args[0]

            try:
                parameters = list(args)
                if hasattr(request, "route_params"):
                    for key, value in request.route_params.items():
                        if key.endswith("_id") or key == "id":
                            parameters.append(value)

                if not Gate.any(abilities, *parameters):
                    abilities_str = ", ".join(abilities)
                    raise AuthorizationFailedException(
                        message=f"Missing any of permissions: {abilities_str}",
                        ability=abilities_str,
                        status_code=403,
                    )

            except Exception as e:
                if "AuthorizationFailedException" in str(type(e)):
                    raise e
                raise AuthorizationFailedException(
                    message="Authorization check failed",
                    ability=str(abilities),
                    status_code=403,
                )

            return func(*func_args, **func_kwargs)

        return wrapper

    return decorator


def authorize(ability: str, *args) -> Callable:
    """🔐 Laravel-style authorize alias"""
    return can(ability, *args)


def policy(model_class: str, action: str) -> Callable:
    """
    📋 Check model policy - Laravel/Masonite style

    @policy("Post", "update")
    @policy("User", "create")
    """
    ability = f"{model_class}.{action}".lower()
    return can(ability)


def admin_only(func: Callable) -> Callable:
    """🔐 Only admins can access - Shorthand"""
    return can("admin")(func)


def authenticated_only(func: Callable) -> Callable:
    """🔑 Must be logged in - Laravel style"""

    @wraps(func)
    def wrapper(*func_args, **func_kwargs):
        request = func_args[1] if len(func_args) > 1 else func_args[0]

        try:
            user = Auth.user()

            if user is None:
                raise AuthorizationFailedException(
                    message="Authentication required",
                    status_code=401,
                )

        except Exception as e:
            if "AuthorizationFailedException" in str(type(e)):
                raise e
            raise AuthorizationFailedException(
                message="Authentication required",
                status_code=401,
            )

        return func(*func_args, **func_kwargs)

    return wrapper


def guest_only(func: Callable) -> Callable:
    """🚪 Only guests (not logged in) can access - Laravel style"""

    @wraps(func)
    def wrapper(*func_args, **func_kwargs):
        request = func_args[1] if len(func_args) > 1 else func_args[0]

        try:
            user = Auth.user()

            if user is not None:
                raise AuthorizationFailedException(
                    message="This action is only available to guests",
                    status_code=403,
                )

        except Exception as e:
            if "AuthorizationFailedException" in str(type(e)):
                raise e
            # If auth fails, user is probably guest anyway
            pass

        return func(*func_args, **func_kwargs)

    return wrapper


# 🎯 Common shortcuts - Laravel/Masonite inspired
authenticated = authenticated_only
admin = admin_only
