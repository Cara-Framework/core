"""
Authorization Gate - Centralized authorization logic for Cara framework.
"""

from typing import Any, Callable, Dict, List, Optional, Union

from cara.authorization.AuthorizationResponse import AuthorizationResponse
from cara.authorization.contracts import Gate
from cara.exceptions import AuthorizationFailedException


class Gate(Gate):
    """Gate for authorization checks with Laravel-style API."""

    def __init__(self, user_resolver: Optional[Callable] = None):
        self._user_resolver = user_resolver
        self._abilities: Dict[str, Union[Callable, str]] = {}
        self._policies: Dict[str, str] = {}
        self._before_callbacks: List[Callable] = []
        self._after_callbacks: List[Callable] = []
        self._current_user: Optional[Any] = None

    def before(self, callback: Callable) -> None:
        """Register a callback to be called before authorization checks."""
        self._before_callbacks.append(callback)

    def after(self, callback: Callable) -> None:
        """Register a callback to be called after authorization checks."""
        self._after_callbacks.append(callback)

    def define(self, ability: str, callback: Union[Callable, str]) -> None:
        """Define a new ability."""
        self._abilities[ability] = callback

    def register_policies(self, policies: List[tuple]) -> None:
        """
        Register multiple policies with model binding (Masonite style).

        Args:
            policies: List of (Model, PolicyClass) tuples
        """
        for model_class, policy_class in policies:
            try:
                # Get model name from class
                model_name = (
                    model_class.__name__.lower()
                    if hasattr(model_class, "__name__")
                    else str(model_class).lower()
                )

                # Get policy class path
                if hasattr(policy_class, "__module__") and hasattr(
                    policy_class, "__name__"
                ):
                    policy_path = f"{policy_class.__module__}.{policy_class.__name__}"
                else:
                    policy_path = str(policy_class)

                # Register the policy
                self._policies[model_name] = policy_path

            except Exception:
                continue

    def allows(self, ability: str, *args) -> bool:
        """Check if the current user is authorized for the given ability."""
        user = self._resolve_user()

        # Run before callbacks
        for callback in self._before_callbacks:
            try:
                result = callback(user, ability, *args)
                if result is True:
                    self._run_after_callbacks(user, ability, True, *args)
                    return True
                elif result is False:
                    self._run_after_callbacks(user, ability, False, *args)
                    return False
                # Continue if result is None
            except Exception:
                continue

        # Guest users are not authorized by default
        if user is None:
            result = False
        else:
            # Check if it's a model-based ability
            result = self._check_model_ability(ability, user, *args)

        # Run after callbacks
        self._run_after_callbacks(user, ability, result, *args)
        return result

    def denies(self, ability: str, *args) -> bool:
        """Check if the current user is denied the given ability."""
        return not self.allows(ability, *args)

    def any(self, abilities: List[str], *args) -> bool:
        """Check if the current user has any of the given abilities."""
        return any(self.allows(ability, *args) for ability in abilities)

    def none(self, abilities: List[str], *args) -> bool:
        """Check if the current user has none of the given abilities."""
        return not any(self.allows(ability, *args) for ability in abilities)

    def inspect(self, ability: str, *args) -> AuthorizationResponse:
        """Inspect the authorization result with detailed response."""
        user = self._resolve_user()

        # Check authorization
        allowed = self.allows(ability, *args)

        # Create detailed message
        if allowed:
            message = f"User authorized for '{ability}'"
            if user:
                message += f" (user: {getattr(user, 'id', 'unknown')})"
        else:
            message = f"User not authorized for '{ability}'"
            if user is None:
                message += " (no authenticated user)"
            else:
                message += f" (user: {getattr(user, 'id', 'unknown')})"

        return AuthorizationResponse(allowed, message)

    def authorize(self, ability: str, *args) -> None:
        """Authorize the given ability or raise an exception."""
        if not self.allows(ability, *args):
            user = self._resolve_user()
            resource = args[0] if args else None

            raise AuthorizationFailedException(
                message=f"This action is unauthorized. Missing ability: {ability}",
                ability=ability,
                user=user,
                resource=resource,
            )

    def for_user(self, user: Any) -> "Gate":
        """Get a gate instance for the given user."""
        gate = Gate(self._user_resolver)
        gate._abilities = self._abilities.copy()
        gate._policies = self._policies.copy()
        gate._before_callbacks = self._before_callbacks.copy()
        gate._after_callbacks = self._after_callbacks.copy()
        gate._current_user = user
        return gate

    def _check_model_ability(self, ability: str, user: Any, *args) -> bool:
        """Check model-based ability using policies."""
        # Direct ability check
        if ability in self._abilities:
            return self._call_ability(self._abilities[ability], user, ability, *args)

        # Model-based ability check
        if args and hasattr(args[0], "__class__"):
            # Get model name from instance
            model_name = args[0].__class__.__name__.lower()
        elif args and hasattr(args[0], "__name__"):
            # Get model name from class
            model_name = args[0].__name__.lower()
        else:
            return False

        # Find policy for model
        if model_name in self._policies:
            policy_class = self._policies[model_name]
            return self._call_policy_method(policy_class, ability, user, *args)

        return False

    def _resolve_user(self) -> Optional[Any]:
        """Resolve the current user."""
        if self._current_user is not None:
            return self._current_user

        if self._user_resolver:
            try:
                return self._user_resolver()
            except Exception:
                return None

        return None

    def _run_after_callbacks(self, user: Any, ability: str, result: bool, *args) -> None:
        """Run after callbacks."""
        for callback in self._after_callbacks:
            try:
                callback(user, ability, result, *args)
            except Exception:
                continue

    def _call_ability(
        self, callback: Union[Callable, str], user: Any, ability: str, *args
    ) -> bool:
        """Call an ability callback."""
        try:
            if callable(callback):
                return bool(callback(user, *args))
            elif isinstance(callback, str) and "@" in callback:
                class_name, method_name = callback.split("@", 1)
                return self._call_policy_method(class_name, method_name, user, *args)
            return False
        except Exception:
            return False

    def _call_policy_method(
        self, policy_class: str, method: str, user: Any, *args
    ) -> bool:
        """Call a policy method."""
        try:
            policy_instance = self._instantiate_policy(policy_class)

            if not hasattr(policy_instance, method):
                return False

            # Call before method if exists
            if hasattr(policy_instance, "before"):
                try:
                    before_result = policy_instance.before(user, method, *args)
                    if before_result is not None:
                        return bool(before_result)
                except Exception:
                    pass

            # Call the ability method
            method_func = getattr(policy_instance, method)
            result = method_func(user, *args)

            # Call after method if exists
            if hasattr(policy_instance, "after"):
                try:
                    after_result = policy_instance.after(
                        user, method, bool(result), *args
                    )
                    if after_result is not None:
                        return bool(after_result)
                except Exception:
                    pass

            return bool(result)

        except Exception:
            return False

    def _instantiate_policy(self, policy_class: str):
        """Instantiate a policy class from string."""
        try:
            if "." in policy_class:
                module_path, class_name = policy_class.rsplit(".", 1)
                module = __import__(module_path, fromlist=[class_name])
                policy_cls = getattr(module, class_name)
            else:
                try:
                    module = __import__("app.policies", fromlist=[policy_class])
                    policy_cls = getattr(module, policy_class)
                except (ImportError, AttributeError):
                    raise ImportError(f"Policy class {policy_class} not found")

            return policy_cls()

        except ImportError:
            raise
        except AttributeError:
            raise
        except Exception:
            raise
