"""Authorization Gate — centralized authorization for the Cara framework.

Laravel-style API: define abilities, register model policies, then check with
``allows`` / ``denies`` / ``authorize`` / ``inspect`` / ``any`` / ``none``.

Design
------
* **One resolution path.** Every public check funnels through ``_resolve`` so
  before/after callbacks, guest handling, response normalization and logging
  behave identically regardless of how the check was invoked.
* **Rich responses.** Abilities and policy methods may return a ``bool`` *or* an
  :class:`AuthorizationResponse`, so a policy can attach a denial message that
  surfaces through ``inspect()`` and the exception raised by ``authorize()``.
* **Fails closed, fails loud.** Any exception while *evaluating* a check is
  logged and treated as a denial (safe). Misconfiguration while *registering* a
  policy raises immediately at boot (loud) — a silently-dropped policy is a
  security hole.
* **Per-request scoping.** ``for_user(user)`` returns a lightweight view that
  shares the root gate's registries (registration happens once at boot) and
  only overrides the resolved user. Cheap enough to call per request.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cara.authorization.AuthorizationResponse import AuthorizationResponse
from cara.authorization.contracts import Gate as GateContract
from cara.exceptions import AuthorizationFailedException

# Sentinel separating "no decision" (None) from an explicit deny when a
# before/after callback or policy hook returns a value.
_UNAUTHENTICATED = "Unauthenticated."


class Gate(GateContract):
    """Authorization gate with a Laravel-style fluent API."""

    def __init__(self, user_resolver: Callable[[], Any] | None = None):
        self._user_resolver = user_resolver
        self._abilities: dict[str, Callable | str] = {}
        # value is a policy class or a "module.Path" import string.
        self._policies: dict[str, Any] = {}
        self._policy_cache: dict[Any, Any] = {}
        self._before_callbacks: list[Callable] = []
        self._after_callbacks: list[Callable] = []
        self._current_user: Any = None

    # -- configuration ----------------------------------------------------- #

    def before(self, callback: Callable) -> Gate:
        """Register a callback run before every check. Return ``True``/``False``
        to short-circuit, or ``None`` to defer to the ability/policy."""
        self._before_callbacks.append(callback)
        return self

    def after(self, callback: Callable) -> Gate:
        """Register a callback run after every check. Return ``True``/``False``
        to override the result, or ``None`` to keep it."""
        self._after_callbacks.append(callback)
        return self

    def define(self, ability: str, callback: Callable | str) -> Gate:
        """Define a standalone ability.

        ``callback`` is either a callable ``(user, *args) -> bool | Response`` or
        a ``"module.path.PolicyClass@method"`` string.
        """
        self._abilities[ability] = callback
        return self

    def policy(self, model_class: Any, policy_class: Any) -> Gate:
        """Bind one policy to a model class.

        ``policy_class`` may be the policy class itself (preferred — instantiated
        directly, no re-import) or a ``"module.Path"`` import string (resolved
        lazily, handy for avoiding circular imports at registration time).
        """
        model_name = self._model_name(model_class)
        if not model_name:
            raise TypeError(f"Cannot derive a model name from {model_class!r}")
        if not isinstance(policy_class, (str, type)):
            raise TypeError(
                "policy_class must be a class or an import-path string, got "
                f"{policy_class!r}"
            )
        self._policies[model_name] = policy_class
        return self

    def register_policies(self, policies: list[tuple]) -> Gate:
        """Bind multiple ``(Model, Policy)`` tuples. Raises on malformed input."""
        for entry in policies:
            try:
                model_class, policy_class = entry
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    "register_policies expects (Model, Policy) tuples, got: "
                    f"{entry!r}"
                ) from exc
            self.policy(model_class, policy_class)
        return self

    def has(self, ability: str) -> bool:
        """Whether a standalone ability has been defined."""
        return ability in self._abilities

    # -- checks ------------------------------------------------------------ #

    def allows(self, ability: str, *args: Any) -> bool:
        """``True`` if the current user may perform ``ability``."""
        return self._resolve(self._resolve_user(), ability, *args).allowed()

    def denies(self, ability: str, *args: Any) -> bool:
        """``True`` if the current user may *not* perform ``ability``."""
        return not self.allows(ability, *args)

    def check(self, ability: str, *args: Any) -> bool:
        """Alias of :meth:`allows`."""
        return self.allows(ability, *args)

    def any(self, abilities: list[str], *args: Any) -> bool:
        """``True`` if the user has *any* of ``abilities``."""
        user = self._resolve_user()
        return any(self._resolve(user, a, *args).allowed() for a in abilities)

    def none(self, abilities: list[str], *args: Any) -> bool:
        """``True`` if the user has *none* of ``abilities``."""
        return not self.any(abilities, *args)

    def inspect(self, ability: str, *args: Any) -> AuthorizationResponse:
        """Return the full :class:`AuthorizationResponse` (with message)."""
        return self._resolve(self._resolve_user(), ability, *args)

    def authorize(self, ability: str, *args: Any) -> AuthorizationResponse:
        """Authorize or raise :class:`AuthorizationFailedException`."""
        user = self._resolve_user()
        response = self._resolve(user, ability, *args)
        if response.denied():
            raise AuthorizationFailedException(
                message=response.message()
                or f"This action is unauthorized. Missing ability: {ability}",
                ability=ability,
                user=user,
                resource=args[0] if args else None,
            )
        return response

    def for_user(self, user: Any) -> Gate:
        """Return a user-scoped view sharing this gate's registries."""
        scoped = Gate(self._user_resolver)
        # Share by reference: registration only happens on the root (singleton)
        # gate at boot, so scoped per-request gates stay read-only and cheap.
        scoped._abilities = self._abilities
        scoped._policies = self._policies
        scoped._policy_cache = self._policy_cache
        scoped._before_callbacks = self._before_callbacks
        scoped._after_callbacks = self._after_callbacks
        scoped._current_user = user
        return scoped

    # -- resolution -------------------------------------------------------- #

    def _resolve(self, user: Any, ability: str, *args: Any) -> AuthorizationResponse:
        """The single resolution path shared by every public check."""
        # 1) before callbacks may short-circuit (e.g. a root-user bypass).
        for callback in self._before_callbacks:
            try:
                decision = callback(user, ability, *args)
            except Exception as exc:  # noqa: BLE001 — log and skip a bad guard
                self._log(f"before-callback failed for ability='{ability}': {exc}")
                continue
            if decision is not None:
                return self._run_after(user, ability, self._normalize(decision), *args)

        # 2) Guests are denied by default unless a before-callback allowed them.
        if user is None:
            response = AuthorizationResponse(False, _UNAUTHENTICATED)
        else:
            response = self._evaluate(user, ability, *args)

        # 3) after callbacks may override the result.
        return self._run_after(user, ability, response, *args)

    def _evaluate(self, user: Any, ability: str, *args: Any) -> AuthorizationResponse:
        """Resolve a non-guest check against a defined ability or model policy."""
        if ability in self._abilities:
            return self._call_ability(self._abilities[ability], user, ability, *args)

        model_name = self._model_name(args[0]) if args else None
        if model_name and model_name in self._policies:
            return self._call_policy(self._policies[model_name], ability, user, *args)

        return AuthorizationResponse(
            False, f"No ability or policy is registered for '{ability}'."
        )

    def _run_after(
        self, user: Any, ability: str, response: AuthorizationResponse, *args: Any
    ) -> AuthorizationResponse:
        for callback in self._after_callbacks:
            try:
                override = callback(user, ability, response.allowed(), *args)
            except Exception as exc:  # noqa: BLE001 — log and skip a bad guard
                self._log(f"after-callback failed for ability='{ability}': {exc}")
                continue
            if override is not None:
                response = self._normalize(override)
        return response

    def _call_ability(
        self, callback: Callable | str, user: Any, ability: str, *args: Any
    ) -> AuthorizationResponse:
        try:
            if isinstance(callback, str):
                class_path, _, method = callback.partition("@")
                return self._call_policy(class_path, method or ability, user, *args)
            return self._normalize(callback(user, *args))
        except Exception as exc:  # noqa: BLE001 — fail closed
            self._log(f"ability '{ability}' evaluation failed: {exc}")
            return AuthorizationResponse(False, "Authorization check failed.")

    def _call_policy(
        self, policy_ref: Any, method: str, user: Any, *args: Any
    ) -> AuthorizationResponse:
        try:
            policy = self._instantiate_policy(policy_ref)
        except Exception as exc:  # noqa: BLE001 — fail closed
            self._log(f"policy {policy_ref!r} could not be instantiated: {exc}")
            return AuthorizationResponse(False, "Authorization check failed.")

        # policy.before hook may short-circuit.
        before = getattr(policy, "before", None)
        if callable(before):
            pre = self._safe_hook(before, policy, "before", user, method, *args)
            if pre is not None:
                return self._normalize(pre)

        handler = getattr(policy, method, None)
        if not callable(handler):
            return AuthorizationResponse(
                False, f"{type(policy).__name__} has no '{method}' ability."
            )

        try:
            result = handler(user, *args)
        except Exception as exc:  # noqa: BLE001 — fail closed
            self._log(f"policy '{method}' on {type(policy).__name__} raised: {exc}")
            return AuthorizationResponse(False, "Authorization check failed.")

        response = self._normalize(result)

        # policy.after hook may override.
        after = getattr(policy, "after", None)
        if callable(after):
            post = self._safe_hook(
                after, policy, "after", user, method, response.allowed(), *args
            )
            if post is not None:
                response = self._normalize(post)
        return response

    def _safe_hook(
        self, hook: Callable, policy: Any, kind: str, *hook_args: Any
    ) -> Any:
        try:
            return hook(*hook_args)
        except Exception as exc:  # noqa: BLE001 — a bad hook must not crash a check
            self._log(
                f"policy {kind}-hook on {type(policy).__name__} raised: {exc}"
            )
            return None

    def _instantiate_policy(self, policy_ref: Any) -> Any:
        cached = self._policy_cache.get(policy_ref)
        if cached is not None:
            return cached

        if isinstance(policy_ref, str):
            if "." in policy_ref:
                module_path, class_name = policy_ref.rsplit(".", 1)
            else:
                module_path, class_name = "app.policies", policy_ref
            module = __import__(module_path, fromlist=[class_name])
            policy_cls = getattr(module, class_name)
        else:
            policy_cls = policy_ref  # already a class

        instance = policy_cls()
        self._policy_cache[policy_ref] = instance
        return instance

    # -- helpers ----------------------------------------------------------- #

    @staticmethod
    def _normalize(result: Any) -> AuthorizationResponse:
        if isinstance(result, AuthorizationResponse):
            return result
        return AuthorizationResponse(bool(result))

    @staticmethod
    def _model_name(obj: Any) -> str | None:
        if obj is None:
            return None
        if isinstance(obj, type):
            return obj.__name__.lower()
        cls = getattr(obj, "__class__", None)
        return cls.__name__.lower() if cls is not None else None

    def _resolve_user(self) -> Any | None:
        if self._current_user is not None:
            return self._current_user
        if self._user_resolver:
            try:
                return self._user_resolver()
            except Exception as exc:  # noqa: BLE001 — never let resolution crash a check
                self._log(f"user resolver failed: {exc}")
        return None

    @staticmethod
    def _log(message: str) -> None:
        try:
            from cara.facades import Log

            Log.error(message, category="cara.authorization", exc_info=True)
        except Exception:  # noqa: BLE001 — logging must never raise
            from cara.facades import Log

            Log.error(message, category="cara.authorization", exc_info=True)
