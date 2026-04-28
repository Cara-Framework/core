"""
Channel authorization registry — Laravel's ``Broadcast::channel()`` equivalent.

In Laravel an app registers channel callbacks once, declaratively::

    Broadcast::channel('user.{userId}.alerts', function ($user, $userId) {
        return (int) $user->id === (int) $userId;
    });

Cara mirrors this. The registry maps channel-name patterns (with
``{var}`` placeholders) to callables that decide "may this user
subscribe?". When a Socket receives a subscribe action targeting an
auth-gated channel (``private-`` / ``presence-`` prefix), the registry
is consulted before the subscription is committed.

The registry is sync-and-async-aware: callbacks may be ``async def``
and are awaited; sync callbacks run inline. The return value is:

- ``False`` / ``None``  → subscription rejected.
- ``True``              → subscription accepted (no presence data).
- ``dict``              → subscription accepted, dict is presence data
                           (mirrors Laravel's presence channel
                           contract — the dict is broadcast to the
                           channel as the joining member's identity).
"""

from __future__ import annotations

import inspect
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

# A channel auth callback receives (user, **placeholders) and returns
# bool / dict / None / Awaitable[same].
ChannelAuthCallback = Callable[..., Union[bool, dict, None, Awaitable[Union[bool, dict, None]]]]


class ChannelRegistry:
    """Pattern-based channel auth callback registry.

    Patterns use Laravel's ``{name}`` placeholder syntax:

      ``user.{user_id}.alerts``  matches ``user.42.alerts``
      ``room.{room}``            matches ``room.lobby``

    Placeholder values are passed to the callback as keyword
    arguments matching the placeholder name. The user object (the
    socket's authenticated user) is the first positional argument.
    """

    # Compiled (pattern_str, regex, var_names, callback) tuples. List
    # rather than dict so registration order is preserved — first match
    # wins, mirroring Laravel's behaviour.
    _patterns: List[Tuple[str, re.Pattern, List[str], ChannelAuthCallback]]

    # Sentinels for parsing the {var} placeholders.
    _PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

    def __init__(self) -> None:
        self._patterns = []

    def register(self, pattern: str, callback: ChannelAuthCallback) -> None:
        """Register a channel auth callback.

        ``pattern`` is matched against the *unprefixed* channel name —
        i.e. ``"user.{id}"`` matches the wire channel
        ``"private-user.42"`` (after the ``private-`` prefix is
        stripped at the call site). This keeps the auth pattern
        language identical to Laravel: prefixes are transport-layer
        details, not channel-name details.
        """
        if not pattern or not isinstance(pattern, str):
            raise ValueError(f"Channel pattern must be a non-empty string, got {pattern!r}")
        if not callable(callback):
            raise TypeError(f"Callback must be callable, got {type(callback).__name__}")

        var_names = self._PLACEHOLDER_RE.findall(pattern)
        # Build a regex from the pattern: each {var} becomes (?P<var>[^.]+)
        # — segments are dot-separated, so a placeholder cannot greedily
        # eat segments it shouldn't.
        regex_str = "^" + self._PLACEHOLDER_RE.sub(r"(?P<\1>[^.]+)", re.escape(pattern)).replace(
            r"\(\?P<", "(?P<"
        ).replace(r">[^.]+\)", ">[^.]+)") + "$"
        # The above replace dance is needed because re.escape() escapes
        # the angle brackets in our placeholder substitution. Reconstruct
        # the pattern more explicitly:
        regex_str = "^"
        last = 0
        for m in self._PLACEHOLDER_RE.finditer(pattern):
            regex_str += re.escape(pattern[last : m.start()])
            regex_str += f"(?P<{m.group(1)}>[^.]+)"
            last = m.end()
        regex_str += re.escape(pattern[last:]) + "$"
        regex = re.compile(regex_str)

        self._patterns.append((pattern, regex, var_names, callback))

    def channel(self, pattern: str) -> Callable[[ChannelAuthCallback], ChannelAuthCallback]:
        """Decorator form of ``register``. Mirrors Laravel's
        ``Broadcast::channel`` when used as ``@Broadcast.channel(...)``::

            @broadcast.channel("user.{user_id}.alerts")
            async def authorize_user_alerts(user, user_id):
                return str(user.id) == str(user_id)
        """

        def _wrap(callback: ChannelAuthCallback) -> ChannelAuthCallback:
            self.register(pattern, callback)
            return callback

        return _wrap

    def find(self, channel: str) -> Optional[Tuple[ChannelAuthCallback, Dict[str, str]]]:
        """Look up the auth callback for a channel name.

        Returns ``(callback, placeholders)`` on the first matching
        pattern, or ``None`` if no pattern matches. ``channel`` must
        be the *unprefixed* channel name (the caller strips
        ``private-`` / ``presence-`` first).
        """
        for _, regex, _var_names, callback in self._patterns:
            m = regex.match(channel)
            if m:
                return callback, m.groupdict()
        return None

    async def authorize(
        self,
        channel: str,
        user: Any,
        *,
        require_callback: bool = True,
    ) -> Union[bool, Dict[str, Any]]:
        """Run the matching callback for ``channel``.

        Args:
            channel: The unprefixed channel name (no ``private-``).
            user: The authenticated socket user (may be ``None``).
            require_callback: If True (default), an unmatched
                channel returns ``False`` — auth-gated channels with no
                registered callback fail closed. If False (used by the
                "is this channel public?" probe), an unmatched channel
                returns ``True``.

        Returns:
            ``False`` to deny subscription, ``True`` to allow without
            presence data, or a ``dict`` with the joining member's
            presence info (PresenceChannel only).
        """
        match = self.find(channel)
        if match is None:
            return not require_callback

        callback, placeholders = match
        try:
            result = callback(user, **placeholders)
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            # An exception in user code → fail closed. Logging is the
            # caller's responsibility (Broadcasting.subscribe).
            raise

        if result is None or result is False:
            return False
        if result is True:
            return True
        if isinstance(result, dict):
            return result
        # Any other truthy → coerce to bool to match Laravel's
        # "any truthy means allowed" semantics. dict-not-bool already
        # handled above so this is defensive.
        return bool(result)


__all__ = ["ChannelRegistry", "ChannelAuthCallback"]
