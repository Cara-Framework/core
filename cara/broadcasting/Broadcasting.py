"""
Broadcasting manager — Laravel BroadcastManager equivalent.

Sits at the front of the broadcasting subsystem and coordinates:

- driver registration + selection (memory / redis / log / null),
- channel authorization callbacks (``Broadcast::channel(...)``),
- event dispatch (``broadcast_event``),
- direct broadcast / per-user broadcast / connection lifecycle
  forwarding to whichever driver is active.

The manager itself holds no transport state — that's all in the
drivers. It owns the channel registry because authorization is a
cross-driver concern (the same callback should run regardless of
whether redis or memory is the active broadcaster).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log

from cara.broadcasting.Channel import Channel, channel_name
from cara.broadcasting.ChannelRegistry import ChannelAuthCallback, ChannelRegistry

ChannelLike = Union[str, Channel]


class Broadcasting:
    """High-level broadcasting API for the application."""

    # Channel-name prefixes that require ``ChannelRegistry.authorize``
    # to return truthy before a subscription is committed. Public
    # channels (no recognised prefix) do not consult the registry.
    _AUTH_PREFIXES = ("private-", "presence-")

    def __init__(self, application: Any, default_driver: str) -> None:
        self.application = application
        self.default_driver = default_driver
        self._drivers: Dict[str, Any] = {}
        self._channels = ChannelRegistry()

    # ------------------------------------------------------------------
    # Driver management
    # ------------------------------------------------------------------
    def driver(self, name: Optional[str] = None) -> Any:
        """Resolve a driver by name. Falls back to the default driver
        when ``name`` is omitted. Raises if the driver isn't registered
        — callers should configure the driver they intend to use."""
        name = name or self.default_driver
        try:
            return self._drivers[name]
        except KeyError as e:
            raise BroadcastingConfigurationException(
                f"Broadcasting driver '{name}' is not registered."
            ) from e

    def add_driver(self, name: str, driver_instance: Any) -> None:
        """Register a driver and wire its cleanup hook into the
        application's shutdown callbacks so background tasks stop on
        graceful exit."""
        self._drivers[name] = driver_instance
        if hasattr(driver_instance, "cleanup"):
            if not hasattr(self.application, "_shutdown_callbacks"):
                self.application._shutdown_callbacks = []
            if driver_instance.cleanup not in self.application._shutdown_callbacks:
                self.application._shutdown_callbacks.append(driver_instance.cleanup)

    # ------------------------------------------------------------------
    # Channel authorization (Laravel ``Broadcast::channel``)
    # ------------------------------------------------------------------
    def channel(self, pattern: str, callback: Optional[ChannelAuthCallback] = None):
        """Register a channel auth callback.

        Usable as either::

            Broadcast.channel("user.{id}.alerts", lambda user, id: str(user.id) == id)

        or as a decorator::

            @Broadcast.channel("user.{id}.alerts")
            async def authorize(user, id):
                return str(user.id) == id
        """
        if callback is None:
            return self._channels.channel(pattern)
        self._channels.register(pattern, callback)
        return None

    @property
    def channels(self) -> ChannelRegistry:
        return self._channels

    async def authorize_subscription(
        self,
        channel: str,
        user: Any,
    ) -> Union[bool, Dict[str, Any]]:
        """Decide whether ``user`` may subscribe to ``channel``.

        Public channels (no recognised auth prefix) always pass.
        Auth-gated channels (``private-...`` / ``presence-...``) must
        match a registered callback that returns truthy.

        Returns either ``True`` / a presence dict on success or
        ``False`` on denial — callers translate the False to a 4007
        on the wire.
        """
        for prefix in self._AUTH_PREFIXES:
            if channel.startswith(prefix):
                # Strip the prefix (e.g. "private-") once before
                # consulting the registry. Patterns are written
                # without the prefix so they match Laravel's
                # convention of ``Broadcast::channel('user.{id}', ...)``
                # for a ``private-user.{id}`` wire channel.
                bare = channel[len(prefix) :]
                return await self._channels.authorize(bare, user, require_callback=True)
        # Public channel — allowed without callback. Apps that want
        # to require auth on a public channel can register a callback
        # for its name explicitly; we'll consult it.
        match = self._channels.find(channel)
        if match is not None:
            return await self._channels.authorize(channel, user, require_callback=True)
        return True

    # ------------------------------------------------------------------
    # Direct dispatch — used by listeners that build the channel + name
    # at the call site. Most callers should prefer ``broadcast_event``.
    # ------------------------------------------------------------------
    async def broadcast(
        self,
        channels: Union[ChannelLike, Sequence[ChannelLike]],
        event: str,
        data: Optional[Dict[str, Any]] = None,
        *,
        except_socket_id: Optional[str] = None,
        driver: Optional[str] = None,
    ) -> None:
        """Fan out ``event`` to ``channels`` via the (optional) named
        driver or the default."""
        names = self._normalize_channels(channels)
        await self.driver(driver).broadcast(
            names, event, data or {}, except_socket_id=except_socket_id
        )

    async def broadcast_event(self, event: Any) -> None:
        """Dispatch a ``ShouldBroadcast`` event."""
        # Local import keeps the contracts module from pulling cara.facades
        # at module-load time.
        from cara.broadcasting.contracts import ShouldBroadcast

        if not isinstance(event, ShouldBroadcast):
            raise BroadcastingConfigurationException(
                "Event must implement ShouldBroadcast interface"
            )

        # broadcast_when() AND not broadcast_unless() — both gates
        # must pass. Mirrors Laravel where both methods exist and
        # both must allow the broadcast.
        try:
            should_fire = bool(event.broadcast_when()) and not bool(event.broadcast_unless())
        except Exception as e:
            Log.warning(
                f"broadcast_when/unless on {type(event).__name__} raised: {e}",
                category="cara.broadcasting",
            )
            return
        if not should_fire:
            Log.debug(
                f"Broadcast skipped for {type(event).__name__} "
                f"(broadcast_when/unless gated it)",
                category="cara.broadcasting",
            )
            return

        names = self._normalize_channels(event.broadcast_on())
        event_name = event.broadcast_as()
        data = event.broadcast_with()
        except_sid = None
        try:
            except_sid = event.except_socket_id()
        except Exception:
            except_sid = None
        driver_name = None
        try:
            driver_name = event.broadcast_via()
        except Exception:
            driver_name = None

        Log.debug(
            f"Broadcasting '{event_name}' on {names} "
            f"(driver={driver_name or self.default_driver}, "
            f"except_socket_id={except_sid or '-'})",
            category="cara.broadcasting",
        )
        try:
            await self.driver(driver_name).broadcast(
                names, event_name, data, except_socket_id=except_sid
            )
        except Exception as e:
            # Log + re-raise. Silently swallowing was the old
            # behaviour and it hid real bugs (Redis down, payload too
            # large, etc.). Callers who want best-effort dispatch can
            # wrap their own try/except.
            Log.error(
                f"Broadcast failed for '{event_name}' on {names}: {e}",
                category="cara.broadcasting",
                exc_info=True,
            )
            raise

    # ------------------------------------------------------------------
    # Connection lifecycle (forward to active driver)
    # ------------------------------------------------------------------
    async def add_connection(
        self,
        connection_id: str,
        websocket: Any,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self.driver().add_connection(connection_id, websocket, user_id, metadata)

    async def remove_connection(self, connection_id: str) -> None:
        await self.driver().remove_connection(connection_id)

    async def subscribe(self, connection_id: str, channel: ChannelLike) -> bool:
        return await self.driver().subscribe(connection_id, channel_name(channel))

    async def unsubscribe(self, connection_id: str, channel: ChannelLike) -> bool:
        return await self.driver().unsubscribe(connection_id, channel_name(channel))

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: Dict[str, Any],
        *,
        except_socket_id: Optional[str] = None,
    ) -> None:
        await self.driver().broadcast_to_user(
            user_id, event, data, except_socket_id=except_socket_id
        )

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------
    def get_connection_count(self) -> int:
        return self.driver().get_connection_count()

    def get_channel_subscribers(self, channel: ChannelLike) -> List[str]:
        return self.driver().get_channel_subscribers(channel_name(channel))

    def get_stats(self) -> Dict[str, Any]:
        return self.driver().get_stats()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_channels(
        channels: Union[ChannelLike, Sequence[ChannelLike]],
    ) -> List[str]:
        if isinstance(channels, (str, Channel)):
            return [channel_name(channels)]
        if isinstance(channels, (list, tuple)):
            return [channel_name(c) for c in channels]
        raise TypeError(
            f"channels must be str, Channel, or sequence of either; got {type(channels).__name__}"
        )
