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

from collections.abc import Sequence
from typing import Any

from cara.broadcasting.Channel import Channel, channel_name
from cara.broadcasting.ChannelRegistry import ChannelAuthCallback, ChannelRegistry
from cara.broadcasting.contracts import ShouldBroadcast
from cara.broadcasting.PresenceChannel import PresenceChannel
from cara.broadcasting.PrivateChannel import PrivateChannel
from cara.exceptions import BroadcastingConfigurationException
from cara.facades import Log


class Broadcasting:
    """High-level broadcasting API for the application."""

    def __init__(self, application: Any, default_driver: str) -> None:
        self.application = application
        self.default_driver = default_driver
        self._drivers: dict[str, Any] = {}
        self._channels = ChannelRegistry()

    # ------------------------------------------------------------------
    # Driver management
    # ------------------------------------------------------------------
    def driver(self, name: str | None = None) -> Any:
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
    def channel(self, pattern: str, callback: ChannelAuthCallback | None = None):
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
        channel: Channel,
        user: Any,
    ) -> bool | dict[str, Any]:
        """Decide whether ``user`` may subscribe to ``channel``.

        Public channels (no recognised auth prefix) always pass.
        Auth-gated channels (``private-...`` / ``presence-...``) must
        match a registered callback that returns truthy.

        Returns either ``True`` / a presence dict on success or
        ``False`` on denial — callers translate the False to a 4007
        on the wire.
        """
        if not isinstance(channel, Channel):
            raise TypeError(f"channel must be Channel, got {type(channel).__name__}")
        if isinstance(channel, PrivateChannel):
            result = await self._channels.authorize(
                channel.name, user, require_callback=True
            )
            if isinstance(channel, PresenceChannel) and result is True:
                raise BroadcastingConfigurationException(
                    "Presence-channel authorization must return an identity dict."
                )
            if not isinstance(channel, PresenceChannel) and isinstance(result, dict):
                raise BroadcastingConfigurationException(
                    "Identity data is valid only for a PresenceChannel."
                )
            return result
        # Public channel — allowed without callback. Apps that want
        # to require auth on a public channel can register a callback
        # for its name explicitly; we'll consult it.
        match = self._channels.find(channel.name)
        if match is not None:
            result = await self._channels.authorize(
                channel.name, user, require_callback=True
            )
            if isinstance(result, dict):
                raise BroadcastingConfigurationException(
                    "Identity data is valid only for a PresenceChannel."
                )
            return result
        return True

    # ------------------------------------------------------------------
    # Direct dispatch — used by listeners that build the channel + name
    # at the call site. Most callers should prefer ``broadcast_event``.
    # ------------------------------------------------------------------
    async def broadcast(
        self,
        channels: Channel | Sequence[Channel],
        event: str,
        data: dict[str, Any] | None = None,
        *,
        except_socket_id: str | None = None,
        driver: str | None = None,
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

        if not isinstance(event, ShouldBroadcast):
            raise BroadcastingConfigurationException(
                "Event must implement ShouldBroadcast interface"
            )

        # broadcast_when() AND not broadcast_unless() — both gates
        # must pass. Mirrors Laravel where both methods exist and
        # both must allow the broadcast.
        should_fire = bool(event.broadcast_when()) and not bool(event.broadcast_unless())
        if not should_fire:
            Log.debug(
                "Broadcast skipped for %s (broadcast_when/unless gated it)",
                type(event).__name__,
                category="cara.broadcasting",
            )
            return

        names = self._normalize_channels(event.broadcast_on())
        event_name = event.broadcast_as()
        data = event.broadcast_with()
        except_sid = event.except_socket_id()
        driver_name = event.broadcast_via()

        Log.debug(
            "Broadcasting '%s' on %s (driver=%s, except_socket_id=%s)",
            event_name,
            names,
            driver_name or self.default_driver,
            except_sid or "-",
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
                "Broadcast failed for '%s' on %s: %s",
                event_name,
                names,
                e,
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
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.driver().add_connection(connection_id, websocket, user_id, metadata)

    async def remove_connection(self, connection_id: str) -> None:
        await self.driver().remove_connection(connection_id)

    async def subscribe(self, connection_id: str, channel: Channel) -> bool:
        return await self.driver().subscribe(connection_id, channel_name(channel))

    async def unsubscribe(self, connection_id: str, channel: Channel) -> bool:
        return await self.driver().unsubscribe(connection_id, channel_name(channel))

    async def broadcast_to_user(
        self,
        user_id: str,
        event: str,
        data: dict[str, Any],
        *,
        except_socket_id: str | None = None,
    ) -> None:
        await self.driver().broadcast_to_user(
            user_id, event, data, except_socket_id=except_socket_id
        )

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------
    def get_connection_count(self) -> int:
        return self.driver().get_connection_count()

    def get_channel_subscribers(self, channel: Channel) -> list[str]:
        return self.driver().get_channel_subscribers(channel_name(channel))

    def get_stats(self) -> dict[str, Any]:
        return self.driver().get_stats()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_channels(
        channels: Channel | Sequence[Channel],
    ) -> list[str]:
        if isinstance(channels, Channel):
            return [channel_name(channels)]
        if isinstance(channels, (list, tuple)):
            return [channel_name(c) for c in channels]
        raise TypeError(
            "channels must be Channel or a sequence of Channel objects; "
            f"got {type(channels).__name__}"
        )
