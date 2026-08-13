"""Broadcasting subsystem — the public API."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BroadcastEvent": (".BroadcastEvent", "BroadcastEvent"),
    "Broadcaster": (".contracts", "Broadcaster"),
    "Broadcasting": (".Broadcasting", "Broadcasting"),
    "BroadcastingProvider": (".BroadcastingProvider", "BroadcastingProvider"),
    "Channel": (".Channel", "Channel"),
    "ChannelAuthCallback": (".ChannelRegistry", "ChannelAuthCallback"),
    "ChannelRegistry": (".ChannelRegistry", "ChannelRegistry"),
    "ConnectionManager": (".ConnectionManager", "ConnectionManager"),
    "LogBroadcaster": (".drivers", "LogBroadcaster"),
    "MemoryBroadcaster": (".drivers", "MemoryBroadcaster"),
    "NullBroadcaster": (".drivers", "NullBroadcaster"),
    "PresenceChannel": (".PresenceChannel", "PresenceChannel"),
    "PrivateChannel": (".PrivateChannel", "PrivateChannel"),
    "RedisBroadcaster": (".drivers", "RedisBroadcaster"),
    "ShouldBroadcast": (".contracts", "ShouldBroadcast"),
    "broadcast": (".helpers", "broadcast"),
    "broadcast_async": (".helpers", "broadcast_async"),
    "broadcast_event": (".helpers", "broadcast_event"),
    "broadcast_event_async": (".helpers", "broadcast_event_async"),
    "broadcast_to_user_async": (".helpers", "broadcast_to_user_async"),
    "channel_from_wire": (".Channel", "channel_from_wire"),
    "channel_name": (".Channel", "channel_name"),
}

__all__ = [
    "BroadcastEvent",
    "Broadcaster",
    "Broadcasting",
    "BroadcastingProvider",
    "Channel",
    "ChannelAuthCallback",
    "ChannelRegistry",
    "ConnectionManager",
    "LogBroadcaster",
    "MemoryBroadcaster",
    "NullBroadcaster",
    "PresenceChannel",
    "PrivateChannel",
    "RedisBroadcaster",
    "ShouldBroadcast",
    "broadcast",
    "broadcast_async",
    "broadcast_event",
    "broadcast_event_async",
    "broadcast_to_user_async",
    "channel_from_wire",
    "channel_name",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
