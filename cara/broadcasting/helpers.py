"""
Broadcasting Helper Functions.

Laravel-style helper functions for broadcasting.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Union

from cara.broadcasting import BroadcastEvent
from cara.broadcasting.contracts import ShouldBroadcast
from cara.facades import Broadcast


def broadcast(
    channels: Union[str, List[str]], event: str = "message", data: Dict[str, Any] = None
):
    """
    Laravel-style broadcast helper.

    Usage:
        broadcast("user.123", "order.created", {"order_id": 456})
        broadcast(["user.123", "admin"], "notification", {"message": "Hello"})
    """
    event_obj = BroadcastEvent(channels, event, data or {})
    # This needs to be run in an event loop
    asyncio.run(broadcast_event(event_obj))


def broadcast_event(event: ShouldBroadcast):
    """
    Broadcast a custom event class.

    Usage:
        await broadcast_event(OrderCreated(data={"order_id": 456}))
    """

    # This helper now ensures it works correctly whether called from a sync or async context.
    # It always returns an awaitable coroutine.
    async def run_broadcast():
        await Broadcast.broadcast_event(event)

    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # If we're in an async context, create a task to run in the background
            # without blocking the current flow.
            return loop.create_task(run_broadcast())
    except RuntimeError:
        # No running loop, so we're in a sync context.
        pass

    # In a sync context, run the coroutine to completion.
    asyncio.run(run_broadcast())


# Laravel-style async helpers for direct use in async contexts
async def broadcast_async(
    channels: Union[str, List[str]], event: str = "message", data: Dict[str, Any] = None
):
    """
    Async version of broadcast helper.

    Usage:
        await broadcast_async("user.123", "order.created", {"order_id": 456})
    """
    await Broadcast.broadcast(channels, event, data or {})


async def broadcast_event_async(event: ShouldBroadcast):
    """
    Async version of broadcast_event helper.

    Usage:
        await broadcast_event_async(OrderCreated(data={"order_id": 456}))
    """
    await Broadcast.broadcast_event(event)


# Helper function for broadcasting to specific users
def broadcast_to_user(user_id: str, event: str = "message", data: Dict[str, Any] = None):
    """
    Broadcast to a specific user.

    Usage:
        broadcast_to_user("123", "notification", {"message": "Hello"})
    """

    async def _broadcast():
        await Broadcast.broadcast_to_user(user_id, event, data or {})

    if hasattr(asyncio, "current_task") and asyncio.current_task():
        return _broadcast()
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_broadcast())
        finally:
            loop.close()


async def broadcast_to_user_async(
    user_id: str, event: str = "message", data: Dict[str, Any] = None
):
    """
    Async version of broadcast_to_user.

    Usage:
        await broadcast_to_user_async("123", "notification", {"message": "Hello"})
    """
    await Broadcast.broadcast_to_user(user_id, event, data or {})


def _get_current_timestamp():
    """Get current timestamp in ISO format."""
    return datetime.now().isoformat()
