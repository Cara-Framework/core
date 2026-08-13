"""Broadcast channels cross the core as value objects, never raw strings."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cara.broadcasting import (
    Broadcasting,
    Channel,
    PresenceChannel,
    PrivateChannel,
    channel_from_wire,
    channel_name,
)
from cara.exceptions import (
    BroadcastingConfigurationException,
    InvalidArgumentException,
)


def test_wire_names_are_parsed_once_into_the_exact_channel_type() -> None:
    assert channel_from_wire("updates") == Channel("updates")
    assert channel_from_wire("private-user.7") == PrivateChannel("user.7")
    assert channel_from_wire("presence-room.2") == PresenceChannel("room.2")

    with pytest.raises(InvalidArgumentException):
        PrivateChannel("private-user.7")
    with pytest.raises(InvalidArgumentException):
        Channel("private-user.7")
    with pytest.raises(InvalidArgumentException):
        channel_from_wire(" updates")
    with pytest.raises(TypeError):
        channel_name("updates")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_presence_authorization_requires_identity_data() -> None:
    broadcasting = Broadcasting(SimpleNamespace(), "memory")
    broadcasting.channel("room.{room_id}", lambda _user, room_id: True)

    assert await broadcasting.authorize_subscription(PrivateChannel("room.2"), object())
    with pytest.raises(
        BroadcastingConfigurationException,
        match="identity dict",
    ):
        await broadcasting.authorize_subscription(PresenceChannel("room.2"), object())

    broadcasting = Broadcasting(SimpleNamespace(), "memory")
    broadcasting.channel("room.{room_id}", lambda _user, room_id: {"id": room_id})
    assert await broadcasting.authorize_subscription(
        PresenceChannel("room.2"), object()
    ) == {"id": "2"}
    with pytest.raises(BroadcastingConfigurationException, match="PresenceChannel"):
        await broadcasting.authorize_subscription(PrivateChannel("room.2"), object())


@pytest.mark.asyncio
async def test_authorization_callback_refuses_ambiguous_truthy_values() -> None:
    broadcasting = Broadcasting(SimpleNamespace(), "memory")
    broadcasting.channel("room.{room_id}", lambda _user, room_id: "yes")

    with pytest.raises(BroadcastingConfigurationException, match="bool, dict, or None"):
        await broadcasting.authorize_subscription(PrivateChannel("room.2"), object())


def test_broadcast_normalization_refuses_raw_channel_strings() -> None:
    with pytest.raises(TypeError, match="Channel"):
        Broadcasting._normalize_channels("updates")  # type: ignore[arg-type]
