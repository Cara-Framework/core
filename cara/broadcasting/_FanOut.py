"""
The fan-out half of ``ConnectionManager``: one frame, many sockets.

Split out of ``ConnectionManager`` for the file budget (DOCTRINE §5) and bound
back onto the class there. Channel and per-user broadcasts both end here.
"""

from __future__ import annotations

import asyncio
from typing import Any

from cara.broadcasting.SocketRetirement import SocketRetirement
from cara.facades import Log
from cara.support import json_dumps


async def _fan_out_send(
    self,
    connection_ids: list[str],
    message: dict[str, Any],
    *,
    skip_conn_id: str | None,
    label: str,
) -> int:
    """Concurrently dispatch ``message`` to every connection id.

    Shared between channel + user-channel fan-out. ``label`` is
    only used for diagnostics so the log line carries the channel
    / user id that fanned out.
    """
    # Pair each (connection_id, websocket) so we can map gather
    # results back to ids without re-looking-up after the await.
    targets: list[tuple] = []
    for connection_id in connection_ids:
        if skip_conn_id and connection_id == skip_conn_id:
            continue
        ws = self.connections.get(connection_id)
        if ws is None:
            # Stale entry — was removed between snapshot and now.
            continue
        targets.append((connection_id, ws))

    if not targets:
        return 0

    # Encode ONCE, before anyone is sent anything. Each socket used to
    # encode the same frame for itself (``send_json``), which cost a
    # 500-subscriber channel 500 encodes per frame, and turned a payload
    # cara cannot encode into 500 "send failures": every healthy
    # subscriber was reaped for the publisher's programming error. That
    # error now fails this one broadcast, loudly, and nobody is touched.
    try:
        text = json_dumps(message)
    except Exception as e:
        Log.error(
            "Broadcast '%s' on %s dropped, its payload cannot be encoded: %s",
            message.get("event"),
            label,
            e,
            category="cara.broadcasting",
        )
        return 0

    # ``return_exceptions=True`` so a single dead client never
    # tears down the whole fan-out — we collect failures and
    # remove them after. Each send is bounded by
    # ``_broadcast_send_timeout``: a client whose TCP send window is full
    # would otherwise stall its ``send_json`` indefinitely, and ``gather``
    # would not resolve until it did — holding the broadcast caller (and,
    # on the Redis driver, the cross-pod publish gated behind it) hostage to
    # the single slowest socket. A timed-out send is collected as a failure
    # and the connection is reaped below, same as any other dead client.
    results = await asyncio.gather(
        *(
            asyncio.wait_for(
                _send_frame(ws, message, text),
                timeout=self._broadcast_send_timeout,
            )
            for _, ws in targets
        ),
        return_exceptions=True,
    )

    delivered = 0
    failed: list[tuple[str, Any]] = []
    for (connection_id, ws), result in zip(targets, results, strict=False):
        if isinstance(result, Exception):
            if _is_connection_closed_error(result):
                Log.debug(
                    "Subscriber %s closed mid-send on %s",
                    connection_id,
                    label,
                    category="cara.broadcasting",
                )
            else:
                Log.warning(
                    "Send to %s on %s failed: %s",
                    connection_id,
                    label,
                    result,
                    category="cara.broadcasting",
                )
            failed.append((connection_id, ws))
        else:
            delivered += 1

    # Remove dead connections in parallel too — a 100-subscriber
    # broadcast where 50 failed would otherwise serially walk
    # the cleanup, holding the broadcast caller for the duration.
    if failed:
        await asyncio.gather(
            *(self.remove_connection(cid) for cid, _ in failed),
            return_exceptions=True,
        )
        await SocketRetirement.settle(
            [
                self._retirement.begin(
                    ws, SocketRetirement.CLOSE_UNRESPONSIVE, "Send timed out"
                )
                for _, ws in failed
            ]
        )

    if delivered:
        Log.debug(
            "Delivered to %s/%s on %s",
            delivered,
            len(targets),
            label,
            category="cara.broadcasting",
        )
    return delivered


# Surface-level pattern matching for "the client just dropped" errors.
# Centralised here so heartbeat / broadcast send paths share the same
# benign-error filter.
_CONNECTION_CLOSED_MARKERS = (
    "ConnectionClosed",
    "WebSocket is closed",
    "Connection closed",
    "ClientDisconnected",
    "Connection is closed",
    "websocket.close",
    "ASGI message",
)


def _send_frame(ws: Any, message: dict[str, Any], text: str) -> Any:
    """Hand one fan-out frame to one socket, already encoded where it can be.

    ``Socket`` takes the text as it is. A registered object that only speaks
    ``send_json`` (the registry's older contract) is given the message itself
    and encodes it as before, so it keeps working, just without the saving.
    """
    send_text = getattr(ws, "send_text", None)
    if send_text is not None:
        return send_text(text)
    return ws.send_json(message)


def _is_connection_closed_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _CONNECTION_CLOSED_MARKERS)
