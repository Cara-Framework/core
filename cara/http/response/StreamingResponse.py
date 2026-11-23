"""
Streaming Response Module.

Laravel-style streaming response support for HTTP responses.
Handles chunked transfers and real-time data streaming.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from collections.abc import AsyncGenerator, Callable
from typing import Any

from cara.support import defuse_csv_cell, json_dumps

from .BaseResponse import BaseResponse
from .HeaderManager import HeaderManager
from .StreamAborted import StreamAborted


class StreamingResponse:
    """
    Laravel-style streaming response handler.

    Provides support for streaming data to clients in chunks,
    useful for large files, real-time data, or server-sent events.
    """

    def __init__(self, base_response: BaseResponse):
        """
        Initialize StreamingResponse with BaseResponse.

        Args:
            base_response: BaseResponse instance to work with
        """
        self.response = base_response
        # Share the Response's HeaderManager so the explicit-content-type
        # flag isn't split across two instances — see the matching
        # comment in ResponseFactory.__init__.
        existing = getattr(base_response, "headers", None)
        self.headers = (
            existing
            if isinstance(existing, HeaderManager)
            else HeaderManager(base_response.header_bag)
        )

    async def stream(
        self,
        generator: AsyncGenerator[bytes],
        send: Callable,
        status: int = 200,
        content_type: str = "application/octet-stream",
        headers: dict[str, str] | None = None,
        receive: Callable | None = None,
    ) -> None:
        """
        Laravel-style streaming response.

        Args:
            generator: Async generator yielding bytes
            send: ASGI send callable
            status: HTTP status code
            content_type: Content type for stream
            headers: Additional headers
            receive: ASGI receive callable. When given, the stream stops as
                soon as the client disconnects (see ``_pump_until_disconnect``).
        """
        if self.response._sent:
            return

        # Set content type and additional headers
        self.headers.content_type(content_type)
        if headers:
            self.headers.merge(headers)

        # Get headers for ASGI (exclude content-length for streaming)
        response_headers = []
        for name, value in self.headers.all().items():
            if name.lower() != "content-length":
                response_headers.append((name.encode(), value.encode()))

        started = False
        try:
            # Headers are emitted before advancing the generator.  Slow data
            # sources therefore cannot delay time-to-first-byte or monopolise
            # the request handler before the client knows the download began.
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": response_headers,
                }
            )
            started = True
            self.response._started = True

            if receive is None:
                await self._pump(generator, send)
            elif await self._pump_until_disconnect(generator, send, receive):
                # The client is gone: nobody can read an end frame, and the
                # source has already been stopped. The response is over.
                self.response._sent = True
                return
        except Exception as e:
            logging.getLogger("cara.http.stream").error(
                "Stream generator raised: %s",
                e,
                exc_info=True,
            )
            if not started:
                # The caller can still render a regular 500 because no ASGI
                # response has begun yet.
                raise
            # Once headers are on the wire the status cannot change, and a
            # clean end frame would hand the reader a short file that looks
            # whole under a 200 (a report stream that failed on page 2).
            # Leaving with no end frame makes the server cut the connection,
            # which every client reports as a failed download.
            self.response._sent = True
            raise StreamAborted("stream failed after the response started") from e
        finally:
            await self._close(generator)

        # End stream
        await send(
            {
                "type": "http.response.body",
                "body": b"",
                "more_body": False,
            }
        )

        self.response._sent = True

    @staticmethod
    async def _pump(generator: Any, send: Callable) -> None:
        """Forward every chunk of ``generator`` as a body frame."""
        async for chunk in generator:
            await send(
                {
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": True,
                }
            )

    @classmethod
    async def _pump_until_disconnect(
        cls, generator: Any, send: Callable, receive: Callable
    ) -> bool:
        """Stream ``generator`` until it ends or the client goes away.

        uvicorn's ``send`` is a silent no-op once the client has gone, so a
        pump that only sends never learns of it: a CSV export whose tab was
        closed kept reading the database to its last row. The client's
        departure is announced on ``receive`` instead, so a watcher waits
        there while the pump runs, and the first to finish decides.

        Returns ``True`` when the client disconnected (the pump was cancelled,
        which also stops the source at its current ``await``; ``stream`` then
        closes it); otherwise the pump's own outcome — its exception
        included — is propagated.
        """
        pump = asyncio.ensure_future(cls._pump(generator, send))
        watcher = asyncio.ensure_future(cls._wait_for_disconnect(receive))
        try:
            await asyncio.wait({pump, watcher}, return_when=asyncio.FIRST_COMPLETED)
            if not pump.done() and watcher.result():
                pump.cancel()
                await asyncio.wait({pump})
                if not pump.cancelled() and pump.exception() is not None:
                    # Whatever the source raised while being stopped, the
                    # client is gone and there is no response left to fail.
                    logging.getLogger("cara.http.stream").debug(
                        "Stream source raised after client disconnect: %s",
                        pump.exception(),
                    )
                return True
            # The pump finished first, or the watcher could not tell (a
            # ``receive`` that failed): the stream runs to its own end.
            await asyncio.wait({pump})
            pump.result()
            return False
        finally:
            # Neither child may outlive the request — on the success path, on
            # an error, and when this task itself is cancelled at shutdown.
            # The pump must have stopped before ``_close`` runs: closing a
            # generator another task is still iterating raises RuntimeError.
            for task in (pump, watcher):
                if not task.done():
                    task.cancel()
            await asyncio.wait({pump, watcher})

    @staticmethod
    async def _wait_for_disconnect(receive: Callable) -> bool:
        """Block until the client disconnects; ``True`` when it did.

        Request-body frames the handler never read are drained and dropped:
        the response has already started, so nothing will read them now.
        A failing ``receive`` gives ``False`` — it cannot say the client is
        gone, so it must not cut a stream short.
        """
        try:
            while True:
                message = await receive()
                if message.get("type") == "http.disconnect":
                    return True
        except Exception as exc:
            logging.getLogger("cara.http.stream").warning(
                "Disconnect watch stopped, stream continues: %s", exc, exc_info=True
            )
            return False

    @staticmethod
    async def _close(generator: Any) -> None:
        """Finish the body generator NOW, whichever way the stream ended.

        A stream that stops early — ``send`` raised, the task was cancelled at
        shutdown — leaves the generator suspended at a ``yield`` with its
        ``finally`` blocks unrun. Python only runs them when the generator is
        garbage-collected, and the exception that stopped the stream keeps it
        reachable through its traceback: the export's database cursor and its
        pooled connection stayed checked out until a later collection. Closing
        here releases them before the request ends. A generator that already
        ran to its end, or raised, is closed and this is a no-op.
        """
        close = getattr(generator, "aclose", None)
        if close is None:
            return
        try:
            await close()
        except Exception as exc:
            # The stream's own outcome (or error) stands; a failing cleanup
            # must not replace it.
            logging.getLogger("cara.http.stream").warning(
                "Stream generator cleanup raised: %s", exc, exc_info=True
            )

    @staticmethod
    async def csv_chunks(
        data_generator: AsyncGenerator[list[Any]],
        *,
        chunk_size: int = 64 * 1024,
        cooperative_rows: int = 16,
    ) -> AsyncGenerator[bytes]:
        """Encode CSV rows into bounded chunks.

        The first row is flushed immediately (normally the header) so clients
        receive body bytes before the backing data query starts.  Remaining
        rows are coalesced up to ``chunk_size`` to avoid one ASGI event per row
        while keeping memory flat for arbitrarily large exports.

        Every cell is defused against spreadsheet formulas here, so an export
        cannot forget to. It was a per-call convention, and three exports did
        forget (CWE-1236). A number passes untouched: a source that wants a
        numeric column yields numbers, not signed strings.
        """

        output = io.StringIO(newline="")
        writer = csv.writer(output)
        first = True
        processed = 0

        try:
            async for row in data_generator:
                writer.writerow([defuse_csv_cell(cell) for cell in row])
                processed += 1
                if first or output.tell() >= chunk_size:
                    yield output.getvalue().encode("utf-8")
                    output.seek(0)
                    output.truncate(0)
                    first = False

                # An async generator may produce immediately-ready rows without a
                # real await between them.  Explicitly hand control back after a
                # small bounded slice so CSV encoding/model serialization cannot
                # starve unrelated requests on the ASGI event loop.
                if cooperative_rows > 0 and processed % cooperative_rows == 0:
                    await asyncio.sleep(0)
        finally:
            # Closing THIS generator does not close the one it reads: leaving
            # an ``async for`` early only abandons its source. The source is
            # the export's row cursor, so it is closed here (see ``_close``).
            await StreamingResponse._close(data_generator)

        if output.tell():
            yield output.getvalue().encode("utf-8")

    def _format_sse_event(self, event: dict[str, Any]) -> str:
        """
        Format event data for Server-Sent Events.

        Args:
            event: Event dictionary with optional keys: id, event, data, retry

        Returns:
            str: Formatted SSE event string
        """
        lines = []

        if "id" in event:
            lines.append(f"id: {event['id']}")

        if "event" in event:
            lines.append(f"event: {event['event']}")

        if "retry" in event:
            lines.append(f"retry: {event['retry']}")

        if "data" in event:
            data = event["data"]
            if isinstance(data, (dict, list)):
                data = json_dumps(data)
            # RFC 8030 §8.3: a multi-line ``data`` field MUST emit
            # ``data: `` on EVERY line — the SSE parser at the
            # browser end joins consecutive ``data:`` lines with
            # ``\n`` to reconstruct the value. Pre-fix this branch
            # emitted a single ``data: <stringified>`` so an
            # embedded ``\n`` (very common in user-content payloads,
            # multi-line markdown, or a JSON value with newline
            # whitespace) split the field — the client read the
            # first line as ``data: <head>``, treated the second
            # line as a malformed field (no ``data:`` prefix → SSE
            # spec says ignore), and the event was either
            # truncated to the first line or dropped entirely.
            for line in str(data).split("\n"):
                lines.append(f"data: {line}")

        lines.append("")  # Empty line to end event
        lines.append("")  # Double newline for SSE format

        return "\n".join(lines)

    async def stream_template_chunks(
        self,
        template_generator: AsyncGenerator[str],
        send: Callable,
        status: int = 200,
        headers: dict[str, str] | None = None,
        receive: Callable | None = None,
    ) -> None:
        """
        Stream HTML template chunks for progressive rendering.

        Args:
            template_generator: Async generator yielding HTML chunks
            send: ASGI send callable
            status: HTTP status code
            headers: Additional headers
        """

        async def html_chunk_generator():
            try:
                async for html_chunk in template_generator:
                    yield html_chunk.encode("utf-8")
            finally:
                await self._close(template_generator)

        await self.stream(
            html_chunk_generator(),
            send,
            status=status,
            content_type="text/html; charset=utf-8",
            headers=headers,
            receive=receive,
        )
