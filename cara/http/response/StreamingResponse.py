"""
Streaming Response Module.

Laravel-style streaming response support for HTTP responses.
Handles chunked transfers and real-time data streaming.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from typing import Any

from .BaseResponse import BaseResponse
from .HeaderManager import HeaderManager


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
    ) -> None:
        """
        Laravel-style streaming response.

        Args:
            generator: Async generator yielding bytes
            send: ASGI send callable
            status: HTTP status code
            content_type: Content type for stream
            headers: Additional headers
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

            async for chunk in generator:
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
        except Exception as e:
            import logging

            logging.getLogger("cara.http.stream").error(
                "Stream generator raised: %s",
                e,
                exc_info=True,
            )
            if not started:
                # The caller can still render a regular 500 because no ASGI
                # response has begun yet.
                raise
            # Once headers are on the wire the status cannot change.  Close
            # the body without leaking exception text into the download.
            try:
                await send(
                    {
                        "type": "http.response.body",
                        "body": b"",
                        "more_body": False,
                    }
                )
            finally:
                self.response._sent = True
            return

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
        """
        import asyncio
        import csv
        import io

        output = io.StringIO(newline="")
        writer = csv.writer(output)
        first = True
        processed = 0

        async for row in data_generator:
            writer.writerow(row)
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

        if output.tell():
            yield output.getvalue().encode("utf-8")

    async def stream_json_lines(
        self,
        data_generator: AsyncGenerator[Any],
        send: Callable,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Stream JSON data line by line (JSONL format).

        Args:
            data_generator: Async generator yielding JSON-serializable data
            send: ASGI send callable
            status: HTTP status code
            headers: Additional headers
        """
        import json

        async def json_chunk_generator():
            async for data in data_generator:
                json_line = f"{json.dumps(data, ensure_ascii=False, default=str)}\n"
                yield json_line.encode("utf-8")

        await self.stream(
            json_chunk_generator(),
            send,
            status=status,
            content_type="application/x-ndjson; charset=utf-8",
            headers=headers,
        )

    async def stream_server_sent_events(
        self,
        event_generator: AsyncGenerator[dict[str, Any]],
        send: Callable,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Stream Server-Sent Events (SSE).

        Args:
            event_generator: Async generator yielding event dictionaries
            send: ASGI send callable
            status: HTTP status code
            headers: Additional headers
        """

        async def sse_chunk_generator():
            async for event in event_generator:
                sse_data = self._format_sse_event(event)
                yield sse_data.encode("utf-8")

        # Set SSE headers (Laravel-style)
        sse_headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        }

        if headers:
            sse_headers.update(headers)

        await self.stream(
            sse_chunk_generator(),
            send,
            status=status,
            content_type="text/event-stream; charset=utf-8",
            headers=sse_headers,
        )

    async def stream_file_download(
        self,
        file_generator: AsyncGenerator[bytes],
        filename: str,
        send: Callable,
        content_type: str = "application/octet-stream",
        content_length: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Stream file download.

        Args:
            file_generator: Async generator yielding file chunks
            filename: Download filename
            send: ASGI send callable
            content_type: File content type
            content_length: Total file size (optional)
            headers: Additional headers
        """
        download_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }

        if content_length is not None:
            download_headers["Content-Length"] = str(content_length)

        if headers:
            download_headers.update(headers)

        await self.stream(
            file_generator,
            send,
            status=200,
            content_type=content_type,
            headers=download_headers,
        )

    async def stream_csv(
        self,
        data_generator: AsyncGenerator[list[str]],
        filename: str,
        send: Callable,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Stream CSV data.

        Args:
            data_generator: Async generator yielding CSV rows
            filename: CSV filename
            send: ASGI send callable
            headers: Additional headers
        """
        download_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }

        if headers:
            download_headers.update(headers)

        await self.stream(
            self.csv_chunks(data_generator),
            send,
            status=200,
            content_type="text/csv; charset=utf-8",
            headers=download_headers,
        )

    def _format_sse_event(self, event: dict[str, Any]) -> str:
        """
        Format event data for Server-Sent Events.

        Args:
            event: Event dictionary with optional keys: id, event, data, retry

        Returns:
            str: Formatted SSE event string
        """
        import json

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
                data = json.dumps(data, ensure_ascii=False, default=str)
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
            async for html_chunk in template_generator:
                yield html_chunk.encode("utf-8")

        await self.stream(
            html_chunk_generator(),
            send,
            status=status,
            content_type="text/html; charset=utf-8",
            headers=headers,
        )
