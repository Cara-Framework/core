"""
Streaming Response Module.

Laravel-style streaming response support for HTTP responses.
Handles chunked transfers and real-time data streaming.
"""

from typing import Any, AsyncGenerator, Callable, Dict, List

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
        self.headers = HeaderManager(base_response.header_bag)

    async def stream(
        self,
        generator: AsyncGenerator[bytes, None],
        send: Callable,
        status: int = 200,
        content_type: str = "application/octet-stream",
        headers: Dict[str, str] = None,
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

        # Send response start
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": response_headers,
            }
        )

        # Stream content chunks
        try:
            async for chunk in generator:
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
        except Exception as e:
            # Log error and close stream gracefully
            error_chunk = f"Stream error: {str(e)}".encode("utf-8")
            await send(
                {
                    "type": "http.response.body",
                    "body": error_chunk,
                    "more_body": False,
                }
            )
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

    async def stream_json_lines(
        self,
        data_generator: AsyncGenerator[Any, None],
        send: Callable,
        status: int = 200,
        headers: Dict[str, str] = None,
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
                json_line = json.dumps(data, ensure_ascii=False) + "\n"
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
        event_generator: AsyncGenerator[Dict[str, Any], None],
        send: Callable,
        status: int = 200,
        headers: Dict[str, str] = None,
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
        file_generator: AsyncGenerator[bytes, None],
        filename: str,
        send: Callable,
        content_type: str = "application/octet-stream",
        content_length: int = None,
        headers: Dict[str, str] = None,
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
        data_generator: AsyncGenerator[List[str], None],
        filename: str,
        send: Callable,
        headers: Dict[str, str] = None,
    ) -> None:
        """
        Stream CSV data.

        Args:
            data_generator: Async generator yielding CSV rows
            filename: CSV filename
            send: ASGI send callable
            headers: Additional headers
        """
        import csv
        import io

        async def csv_chunk_generator():
            async for row in data_generator:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(row)
                csv_line = output.getvalue()
                yield csv_line.encode("utf-8")

        download_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }

        if headers:
            download_headers.update(headers)

        await self.stream(
            csv_chunk_generator(),
            send,
            status=200,
            content_type="text/csv; charset=utf-8",
            headers=download_headers,
        )

    def _format_sse_event(self, event: Dict[str, Any]) -> str:
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
                data = json.dumps(data, ensure_ascii=False)
            lines.append(f"data: {data}")

        lines.append("")  # Empty line to end event
        lines.append("")  # Double newline for SSE format

        return "\n".join(lines)

    async def stream_template_chunks(
        self,
        template_generator: AsyncGenerator[str, None],
        send: Callable,
        status: int = 200,
        headers: Dict[str, str] = None,
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
