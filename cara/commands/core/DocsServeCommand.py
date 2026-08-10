"""DocsServeCommand: serve the documentation viewer over local HTTP.

Exists because ``python -m http.server`` resolves the cwd at import time for
its argparse default and dies with PermissionError when launched from a
sandboxed or deleted cwd. Here the directory is passed explicitly and the cwd
is never touched.

The default port comes from the product's manifest and has no framework
fallback on purpose: two checkouts on one machine that share a default mean
whichever viewer binds first wins and the second silently serves the OTHER
product's documentation — the most expensive kind of wrong answer.
"""

from __future__ import annotations

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from cara.commands.core.DocsCommand import DocsCommand


class DocsServeCommand(DocsCommand):
    """Serve the documentation viewer (index.html + markdown) over HTTP."""

    name = "maintenance:docs:serve"
    help = "Serve the documentation viewer at http://<host>:<port>"
    _cli_options = [
        {
            "name": "--port",
            "type": int,
            "help": "Port to listen on (default: the product's declared viewer port)",
        },
        {
            "name": "--host",
            "type": str,
            "help": "Bind address (default: 127.0.0.1; use 0.0.0.0 behind a proxy)",
        },
    ]

    async def handle(self, port: int | None = None, host: str | None = None) -> int:
        """Bind the viewer directory and serve it until interrupted."""
        manifest = self._manifest()
        try:
            port_int = int(port) if port is not None else manifest.viewer_port
        except TypeError, ValueError:
            self.line("<error>--port must be an integer</error>")
            return 2
        bind_host = str(host or manifest.viewer_host).strip() or manifest.viewer_host
        docs = manifest.docs
        if not (docs / "index.html").is_file():
            self.line(f"<error>docs viewer missing: {docs / 'index.html'}</error>")
            return 2
        handler = partial(SimpleHTTPRequestHandler, directory=str(docs))
        self.line(f"docs @ http://{bind_host}:{port_int} ({docs}) — Ctrl+C to stop")
        ThreadingHTTPServer((bind_host, port_int), handler).serve_forever()
        return 0
