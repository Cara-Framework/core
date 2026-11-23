"""The serve command's "Runtime & Integrations" startup banner.

Bound back onto ``ServeCommand`` (as ``_QueueWorkRuntime`` is onto the queue
worker): it only reads configuration and prints, shares nothing with the
server process handling, and living here keeps ``ServeCommand.py`` inside the
source-shape line budget.
"""

from __future__ import annotations

import platform
from multiprocessing import cpu_count

from cara.configuration import config


def _serve_show_additional_metrics(self, cfg: dict) -> None:
    """Show extra runtime and integration metrics helpful during development."""
    self.console.print()
    self.console.print("[bold #e5c07b]┌─ Runtime & Integrations[/bold #e5c07b]")

    # Runtime
    py_ver = platform.python_version()
    os_name = platform.system()
    os_rel = platform.release()
    cpu_cores = cpu_count()
    self.console.print(f"[#e5c07b]│[/#e5c07b] [white]Python:[/white] [dim]{py_ver}[/dim]")
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Platform:[/white] [dim]{os_name} {os_rel}[/dim]"
    )
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]CPU Cores:[/white] [dim]{cpu_cores}[/dim]"
    )

    # App integrations
    queue_default = config("queue.default", "unknown")
    db_default = config("database.default", "unknown")
    cache_default = config("cache.default", "unknown")
    bc_default = config("broadcasting.default", "unknown")
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Database:[/white] [dim]{db_default}[/dim]  [white]Queue:[/white] [dim]{queue_default}[/dim]"
    )
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Cache:[/white] [dim]{cache_default}[/dim]   [white]Broadcasting:[/white] [dim]{bc_default}[/dim]"
    )

    # Features
    # Lowercase path — Configuration.load lower-cases module attribute names.
    ws_enabled = bool(config("broadcasting.websocket.enabled", True))
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]WebSocket:[/white] [{'#30e047' if ws_enabled else '#E21102'}]{'✓' if ws_enabled else '×'}[/{'#30e047' if ws_enabled else '#E21102'}]"
    )

    # Logging snapshot
    log_stack = config("logging.default", "daily")
    console_level = config("logging.channels.console.LEVEL", "DEBUG")
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]Log Stack:[/white] [dim]{log_stack}[/dim]  [white]Console Level:[/white] [dim]{console_level}[/dim]"
    )

    # App URL (useful for callbacks)
    app_url = config("app.url", "not set")
    self.console.print(
        f"[#e5c07b]│[/#e5c07b] [white]App URL:[/white] [dim]{app_url}[/dim]"
    )

    self.console.print("[#e5c07b]└─[/#e5c07b]")
