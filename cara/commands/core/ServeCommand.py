"""
Development Server Command for the Cara framework.

This module provides a CLI command to start the development server with enhanced UX.
"""

import os
import platform
import subprocess
import sys
import time
from multiprocessing import cpu_count
from typing import Optional

from cara.commands import CommandBase
from cara.configuration import config
from cara.decorators import command
from cara.support.LogColors import LogColors


@command(
    name="serve",
    help="Start the development server with enhanced configuration options.",
    options={
        "--host=?": "Server host address (default: 127.0.0.1)",
        "--port=?": "Server port number (default: 8000)",
        "--reload": "Enable auto-reload on file changes",
        "--debug": "Enable debug mode",
        "--workers=?": "Number of worker processes (default: 1)",
    },
)
class ServeCommand(CommandBase):
    """Start development server with enhanced configuration and monitoring."""

    def __init__(self, application=None):
        super().__init__(application)
        self.log_colors = LogColors()

    def handle(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        workers: Optional[str] = None,
    ):
        """Handle development server startup with enhanced options."""
        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Development Server ─╮[/bold #e5c07b]")
        self.console.print()

        # Prepare server configuration
        try:
            server_config = self._prepare_server_config(host, port, workers)
        except ValueError as e:
            self.error(f"× Configuration error: {e}")
            return

        # Show configuration
        self._show_server_config(server_config)

        # Show routes
        self._show_routes_compact()

        # Start server
        try:
            self._start_server(server_config)
        except Exception as e:
            self.error(f"× Server error: {e}")

    def _prepare_server_config(
        self, host: Optional[str], port: Optional[str], workers: Optional[str]
    ) -> dict:
        """Prepare and validate server configuration."""
        # Get host
        server_host = host or config("server.host", "127.0.0.1")

        # Get and validate port
        server_port = self._parse_port(port)

        # Get and validate workers
        worker_count = self._parse_workers(workers)

        # Get other settings
        reload_enabled = self.option("reload") or config("app.debug", False)
        debug_enabled = self.option("debug") or config("app.debug", False)

        return {
            "host": server_host,
            "port": server_port,
            "workers": worker_count,
            "reload": reload_enabled,
            "debug": debug_enabled,
            "app_name": config("app.name", "Cara Application"),
            "app_env": config("app.env", "local"),
        }

    def _parse_port(self, port: Optional[str]) -> int:
        """Parse and validate port number."""
        if port:
            try:
                port_num = int(port)
                if port_num < 1 or port_num > 65535:
                    raise ValueError("Port must be between 1 and 65535")
                return port_num
            except ValueError as e:
                raise ValueError(f"Invalid port number: {e}")

        return config("server.port", 8000)

    def _parse_workers(self, workers: Optional[str]) -> int:
        """Parse and validate worker count."""
        if workers:
            try:
                worker_count = int(workers)
                if worker_count < 1:
                    raise ValueError("Worker count must be at least 1")
                if worker_count > 10:
                    self.warning("⚠ High worker count may impact performance")
                return worker_count
            except ValueError as e:
                raise ValueError(f"Invalid worker count: {e}")

        return 1

    def _show_server_config(self, config: dict) -> None:
        """Display server configuration."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Application:[/white] [dim]{config['app_name']}[/dim]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Environment:[/white] [dim]{config['app_env']}[/dim]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Host:[/white] [bold white]{config['host']}[/bold white]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Port:[/white] [bold white]{config['port']}[/bold white]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Workers:[/white] [dim]{config['workers']}[/dim]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Debug Mode:[/white] [{'#30e047' if config['debug'] else '#E21102'}]{'✓' if config['debug'] else '×'}[/{'#30e047' if config['debug'] else '#E21102'}]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if config['reload'] else '#E21102'}]{'✓' if config['reload'] else '×'}[/{'#30e047' if config['reload'] else '#E21102'}]"
        )
        self.console.print("[#e5c07b]└─[/#e5c07b]")

        # Show loaded environment files
        self._show_environment_files()

        # Show additional runtime and integration metrics
        self._show_additional_metrics(config)

        # Show URLs
        self._show_server_urls(config)

    def _show_server_urls(self, config: dict) -> None:
        """Show server access URLs."""
        host = config["host"]
        port = config["port"]

        self.console.print()
        self.console.print("[bold #e5c07b]┌─ Server URLs[/bold #e5c07b]")

        # Local URL
        if host in ["127.0.0.1", "localhost"]:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Local:[/white]   [bold white]http://127.0.0.1:{port}[/bold white]"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Local:[/white]   [bold white]http://localhost:{port}[/bold white]"
            )
        else:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Server:[/white]  [bold white]http://{host}:{port}[/bold white]"
            )

        # Network URL (if not localhost)
        if host == "0.0.0.0":
            try:
                import socket

                hostname = socket.gethostname()
                local_ip = socket.gethostbyname(hostname)
                self.console.print(
                    f"[#e5c07b]│[/#e5c07b] [white]Network:[/white] [bold white]http://{local_ip}:{port}[/bold white]"
                )
            except:
                pass

        self.console.print("[#e5c07b]└─[/#e5c07b]")

    def _show_environment_files(self) -> None:
        """Show loaded environment files."""
        from cara.environment.Environment import LoadEnvironment

        if LoadEnvironment.loaded_files:
            self.console.print()
            self.console.print("[bold #e5c07b]┌─ Environment Files[/bold #e5c07b]")
            for env_file in LoadEnvironment.loaded_files:
                file_name = env_file.split("/")[-1]  # Get just the filename
                self.console.print(
                    f"[#e5c07b]│[/#e5c07b] [white]Loaded:[/white] [dim]{file_name}[/dim]"
                )
            self.console.print("[#e5c07b]└─[/#e5c07b]")

    def _start_server(self, config: dict) -> None:
        """Start the development server."""
        self.console.print()
        self.console.print("[bold #e5c07b]┌─ Starting Server[/bold #e5c07b]")
        self.console.print("[#e5c07b]│[/#e5c07b] [dim]Initializing...[/dim]")

        # Build server command
        cmd = self._build_server_command(config)

        # Show startup message
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [#30e047]✓ Server running at[/#30e047] [bold white]http://{config['host']}:{config['port']}[/bold white]"
        )
        self.console.print(
            "[#e5c07b]│[/#e5c07b] [white]Press[/white] [bold]Ctrl+C[/bold] [white]to stop the server[/white]"
        )

        if config["reload"]:
            self.console.print(
                "[#e5c07b]│[/#e5c07b] [dim]Auto-reload enabled - server will restart on file changes[/dim]"
            )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

        # Start server process
        try:
            # Capture subprocess output so we can colorize it
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
            )

            # Monitor process with output capture for colorization
            self._monitor_server_process(process, config)

        except FileNotFoundError:
            self.error(
                "× Server executable not found. Make sure your WSGI server is installed."
            )
        except Exception as e:
            self.error(f"× Failed to start server: {e}")

    def _show_routes_compact(self) -> None:
        """Show registered routes in compact format."""
        try:
            router = self.application.make("router")
            routes = list(router.routes)

            if not routes:
                return

            self.console.print()
            self.console.print("[bold #e5c07b]┌─ Routes[/bold #e5c07b]")

            for route in routes:  # Show all routes
                methods = "/".join(sorted(m.upper() for m in route.request_method))
                uri = route.url
                name = route.get_name() or "—"

                self.console.print(
                    f"[#e5c07b]│[/#e5c07b] [cyan]{methods:<8}[/cyan] [white]{uri:<40}[/white] [dim]{name}[/dim]"
                )

            self.console.print("[#e5c07b]└─[/#e5c07b]")

        except Exception:
            # Silently ignore route listing errors
            pass

    def _build_server_command(self, config: dict) -> list:
        """Build the server command based on configuration."""
        # Use virtual environment python if available, fallback to system python

        venv_python = os.path.join(os.getcwd(), "venv", "bin", "python")
        python_executable = venv_python if os.path.exists(venv_python) else sys.executable

        cmd = [
            python_executable,
            "-u",  # Unbuffered output for immediate print visibility
            "-m",
            "uvicorn",
            "bootstrap:application",  # Use bootstrap.py where application is defined
            "--host",
            config["host"],
            "--port",
            str(config["port"]),
            "--no-use-colors",  # Disable uvicorn's built-in colors
            "--no-access-log",  # Disable uvicorn's HTTP access logging
        ]

        if config["reload"]:
            cmd.append("--reload")

        if config["workers"] > 1:
            cmd.extend(["--workers", str(config["workers"])])

        return cmd

    def _show_additional_metrics(self, cfg: dict) -> None:
        """Show extra runtime and integration metrics helpful during development."""
        self.console.print()
        self.console.print("[bold #e5c07b]┌─ Runtime & Integrations[/bold #e5c07b]")

        # Runtime
        py_ver = platform.python_version()
        os_name = platform.system()
        os_rel = platform.release()
        cpu_cores = cpu_count()
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Python:[/white] [dim]{py_ver}[/dim]"
        )
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
        ws_enabled = bool(config("broadcasting.WEBSOCKET.enabled", True))
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

    def _monitor_server_process(self, process: subprocess.Popen, config: dict) -> None:
        """Monitor server process output and handle shutdown."""
        try:
            start_time = time.time()

            while True:
                output = process.stdout.readline()
                if output:
                    line = output.strip()
                    if line:
                        # Colorize and print the line
                        colorized_line = self.log_colors.colorize_line(line)
                        print(colorized_line)

                # Check if process is still running
                if process.poll() is not None:
                    break

                time.sleep(0.1)

        except KeyboardInterrupt:
            self.console.print()
            self.console.print("[bold #e5c07b]┌─ Shutting Down[/bold #e5c07b]")
            self.console.print(
                "[#e5c07b]│[/#e5c07b] [dim]Gracefully stopping server...[/dim]"
            )

            # Graceful shutdown
            process.terminate()
            try:
                process.wait(timeout=5)
                self.console.print(
                    "[#e5c07b]│[/#e5c07b] [#30e047]✓ Server stopped gracefully[/#30e047]"
                )
            except subprocess.TimeoutExpired:
                self.console.print(
                    "[#e5c07b]│[/#e5c07b] [#E21102]⚠ Force killing server process...[/#E21102]"
                )
                process.kill()

            runtime = time.time() - start_time
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]Runtime:[/white] [dim]{runtime:.1f} seconds[/dim]"
            )
            self.console.print("[#e5c07b]└─[/#e5c07b]")
            self.console.print()
