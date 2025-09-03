"""
Shell - Interactive shell for Cara Tinker

This file provides the interactive shell functionality with Rich integration.
"""

import os
import sys
from typing import Any, Dict

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


class Shell:
    """Interactive shell for Cara Tinker with Rich integration."""

    def __init__(self):
        """Initialize shell with Rich console."""
        self.namespace = {}
        self.console = Console()
        self.setup_namespace()

    def setup_namespace(self):
        """Set up shell namespace with common imports."""
        # Add built-in functions
        self.namespace.update(
            {
                "__builtins__": __builtins__,
                "help": help,
                "dir": dir,
                "type": type,
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
            }
        )

        # Try to import common Cara modules
        self.import_cara_modules()

    def import_cara_modules(self):
        """Import common Cara modules into namespace - Laravel style!"""
        self.console.print("[dim]Loading Cara framework components...[/dim]")

        # 1. Load all Facades (Laravel style)
        self._load_facades()

        # 2. Load Models from app/models
        self._load_models()

        # 3. Load Support classes
        self._load_support_classes()

        # 4. Load ORM/Eloquent classes
        self._load_orm_classes()

        # 5. Load common utilities
        self._load_utilities()

        # 6. Load application instance and helpers
        self._load_application_helpers()

    def _load_facades(self):
        """Load all available facades."""
        facade_modules = [
            (
                "cara.facades",
                [
                    "App",
                    "Auth",
                    "Cache",
                    "Config",
                    "DB",
                    "Log",
                    "Mail",
                    "Queue",
                    "Session",
                    "Storage",
                    "View",
                    "Route",
                    "Request",
                    "Response",
                    "Notification",
                    "Event",
                    "Broadcast",
                    "Hash",
                ],
            ),
        ]

        loaded_facades = []
        for module_path, facade_names in facade_modules:
            try:
                module = __import__(module_path, fromlist=facade_names)

                for facade_name in facade_names:
                    try:
                        facade = getattr(module, facade_name)
                        self.namespace[facade_name] = facade
                        loaded_facades.append(facade_name)
                    except AttributeError:
                        pass

            except ImportError:
                pass

        if loaded_facades:
            self.console.print(
                f"[green]✅ Loaded facades:[/green] {', '.join(loaded_facades)}"
            )

    def _load_models(self):
        """Load all models from app/models directory."""
        from pathlib import Path

        from cara.support import paths

        loaded_models = []

        # Try different model locations
        model_paths = [
            "app.models",
            paths("models"),  # Use paths() helper instead of hardcoded path
            "models",
        ]

        for model_path in model_paths:
            try:
                if "." in model_path:
                    # Python module import
                    module = __import__(model_path, fromlist=[""])

                    for attr_name in dir(module):
                        if not attr_name.startswith("_"):
                            attr = getattr(module, attr_name)
                            if isinstance(attr, type) and hasattr(attr, "__table__"):
                                self.namespace[attr_name] = attr
                                loaded_models.append(attr_name)
                else:
                    # Directory scan
                    model_dir = Path(model_path)
                    if model_dir.exists():
                        for py_file in model_dir.glob("*.py"):
                            if py_file.name.startswith("__"):
                                continue

                            model_name = py_file.stem
                            try:
                                spec = __import__(
                                    f"{model_path.replace('/', '.')}.{model_name}"
                                )
                                model_class = getattr(spec, model_name, None)
                                if model_class and isinstance(model_class, type):
                                    self.namespace[model_name] = model_class
                                    loaded_models.append(model_name)
                            except:
                                pass

            except ImportError:
                continue

        if loaded_models:
            self.console.print(
                f"[green]✅ Loaded models:[/green] {', '.join(loaded_models)}"
            )

    def _load_support_classes(self):
        """Load support classes and utilities."""
        support_modules = [
            ("cara.support", ["Collection", "Str", "Arr", "Carbon"]),
            ("cara.support.helpers", ["collect", "str_", "arr"]),
        ]

        loaded_support = []
        for module_path, class_names in support_modules:
            try:
                module = __import__(module_path, fromlist=class_names)

                for class_name in class_names:
                    try:
                        cls = getattr(module, class_name)
                        self.namespace[class_name] = cls
                        loaded_support.append(class_name)
                    except AttributeError:
                        pass

            except ImportError:
                pass

        if loaded_support:
            self.console.print(
                f"[green]✅ Loaded support:[/green] {', '.join(loaded_support)}"
            )

    def _load_orm_classes(self):
        """Load ORM/Eloquent classes."""
        orm_modules = [
            ("cara.orm", ["Model", "Builder", "Query"]),
            ("cara.eloquent", ["Model", "Builder", "Collection"]),
            ("cara.database", ["Schema", "Migration"]),
        ]

        loaded_orm = []
        for module_path, class_names in orm_modules:
            try:
                module = __import__(module_path, fromlist=class_names)

                for class_name in class_names:
                    try:
                        cls = getattr(module, class_name)
                        self.namespace[class_name] = cls
                        loaded_orm.append(class_name)
                    except AttributeError:
                        pass

            except ImportError:
                pass

        if loaded_orm:
            self.console.print(f"[green]✅ Loaded ORM:[/green] {', '.join(loaded_orm)}")

    def _load_utilities(self):
        """Load common utilities and helpers."""
        # Add common Python modules that are useful in development
        common_modules = {
            "json": "json",
            "os": "os",
            "sys": "sys",
            "datetime": "datetime",
            "time": "time",
            "re": "re",
            "uuid": "uuid",
            "random": "random",
            "math": "math",
        }

        loaded_utils = []
        for alias, module_name in common_modules.items():
            try:
                module = __import__(module_name)
                self.namespace[alias] = module
                loaded_utils.append(alias)
            except ImportError:
                pass

        if loaded_utils:
            self.console.print(
                f"[green]✅ Loaded utilities:[/green] {', '.join(loaded_utils)}"
            )

    def _load_application_helpers(self):
        """Load application instance and helper functions."""
        helpers = {}

        # Try to get application instance
        try:
            from cara.foundation import Application

            # This would be the actual app instance in a real scenario
            # helpers["app"] = Application.getInstance()
            helpers["Application"] = Application
        except ImportError:
            pass

        # Add Laravel-style helper functions
        def app(service_name=None):
            """Get application instance or resolve service."""
            try:
                # Try to get from bootstrap first
                from bootstrap import application

                if service_name:
                    return application.make(service_name)
                return application
            except ImportError:
                try:
                    # Check if app() is already available in builtins (from SupportProvider)
                    import builtins

                    if hasattr(builtins, "app"):
                        app_instance = builtins.app()
                        if service_name:
                            return app_instance.make(service_name)
                        return app_instance
                except:
                    pass
                return None

        def config(key, default=None):
            """Get configuration value."""
            try:
                from cara.facades import Config

                return Config.get(key, default)
            except:
                return default

        def env(key, default=None):
            """Get environment variable."""
            import os

            return os.getenv(key, default)

        def collect(items=None):
            """Create a collection."""
            try:
                from cara.support import Collection

                return Collection(items or [])
            except:
                return items or []

        def cache(key=None, value=None, ttl=None):
            """Cache helper function."""
            try:
                from cara.facades import Cache

                if value is not None:
                    return Cache.put(key, value, ttl)
                elif key is not None:
                    return Cache.get(key)
                else:
                    return Cache
            except:
                return None

        def route(name, parameters=None):
            """Generate route URL."""
            try:
                from cara.facades import Route

                return Route.url(name, parameters or {})
            except:
                return f"/{name}"

        # Add helpers to namespace
        # Check if app() is already available in builtins (from SupportProvider)
        import builtins

        if hasattr(builtins, "app"):
            helpers["app"] = builtins.app
        else:
            helpers["app"] = app

        helpers.update(
            {
                "config": config,
                "env": env,
                "collect": collect,
                "cache": cache,
                "route": route,
            }
        )

        self.namespace.update(helpers)
        self.console.print(
            f"[green]✅ Loaded helpers:[/green] {', '.join(helpers.keys())}"
        )

        # Add some utility functions
        self.namespace.update(
            {
                "dd": self.dd,
                "dump": self.dump,
                "info": self.info,
                "clear": self.clear_screen,
                "exit": self.exit_shell,
                "quit": self.exit_shell,
            }
        )

    def dd(self, *args):
        """Dump and die - print variables and exit with Rich formatting."""
        for arg in args:
            self.dump(arg)
        sys.exit(0)

    def dump(self, obj):
        """Dump variable in a beautiful Rich format."""
        self.console.print(obj, style="bold cyan")

    def info(self, obj=None):
        """Show information about object or available functions with Rich formatting."""
        if obj is None:
            table = Table(
                title="Available Functions and Classes",
                show_header=True,
                header_style="bold magenta",
            )
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Type", style="green")

            for name, value in sorted(self.namespace.items()):
                if not name.startswith("_"):
                    table.add_row(name, type(value).__name__)

            self.console.print(table)
        else:
            panel_content = []
            panel_content.append(f"[bold cyan]Type:[/bold cyan] {type(obj).__name__}")
            panel_content.append(f"[bold cyan]Value:[/bold cyan] {repr(obj)}")

            if hasattr(obj, "__doc__") and obj.__doc__:
                panel_content.append(
                    f"[bold cyan]Documentation:[/bold cyan] {obj.__doc__}"
                )

            # Show first 10 attributes
            attrs = [attr for attr in dir(obj) if not attr.startswith("_")][:10]
            if attrs:
                panel_content.append(
                    f"[bold cyan]Attributes:[/bold cyan] {', '.join(attrs)}"
                )
                if len(dir(obj)) > 10:
                    panel_content.append(f"[dim]... and {len(dir(obj)) - 10} more[/dim]")

            self.console.print(
                Panel(
                    "\n".join(panel_content),
                    title=f"Object Info: {type(obj).__name__}",
                    border_style="blue",
                )
            )

    def clear_screen(self):
        """Clear the screen."""
        os.system("cls" if os.name == "nt" else "clear")

    def exit_shell(self):
        """Exit the shell with Rich goodbye message."""
        self.console.print("👋 [bold green]Goodbye![/bold green]")
        sys.exit(0)

    def start(self, use_ipython: bool = True):
        """Start interactive shell."""
        if not os.environ.get("CARA_TINKER_QUIET"):
            self.print_banner()

        if use_ipython:
            try:
                self.start_ipython()
                return
            except ImportError:
                print("⚠️  IPython not available, falling back to basic Python shell")

        self.start_basic_shell()

    def print_banner(self):
        """Print beautiful welcome banner with Rich."""
        banner_text = Text()
        banner_text.append("🔧 ", style="bold yellow")
        banner_text.append("Cara Tinker", style="bold blue")

        panel = Panel.fit(
            "[bold green]Laravel-style interactive shell for Cara framework[/bold green]\n\n"
            "[cyan]🚀 Framework Features Available:[/cyan]\n"
            "• [bold]Facades:[/bold] Auth, DB, Cache, Config, Mail, Queue, etc.\n"
            "• [bold]Models:[/bold] Your app models auto-loaded\n"
            "• [bold]Helpers:[/bold] app(), config(), env(), collect(), cache(), route()\n"
            "• [bold]ORM:[/bold] Model, Builder, Query classes\n"
            "• [bold]Support:[/bold] Collection, Str, Arr utilities\n\n"
            "[cyan]💡 Tinker Commands:[/cyan]\n"
            "• [bold]help()[/bold] - Show Python help\n"
            "• [bold]info()[/bold] - Show available functions and classes\n"
            "• [bold]info(obj)[/bold] - Show object information\n"
            "• [bold]dump(obj)[/bold] - Pretty print object\n"
            "• [bold]dd(obj)[/bold] - Dump and die\n"
            "• [bold]clear()[/bold] - Clear screen\n"
            "• [bold]exit()[/bold] or [bold]quit()[/bold] - Exit shell\n\n"
            "[yellow]💡 Example usage:[/yellow]\n"
            "[dim]>>> User.all()  # Get all users\n"
            ">>> Auth.user()  # Get current user\n"
            ">>> config('app.name')  # Get config value\n"
            ">>> collect([1,2,3]).map(lambda x: x*2)  # Collections[/dim]\n\n"
            "[cyan]🎯 Magic Commands:[/cyan]\n"
            "[dim]>>> %models  # List all models\n"
            ">>> %facades  # List all facades\n"
            ">>> %helpers  # List all helpers[/dim]",
            title=banner_text,
            border_style="blue",
            padding=(1, 2),
        )

        self.console.print(panel)

    def start_ipython(self):
        """Start IPython shell with enhanced autocomplete."""
        try:
            from IPython import embed
            from IPython.terminal.interactiveshell import TerminalInteractiveShell

            # Configure IPython with custom completers
            self._setup_ipython_completers()

            embed(user_ns=self.namespace, colors="neutral")
        except ImportError:
            raise ImportError("IPython not available")

    def _setup_ipython_completers(self):
        """Setup custom autocompletion for Cara framework."""
        try:
            from IPython import get_ipython
            from IPython.core.completer import IPCompleter

            # Get IPython instance
            ip = get_ipython()
            if ip is None:
                return

            # Enable better tab completion
            ip.Completer.use_jedi = True
            ip.Completer.greedy = True

            # Add custom attribute completer for facades and models
            original_attr_matches = ip.Completer.attr_matches

            def enhanced_attr_matches(self, text):
                """Enhanced attribute matching for Cara objects."""
                matches = original_attr_matches(text)

                # Add Cara-specific completions
                if "." in text:
                    obj_name, attr_prefix = text.rsplit(".", 1)

                    # Facade method completions
                    facade_completions = {
                        "Auth": [
                            "user",
                            "check",
                            "guest",
                            "id",
                            "login",
                            "logout",
                            "attempt",
                            "once",
                            "loginUsingId",
                        ],
                        "DB": [
                            "table",
                            "select",
                            "insert",
                            "update",
                            "delete",
                            "raw",
                            "transaction",
                            "beginTransaction",
                            "commit",
                            "rollback",
                        ],
                        "Cache": [
                            "get",
                            "put",
                            "forget",
                            "flush",
                            "remember",
                            "forever",
                            "increment",
                            "decrement",
                            "pull",
                        ],
                        "Config": [
                            "get",
                            "set",
                            "has",
                            "all",
                            "forget",
                            "push",
                            "prepend",
                        ],
                        "Mail": ["send", "queue", "later", "raw", "plain"],
                        "Queue": ["push", "later", "bulk", "pushOn", "laterOn"],
                        "Storage": [
                            "disk",
                            "get",
                            "put",
                            "delete",
                            "exists",
                            "size",
                            "lastModified",
                            "copy",
                            "move",
                        ],
                        "View": [
                            "make",
                            "share",
                            "composer",
                            "creator",
                            "exists",
                            "file",
                            "first",
                        ],
                        "Session": [
                            "get",
                            "put",
                            "push",
                            "flash",
                            "forget",
                            "flush",
                            "regenerate",
                            "invalidate",
                        ],
                        "Request": [
                            "all",
                            "input",
                            "get",
                            "post",
                            "query",
                            "file",
                            "hasFile",
                            "header",
                            "ip",
                            "userAgent",
                        ],
                        "Response": [
                            "make",
                            "json",
                            "jsonp",
                            "stream",
                            "download",
                            "file",
                            "redirectTo",
                            "redirectToRoute",
                        ],
                    }

                    if obj_name in facade_completions:
                        cara_matches = [
                            f"{obj_name}.{method}"
                            for method in facade_completions[obj_name]
                            if method.startswith(attr_prefix)
                        ]
                        matches.extend(cara_matches)

                    # Model method completions for any model
                    model_methods = [
                        "all",
                        "find",
                        "first",
                        "get",
                        "create",
                        "update",
                        "delete",
                        "destroy",
                        "where",
                        "orWhere",
                        "whereIn",
                        "whereNotIn",
                        "whereBetween",
                        "whereNull",
                        "whereNotNull",
                        "orderBy",
                        "orderByDesc",
                        "groupBy",
                        "having",
                        "limit",
                        "offset",
                        "skip",
                        "take",
                        "count",
                        "sum",
                        "avg",
                        "min",
                        "max",
                        "exists",
                        "doesntExist",
                        "with",
                        "withCount",
                        "has",
                        "doesntHave",
                        "whereHas",
                        "whereDoesntHave",
                        "join",
                        "leftJoin",
                        "rightJoin",
                        "crossJoin",
                        "union",
                        "unionAll",
                        "distinct",
                        "select",
                        "addSelect",
                    ]

                    # Check if it's a model (has __table__ attribute)
                    try:
                        obj = eval(obj_name, ip.user_ns)
                        if hasattr(obj, "__table__") or (
                            hasattr(obj, "__name__") and obj.__name__ in ["User", "Post"]
                        ):
                            model_matches = [
                                f"{obj_name}.{method}"
                                for method in model_methods
                                if method.startswith(attr_prefix)
                            ]
                            matches.extend(model_matches)
                    except:
                        pass

                return matches

            # Replace the original method
            ip.Completer.attr_matches = enhanced_attr_matches.__get__(
                ip.Completer, ip.Completer.__class__
            )

            # Register magic commands for better UX
            self._register_magic_commands(ip)

        except ImportError:
            pass

    def _cara_completer(self, self_obj, event):
        """Custom completer for Cara framework objects."""
        completions = []

        # Get the current line and cursor position
        line = event.line
        text_until_cursor = event.text_until_cursor

        # Facade completions
        if any(
            facade in text_until_cursor
            for facade in ["Auth.", "DB.", "Cache.", "Config."]
        ):
            facade_methods = {
                "Auth.": ["user", "check", "guest", "id", "login", "logout", "attempt"],
                "DB.": [
                    "table",
                    "select",
                    "insert",
                    "update",
                    "delete",
                    "raw",
                    "transaction",
                ],
                "Cache.": ["get", "put", "forget", "flush", "remember", "forever"],
                "Config.": ["get", "set", "has", "all"],
                "Mail.": ["send", "queue", "later"],
                "Queue.": ["push", "later", "bulk"],
                "Storage.": ["disk", "get", "put", "delete", "exists"],
                "View.": ["make", "share", "composer"],
            }

            for facade, methods in facade_methods.items():
                if facade in text_until_cursor:
                    completions.extend(methods)

        # Model method completions
        model_methods = [
            "all",
            "find",
            "first",
            "get",
            "create",
            "update",
            "delete",
            "where",
            "orWhere",
            "whereIn",
            "whereNotIn",
            "whereBetween",
            "orderBy",
            "groupBy",
            "having",
            "limit",
            "offset",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "exists",
            "doesntExist",
        ]

        if any(model in text_until_cursor for model in ["User.", "Post."]):
            completions.extend(model_methods)

        # Collection method completions
        collection_methods = [
            "map",
            "filter",
            "reduce",
            "each",
            "pluck",
            "sort",
            "sortBy",
            "reverse",
            "shuffle",
            "chunk",
            "split",
            "take",
            "skip",
            "first",
            "last",
            "count",
            "isEmpty",
            "isNotEmpty",
            "contains",
        ]

        if "collect(" in text_until_cursor or ".map(" in text_until_cursor:
            completions.extend(collection_methods)

        return completions

    def _register_magic_commands(self, ip):
        """Register custom magic commands for Cara."""
        from IPython.core.magic import Magics, line_magic, magics_class

        @magics_class
        class CaraMagics(Magics):
            @line_magic
            def models(self, line):
                """List all available models."""
                models = [
                    name
                    for name, obj in ip.user_ns.items()
                    if isinstance(obj, type) and hasattr(obj, "__table__")
                ]

                if models:
                    print("📦 Available Models:")
                    for model in sorted(models):
                        print(f"  • {model}")
                else:
                    print("No models found")

            @line_magic
            def facades(self, line):
                """List all available facades."""
                facades = [
                    name
                    for name, obj in ip.user_ns.items()
                    if hasattr(obj, "key") and isinstance(getattr(obj, "key", None), str)
                ]

                if facades:
                    print("🎭 Available Facades:")
                    for facade in sorted(facades):
                        print(f"  • {facade}")
                else:
                    print("No facades found")

            @line_magic
            def helpers(self, line):
                """List all available helper functions."""
                helpers = ["app", "config", "env", "collect", "cache", "route"]
                print("🛠️  Available Helpers:")
                for helper in helpers:
                    if helper in ip.user_ns:
                        func = ip.user_ns[helper]
                        doc = getattr(func, "__doc__", "No documentation")
                        print(f"  • {helper}() - {doc}")

        # Register the magic commands
        ip.register_magic_function(CaraMagics(ip).models, "line", "models")
        ip.register_magic_function(CaraMagics(ip).facades, "line", "facades")
        ip.register_magic_function(CaraMagics(ip).helpers, "line", "helpers")

    def start_basic_shell(self):
        """Start basic Python shell."""
        import code

        # Create console
        console = code.InteractiveConsole(locals=self.namespace)

        # Start interactive session
        try:
            console.interact()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Goodbye!")

    def execute_command(self, command: str):
        """Execute a single command."""
        try:
            # Try to evaluate as expression first
            try:
                result = eval(command, self.namespace)
                if result is not None:
                    print(repr(result))
                return result
            except SyntaxError:
                # If it's not an expression, execute as statement
                exec(command, self.namespace)
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def add_to_namespace(self, name: str, value: Any):
        """Add variable to namespace."""
        self.namespace[name] = value

    def get_namespace(self) -> Dict[str, Any]:
        """Get current namespace."""
        return self.namespace.copy()

    def update_namespace(self, updates: Dict[str, Any]):
        """Update namespace with new variables."""
        self.namespace.update(updates)
