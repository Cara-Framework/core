"""Optional IPython integration for the Cara tinker shell."""

from __future__ import annotations


class _IPythonShell:
    def __init__(self, owner) -> None:
        self._owner = owner

    def start_ipython(self):
        """Start IPython shell with enhanced autocomplete."""
        try:
            from IPython import embed  # local: heavy optional dep

            # Configure IPython with custom completers
            self._setup_ipython_completers()

            embed(user_ns=self._owner.namespace, colors="neutral")
        except ImportError as e:
            raise ImportError("IPython not available") from e

    def _setup_ipython_completers(self):
        """Setup custom autocompletion for Cara framework."""
        try:
            from IPython import get_ipython  # local: heavy optional dep

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
                    except Exception as e:
                        print(f"[tinker] swallowed completer error for {obj_name}: {e}")

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
        from IPython.core.magic import (  # local: heavy optional dep
            Magics,
            line_magic,
            magics_class,
        )

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
