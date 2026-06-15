"""
Command Runner for the Cara framework.

Provides utilities for registering CLI commands and executing them with Typer.
Prints full traceback on errors for easier debugging.
"""

from __future__ import annotations

import asyncio
import inspect
import traceback
from typing import Any

import typer
from rich import print as rprint

from cara.decorators import _run_after, _run_before, _run_on_error


class CommandRunner:
    """Handles Typer command registration and execution, including hooks and full-traceback on errors."""

    def __init__(self, application: Any):
        self.application = application
        self.console_app = typer.Typer(help="Cara CLI")

    def run(self):
        """Run the Typer application."""
        self.console_app()

    def register(self, cmd_cls: type[Any]):
        """
        Register a command class with Typer, building its signature from handle()
        and decorator options, and wiring before/after/error hooks.
        """
        name = getattr(cmd_cls, "name", None)
        if not name:
            return
        help_text = getattr(cmd_cls, "help", "")

        # 1) Inspect handle signature. Pass the underlying function too
        # so ``_split_handle_params`` can resolve PEP 563 string annotations
        # via ``typing.get_type_hints(cmd_cls.handle)`` — without it,
        # commands using ``from __future__ import annotations`` get every
        # primitive misclassified as a DI dep (see _split_handle_params).
        sig = inspect.signature(cmd_cls.handle)
        cli_params, di_params = self._split_handle_params(sig, cmd_cls.handle)

        # 2) Parse decorator options
        raw_options = getattr(cmd_cls, "_cli_options", {}) or {}
        parsed_options = self._parse_decorator_options(raw_options)

        # 3) Build Typer signature parameters
        parameters = self._build_signature_parameters(cli_params, parsed_options)

        # 4) Create callback
        callback = self._make_callback(cmd_cls, name, di_params)

        # 5) Attach the built signature and register with Typer
        callback.__signature__ = inspect.Signature(parameters)
        self.console_app.command(name=name, help=help_text)(callback)

    def _split_handle_params(
        self, sig: inspect.Signature, handle_fn: Any = None
    ) -> tuple[list[inspect.Parameter], list[inspect.Parameter]]:
        """
        Split handle() parameters into CLI parameters (primitive types or no annotation)
        and DI parameters (other annotated types).

        ROOT CAUSE FIX (scenario 8 / AI batch enrichment):
        Commands that import ``from __future__ import annotations`` (PEP 563)
        return their parameter annotations as raw STRINGS rather than evaluated
        types. The original tuple-membership check ``ann in (str, int, float, bool)``
        therefore failed for every PEP 563 command — every primitive parameter
        ended up in di_params, cli_params came back empty, and Typer rebuilt
        the CLI from decorator options alone. For ``ai:flush --sync`` that
        meant ``priority`` (declared ``"--priority"`` without ``=default``)
        was registered as a bool flag with default ``False``, so the handle
        body's ``if priority not in VALID_PRIORITIES`` rejected it as
        ``"Invalid priority 'False'"`` on every invocation. Resolving the
        hints via ``typing.get_type_hints`` (with the function's own
        module globals) restores the original intent across every command
        that uses PEP 563 — AIDiscover, DevReset, QueuePurge, etc.
        """
        primitive_types = (str, int, float, bool)
        # Resolve PEP 563 string annotations to real types. Use the
        # original function so get_type_hints can resolve forward refs
        # against the right module globals/locals. Falls back to the raw
        # signature on any failure so a single bad annotation can't take
        # the whole CLI out.
        resolved_hints: dict[str, Any] = {}
        target_fn = handle_fn
        if target_fn is None:
            # Best-effort recovery for callers that didn't pass the fn.
            target_fn = getattr(sig, "__wrapped__", None)
        if target_fn is not None:
            try:
                from typing import get_type_hints as _get_type_hints

                resolved_hints = _get_type_hints(target_fn) or {}
            except Exception:
                resolved_hints = {}
        cli_params: list[inspect.Parameter] = []
        di_params: list[inspect.Parameter] = []
        for param in sig.parameters.values():
            if param.name == "self":
                continue
            ann = param.annotation
            # Prefer the resolved (real-type) hint; fall back to the raw
            # string-or-class annotation from the signature.
            if param.name in resolved_hints:
                ann = resolved_hints[param.name]
            # Optional[T] / Union[T, None] should follow T's classification
            # so ``marketplace: Optional[str] = None`` lands on the CLI side.
            try:
                from typing import Union, get_args, get_origin

                if get_origin(ann) is Union:
                    non_none = [a for a in get_args(ann) if a is not type(None)]
                    if len(non_none) == 1:
                        ann = non_none[0]
            except (OSError, RuntimeError, AttributeError, ConnectionError):
                pass
            if ann is inspect.Parameter.empty or ann in primitive_types:
                # Replace the parameter's raw (PEP 563 string) annotation
                # with the resolved type so downstream Typer/Click receive
                # a real class — typer.get_click_type raises
                # "Type not yet supported: bool" if it sees the string
                # ``"bool"`` instead of the type ``bool``.
                if param.name in resolved_hints and ann is not param.annotation:
                    param = param.replace(annotation=ann)
                cli_params.append(param)
            else:
                if param.name in resolved_hints and ann is not param.annotation:
                    param = param.replace(annotation=ann)
                di_params.append(param)
        return cli_params, di_params

    def _parse_decorator_options(
        self, raw_options
    ) -> list[tuple[str, list[str], Any, str, type]]:
        """
        Parse decorator options into a list of tuples:
        (param_name, flags_list, default_value, help_text, annotation).

        Accepts either dict format: {"--flag=default": "help text"}
        or list format with rich metadata:
            [{"name": "--flag", "help": "...", "type": int, "default": 5}]
            [{"name": "--flag", "help": "...", "is_flag": True}]
            [{"name": "--flag=24", "help": "...", "type": "integer"}]

        ROOT CAUSE FIX (scenario 12 / full pipeline integration):
        Pre-fix, the list-format ingestion path collapsed every option to
        ``{name: help}`` and threw away ``type``, ``default``, and
        ``is_flag``. Anything declared without ``=default`` in the name
        string therefore landed in the legacy "bare flag → bool" branch
        below, so e.g. ``DeduplicateCommand``'s ``{"name": "--container",
        "type": int}`` was registered as a Typer bool flag and Click
        rejected ``--container=170`` with "Option '--container' does not
        take a value." Same silent damage for ``RefreshProductsCommand``
        (``--limit/--min-age-hours/--marketplace/--zipcode``),
        ``WishlistDropSweepCommand`` (``--batch-size/--max-batches/
        --threshold``), ``PipelineTraceCommand`` (``--listing/--asin/
        --min-age/--stage/--minutes``), and ``PriceAlertSweepCommand``
        (``--batch-size/--max-batches/--product``) — every typed,
        value-bearing option without an inline default registered as a
        bool flag and rejected the value at the CLI. The fix preserves
        list-format metadata end-to-end and propagates the resolved
        annotation through ``_build_signature_parameters`` so Typer/Click
        emit the right click_type for each option.
        """
        # Normalize input → list of per-option dicts so we keep the
        # rich metadata (``type``, ``default``, ``is_flag``) instead of
        # collapsing to ``{name: help}``.
        items: list[dict[str, Any]] = []
        if isinstance(raw_options, list):
            for item in raw_options:
                if isinstance(item, dict) and item.get("name"):
                    items.append(dict(item))
        elif isinstance(raw_options, dict):
            for k, v in raw_options.items():
                items.append({"name": k, "help": v})
        else:
            return []

        # Map ``"integer"``/``"string"``/``"bool"`` to real types so the
        # legacy string spellings still work alongside ``type=int``.
        _TYPE_ALIASES: dict[str, type] = {
            "int": int,
            "integer": int,
            "str": str,
            "string": str,
            "text": str,
            "float": float,
            "double": float,
            "number": float,
            "bool": bool,
            "boolean": bool,
            "flag": bool,
        }

        _SENTINEL = object()
        parsed: list[tuple[str, list[str], Any, str, type]] = []
        for item in items:
            key = item.get("name", "")
            desc = item.get("help", "") or ""
            explicit_type = item.get("type")
            if isinstance(explicit_type, str):
                explicit_type = _TYPE_ALIASES.get(explicit_type.strip().lower(), str)
            explicit_default = item.get("default", _SENTINEL)
            is_flag = bool(item.get("is_flag", False))

            if "=" in key:
                flags_part, default_str = key.split("=", 1)
                if default_str == "?":
                    inline_default: Any = None
                else:
                    inline_default = default_str
                has_inline = True
            else:
                flags_part = key
                inline_default = None
                has_inline = False

            # Resolve final annotation + default using explicit metadata
            # first, falling back to inline ``=default`` parsing for
            # legacy dict-format keys.
            if is_flag:
                ann: type = bool
                if explicit_default is not _SENTINEL:
                    final_default: Any = bool(explicit_default)
                else:
                    final_default = False
            elif explicit_type is not None:
                ann = explicit_type
                if explicit_default is not _SENTINEL:
                    final_default = explicit_default
                elif has_inline:
                    try:
                        final_default = (
                            ann(inline_default) if inline_default is not None else None
                        )
                    except Exception:
                        final_default = inline_default
                else:
                    # Typed value option with no default → expose as
                    # ``Optional[T]`` (None) so the CLI accepts a value
                    # and the command can detect "not provided" cleanly.
                    final_default = None
            elif explicit_default is not _SENTINEL:
                ann = type(explicit_default) if explicit_default is not None else str
                final_default = explicit_default
            elif has_inline:
                # Legacy dict-format: ``"--flag=24"`` keeps the string
                # default but pre-fix consumers expect strings here, so
                # don't auto-cast to int even if it looks numeric.
                ann = type(inline_default) if inline_default is not None else str
                final_default = inline_default
            else:
                # Bare flag (no type, no default, no inline) — preserve
                # the legacy "implicit bool" behaviour so commands like
                # ``QueueMonitorCommand --queue`` (intended as a string
                # option) keep working when handle() supplies the real
                # annotation. handle()-bound CLI params override the
                # decorator default in ``_build_signature_parameters``,
                # so this only affects decorator-only options.
                ann = bool
                final_default = False

            flag_tokens = flags_part.split("|")
            flags: list[str] = []
            param_name: str | None = None
            for tok in flag_tokens:
                tok = tok.strip()
                if not tok:
                    continue
                stripped = tok.lstrip("-")
                if len(stripped) == 1:
                    flags.append(f"-{stripped}")
                else:
                    flags.append(f"--{stripped}")
                if len(stripped) > 1:
                    param_name = stripped.replace("-", "_")
                elif param_name is None:
                    param_name = stripped
            if not param_name:
                continue
            parsed.append((param_name, flags, final_default, desc, ann))
        return parsed

    def _build_signature_parameters(
        self,
        cli_params: list[inspect.Parameter],
        parsed_options: list[tuple[str, list[str], Any, str, type]],
    ) -> list[inspect.Parameter]:
        """
        Build a list of inspect.Parameter for Typer, wrapping handle() params
        with typer.Argument or typer.Option, binding decorator options where names match,
        then adding decorator-only options as keyword-only.
        """
        from inspect import Parameter

        parameters: list[inspect.Parameter] = []
        existing_names = {param.name for param in cli_params}
        # Map option names to their flags/default/help/annotation for
        # quick lookup. The annotation is sourced from the decorator's
        # explicit ``type``/``is_flag`` metadata in
        # ``_parse_decorator_options`` and propagated here so Typer/Click
        # picks the right click_type (scenario 12 fix — see the
        # ``_parse_decorator_options`` ROOT CAUSE comment).
        option_map: dict[str, tuple[list[str], Any, str, type]] = {
            name_opt: (flags, default, help_text, ann)
            for name_opt, flags, default, help_text, ann in parsed_options
        }

        # 1) Wrap handle parameters
        for param in cli_params:
            pname = param.name
            annotation = (
                param.annotation
                if param.annotation is not inspect.Parameter.empty
                else Any
            )
            if pname in option_map:
                flags, opt_default, help_text, opt_ann = option_map[pname]
                # Prefer handle default if present, else decorator default
                if param.default is not inspect.Parameter.empty:
                    handle_def = param.default
                else:
                    handle_def = opt_default
                # If handle() left the annotation blank, fall back to the
                # decorator's declared annotation so Typer doesn't see
                # ``Any`` (which it can't translate into a click_type).
                if annotation is Any and opt_ann is not None:
                    annotation = opt_ann
                default = typer.Option(
                    handle_def, *flags, help=help_text, show_default=True
                )
            else:
                if param.default is inspect.Parameter.empty:
                    default = typer.Argument(..., help=pname)
                else:
                    default = typer.Option(param.default, help=pname, show_default=True)
            parameters.append(
                Parameter(
                    pname,
                    kind=Parameter.POSITIONAL_OR_KEYWORD,
                    default=default,
                    annotation=annotation,
                )
            )

        # 2) Add decorator-only options as keyword-only. Use the
        # decorator-provided annotation directly so typed value options
        # (``type=int`` etc.) round-trip through Typer correctly. We
        # only fall back to the legacy "bool if default is bool, else
        # str" inference if the parser couldn't decide on a type.
        for name_opt, flags, default_val, help_text, ann in parsed_options:
            if name_opt in existing_names:
                continue
            if ann is None:
                ann = bool if isinstance(default_val, bool) else str
            parameters.append(
                Parameter(
                    name_opt,
                    kind=Parameter.KEYWORD_ONLY,
                    default=typer.Option(
                        default_val, *flags, help=help_text, show_default=True
                    ),
                    annotation=ann,
                )
            )

        return parameters

    def _make_callback(
        self, cmd_cls: type[Any], name: str, _di_params: list[inspect.Parameter]
    ):
        """
        Create the Typer callback that:
        - Runs before hooks
        - Instantiates the command class
        - Sets parsed options
        - Calls handle() (DI via application.call())
        - Runs after hooks or on_error hooks, printing full traceback if exceptions occur
        """

        def callback(**cli_kwargs):
            _run_before(name)
            try:
                inst = cmd_cls(self.application)
            except Exception as e:
                rprint(f"[red]Failed to instantiate {cmd_cls.__name__}: {e}[/red]")
                traceback.print_exc()
                raise typer.Exit(code=1)

            # Pass parsed options so self.option() works
            if hasattr(inst, "set_parsed_options"):
                inst.set_parsed_options(cli_kwargs)

            # Filter cli_kwargs for handle signature
            handle_sig = inspect.signature(inst.handle)
            filtered_cli = {
                k: v for k, v in cli_kwargs.items() if k in handle_sig.parameters
            }

            # Prometheus command-invocation instrumentation. Wraps
            # handle() so every CLI call is counted + timed by name,
            # regardless of whether it runs sync or async. Bounded
            # cardinality (``name`` is a static registered command).
            import time as _t

            try:
                from app.support.Metrics import Metrics as _M

                # Optionally push metrics to a gateway so short-lived CLI
                # commands show up in Grafana. The autopush helper may not
                # exist (it's opt-in); that's fine — the important thing
                # is that _M is set so counters still work for scrape-
                # based collection.
                try:
                    from app.support.Metrics import (
                        start_pushgateway_autopush as _start_autopush,
                    )

                    try:
                        from cara.configuration import config

                        _push_interval = int(config("metrics.pushgateway_interval_s", 15))
                    except (ImportError, RuntimeError, TypeError, ValueError):
                        _push_interval = int(
                            __import__("os").environ.get(
                                "METRICS_PUSHGATEWAY_INTERVAL_S", "15"
                            )
                        )
                    _start_autopush(interval_seconds=_push_interval)
                except (ImportError, AttributeError):
                    pass
            except (ImportError, RuntimeError):
                _M = None  # type: ignore[assignment]

            _cmd_start = _t.time()
            _cmd_outcome = "success"
            try:
                result = self.application.call(inst.handle, **filtered_cli)
                if inspect.isawaitable(result):
                    result = asyncio.run(result)
                _run_after(name)
            except Exception as e:
                _cmd_outcome = "failure"
                traceback.print_exc()
                _run_on_error(name, e)
                rprint(f"[red]Error in {name}: {e}[/red]")
                if _M is not None:
                    try:
                        _M.command_invocations_total.labels(
                            command=name,
                            outcome=_cmd_outcome,
                        ).inc()
                        _M.command_duration_seconds.labels(
                            command=name,
                        ).observe(_t.time() - _cmd_start)
                    except (OSError, RuntimeError, AttributeError, ConnectionError):
                        pass
                raise typer.Exit(code=1)

            if _M is not None:
                try:
                    _M.command_invocations_total.labels(
                        command=name,
                        outcome=_cmd_outcome,
                    ).inc()
                    _M.command_duration_seconds.labels(
                        command=name,
                    ).observe(_t.time() - _cmd_start)
                except (OSError, RuntimeError, AttributeError, ConnectionError):
                    pass

            if isinstance(result, int):
                raise typer.Exit(code=result)

        return callback
