"""
Command Runner for the Cara framework.

Provides utilities for registering CLI commands and executing them with Typer.
Prints full traceback on errors for easier debugging.
"""

from __future__ import annotations

import asyncio
import inspect
import time as _t
import traceback
from inspect import Parameter
from types import UnionType
from typing import Any, Union, get_args, get_origin
from typing import get_type_hints as _get_type_hints

import typer
from rich import print as rprint

from cara.decorators import _run_after, _run_before, _run_on_error
from cara.observability import MetricsBase


class CommandRunner:
    """Handles Typer command registration and execution, including hooks and full-traceback on errors."""

    def __init__(
        self,
        application: Any,
        *,
        instrument_commands: bool = True,
    ):
        self.application = application
        self.instrument_commands = instrument_commands
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
        raw_options = getattr(cmd_cls, "_cli_options", []) or []
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
        the CLI from decorator options alone. For ``search:reindex --synonyms-only`` that
        meant ``priority`` (declared ``"--priority"`` without ``=default``)
        was registered as a bool flag with default ``False``, so the handle
        body's ``if priority not in VALID_PRIORITIES`` rejected it as
        ``"Invalid priority 'False'"`` on every invocation. Resolving the
        hints via ``typing.get_type_hints`` (with the function's own
        module globals) restores the original intent across every command
        that uses PEP 563 — AIDiscover, DevReset, QueuePurge, etc.
        """
        primitive_types = (str, int, float, bool)
        # Resolve PEP 563 string annotations to real types. Use the original
        # function so get_type_hints can resolve forward refs against the
        # right module globals/locals. An invalid annotation is a broken
        # command contract and must stop command registration at boot.
        resolved_hints: dict[str, Any] = {}
        target_fn = handle_fn
        if target_fn is None:
            target_fn = getattr(sig, "__wrapped__", None)
        if target_fn is not None:
            resolved_hints = _get_type_hints(target_fn) or {}
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
            # so ``region: Optional[str] = None`` lands on the CLI side.
            if get_origin(ann) in (Union, UnionType):
                non_none = [a for a in get_args(ann) if a is not type(None)]
                if len(non_none) == 1:
                    ann = non_none[0]
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
        self, raw_options: list[dict[str, Any]]
    ) -> list[tuple[str, list[str], Any, str, type]]:
        """Parse canonical option metadata into runner tuples:
        (param_name, flags_list, default_value, help_text, annotation).

        There is deliberately one declaration shape. Rejecting malformed
        command metadata at boot prevents a misspelled type, implicit bool,
        or encoded default from silently changing the CLI contract.
        """
        if not isinstance(raw_options, list):
            raise TypeError("Command options must be a list of metadata objects")

        sentinel = object()
        parsed: list[tuple[str, list[str], Any, str, type]] = []
        allowed_keys = {"name", "help", "type", "default", "is_flag"}
        seen_flags: set[str] = set()
        seen_parameters: set[str] = set()
        for index, item in enumerate(raw_options):
            if not isinstance(item, dict):
                raise TypeError(f"Command option #{index} must be a metadata object")
            unknown_keys = set(item) - allowed_keys
            if unknown_keys:
                unknown = ", ".join(sorted(str(key) for key in unknown_keys))
                raise ValueError(f"Command option #{index} has unknown keys: {unknown}")

            key = item.get("name")
            desc = item.get("help")
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"Command option #{index} requires a non-empty name")
            if "=" in key:
                raise ValueError(
                    f"Command option {key!r} must declare its default explicitly"
                )
            if not isinstance(desc, str) or not desc.strip():
                raise ValueError(f"Command option {key!r} requires help text")

            explicit_type = item.get("type")
            explicit_default = item.get("default", sentinel)
            is_flag = item.get("is_flag", False)
            if not isinstance(is_flag, bool):
                raise TypeError(f"Command option {key!r} is_flag must be boolean")
            if is_flag:
                if explicit_type not in (None, bool):
                    raise TypeError(f"Command flag {key!r} may only declare type=bool")
                ann = bool
                if explicit_default is not sentinel:
                    if not isinstance(explicit_default, bool):
                        raise TypeError(f"Command flag {key!r} default must be boolean")
                    final_default: Any = explicit_default
                else:
                    final_default = False
            else:
                if explicit_type not in (str, int, float):
                    raise TypeError(
                        f"Command option {key!r} requires a real primitive type"
                    )
                ann = explicit_type
                if explicit_default is not sentinel:
                    if explicit_default is not None and type(explicit_default) is not ann:
                        raise TypeError(
                            f"Command option {key!r} default must match its type"
                        )
                    final_default = explicit_default
                else:
                    final_default = None

            flag_tokens = key.split("|")
            flags: list[str] = []
            param_name: str | None = None
            for tok in flag_tokens:
                tok = tok.strip()
                if not tok:
                    raise ValueError(f"Command option {key!r} contains an empty flag")
                stripped = tok.lstrip("-")
                if not stripped or tok == stripped:
                    raise ValueError(f"Command option token {tok!r} must start with '-'")
                if len(stripped) == 1:
                    flag = f"-{stripped}"
                else:
                    flag = f"--{stripped}"
                if flag in seen_flags:
                    raise ValueError(f"Duplicate command option flag {flag!r}")
                seen_flags.add(flag)
                flags.append(flag)
                if len(stripped) > 1:
                    param_name = stripped.replace("-", "_")
                elif param_name is None:
                    param_name = stripped
            if not param_name:
                raise ValueError(f"Command option {key!r} has no parameter name")
            if param_name in seen_parameters:
                raise ValueError(f"Duplicate command option parameter {param_name!r}")
            seen_parameters.add(param_name)
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

        # 2) Add decorator-only options as keyword-only. The canonical
        # metadata parser has already required and validated the annotation.
        for name_opt, flags, default_val, help_text, ann in parsed_options:
            if name_opt in existing_names:
                continue
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

            # Full application runners instrument command invocations by
            # default. Bootless runners disable this dependency so pure
            # filesystem commands remain independent of observability setup.
            # Cardinality stays bounded by the static command name.

            metrics = MetricsBase if self.instrument_commands else None

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
                if metrics is not None:
                    try:
                        metrics.command_invocations_total.labels(
                            command=name,
                            outcome=_cmd_outcome,
                        ).inc()
                        metrics.command_duration_seconds.labels(
                            command=name,
                        ).observe(_t.time() - _cmd_start)
                    except OSError, RuntimeError, AttributeError, ConnectionError:
                        pass
                raise typer.Exit(code=1)

            if metrics is not None:
                try:
                    metrics.command_invocations_total.labels(
                        command=name,
                        outcome=_cmd_outcome,
                    ).inc()
                    metrics.command_duration_seconds.labels(
                        command=name,
                    ).observe(_t.time() - _cmd_start)
                except OSError, RuntimeError, AttributeError, ConnectionError:
                    pass

            if isinstance(result, int):
                raise typer.Exit(code=result)

        return callback
