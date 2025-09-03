"""
Command Runner for the Cara framework.

Provides utilities for registering CLI commands and executing them with Typer.
Prints full traceback on errors for easier debugging.
"""

import asyncio
import inspect
import traceback
from typing import Any, Dict, List, Tuple, Type

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

    def register(self, cmd_cls: Type[Any]):
        """
        Register a command class with Typer, building its signature from handle()
        and decorator options, and wiring before/after/error hooks.
        """
        name = getattr(cmd_cls, "name", None)
        if not name:
            return
        help_text = getattr(cmd_cls, "help", "")

        # 1) Inspect handle signature
        sig = inspect.signature(cmd_cls.handle)
        cli_params, di_params = self._split_handle_params(sig)

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
        self, sig: inspect.Signature
    ) -> Tuple[List[inspect.Parameter], List[inspect.Parameter]]:
        """
        Split handle() parameters into CLI parameters (primitive types or no annotation)
        and DI parameters (other annotated types).
        """
        primitive_types = (str, int, float, bool)
        cli_params: List[inspect.Parameter] = []
        di_params: List[inspect.Parameter] = []
        for param in sig.parameters.values():
            if param.name == "self":
                continue
            ann = param.annotation
            if ann is inspect.Parameter.empty or ann in primitive_types:
                cli_params.append(param)
            else:
                di_params.append(param)
        return cli_params, di_params

    def _parse_decorator_options(
        self, raw_options: Dict[str, str]
    ) -> List[Tuple[str, List[str], Any, str]]:
        """
        Parse decorator options dict into a list of tuples:
        (param_name, flags_list, default_value, help_text).
        """
        parsed: List[Tuple[str, List[str], Any, str]] = []
        for key, desc in raw_options.items():
            if "=" in key:
                flags_part, default_str = key.split("=", 1)
                if default_str == "?":
                    default_val = None
                    is_bool = False
                else:
                    default_val = default_str
                    is_bool = False
            else:
                flags_part = key
                default_val = False
                is_bool = True

            flag_tokens = flags_part.split("|")
            flags: List[str] = []
            param_name = None
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
            default = False if is_bool else default_val
            parsed.append((param_name, flags, default, desc))
        return parsed

    def _build_signature_parameters(
        self,
        cli_params: List[inspect.Parameter],
        parsed_options: List[Tuple[str, List[str], Any, str]],
    ) -> List[inspect.Parameter]:
        """
        Build a list of inspect.Parameter for Typer, wrapping handle() params
        with typer.Argument or typer.Option, binding decorator options where names match,
        then adding decorator-only options as keyword-only.
        """
        from inspect import Parameter

        parameters: List[inspect.Parameter] = []
        existing_names = {param.name for param in cli_params}
        # Map option names to their flags/default/help for quick lookup
        option_map: Dict[str, Tuple[List[str], Any, str]] = {
            name_opt: (flags, default, help_text)
            for name_opt, flags, default, help_text in parsed_options
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
                flags, opt_default, help_text = option_map[pname]
                # Prefer handle default if present, else decorator default
                if param.default is not inspect.Parameter.empty:
                    handle_def = param.default
                else:
                    handle_def = opt_default
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

        # 2) Add decorator-only options as keyword-only
        for name_opt, flags, default_val, help_text in parsed_options:
            if name_opt in existing_names:
                continue
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
        self, cmd_cls: Type[Any], name: str, di_params: List[inspect.Parameter]
    ):
        """
        Create the Typer callback that:
        - Runs before hooks
        - Instantiates the command class
        - Sets parsed options
        - Resolves DI parameters
        - Calls handle()
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

            # Resolve DI params
            di_kwargs: Dict[str, Any] = {}
            for param in di_params:
                if param.name in cli_kwargs:
                    continue
                try:
                    if param.annotation is not inspect.Parameter.empty:
                        di_kwargs[param.name] = self.application.make(param.annotation)
                    else:
                        di_kwargs[param.name] = self.application.make(param.name)
                except Exception:
                    if param.default is not inspect.Parameter.empty:
                        di_kwargs[param.name] = param.default
                    else:
                        traceback.print_exc()
                        raise

            # Filter cli_kwargs for handle signature
            handle_sig = inspect.signature(inst.handle)
            filtered_cli = {
                k: v for k, v in cli_kwargs.items() if k in handle_sig.parameters
            }

            try:
                result = inst.handle(**filtered_cli, **di_kwargs)
                if inspect.isawaitable(result):
                    result = asyncio.run(result)
                _run_after(name)
            except Exception as e:
                traceback.print_exc()
                _run_on_error(name, e)
                rprint(f"[red]Error in {name}: {e}[/red]")
                raise typer.Exit(code=1)

            if isinstance(result, int):
                raise typer.Exit(code=result)

        return callback
