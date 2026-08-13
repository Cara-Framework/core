"""
Logging Channel Configurator for the Cara framework.

This module provides utilities for configuring and managing logging channels.
"""

from __future__ import annotations

from inspect import signature
from pathlib import Path
from typing import Any

from cara.configuration import config
from cara.logging.channels import ConsoleChannel, FileChannel, SlackChannel


class ChannelConfigurator:
    """Reads `config("logging")` and adds Loguru handlers for each channel in the active stack."""

    def __init__(self, loguru_logger: Any, settings: dict | None = None) -> None:
        self._logger = loguru_logger
        self._settings = settings

    def _config(self, key: str) -> object:
        if self._settings is None:
            return config(key)
        leaf = key.removeprefix("logging.")
        return self._settings.get(leaf)

    def configure(self) -> None:
        """
        1. Load default stack.
        2. Determine which channels are enabled for that stack.
        3. Instantiate each channel's "sink" and call `_logger.add(...)`.
        """
        default_stack = self._config("logging.default")
        stacks = self._config("logging.stacks")
        channels_cfg = self._config("logging.channels")
        slack_cfg = self._config("logging.slack")
        if not isinstance(default_stack, str) or not default_stack.strip():
            raise ValueError("logging.default must be a non-empty stack name")
        if not isinstance(stacks, dict) or not isinstance(channels_cfg, dict):
            raise TypeError("logging.stacks and logging.channels must be dictionaries")
        if not isinstance(slack_cfg, dict):
            raise TypeError("logging.slack must be a dictionary")

        # 1) Which channels belong to the default stack?
        if default_stack not in stacks:
            raise ValueError(f"logging stack {default_stack!r} is not configured")
        enabled_channels = stacks[default_stack]
        if not isinstance(enabled_channels, list) or any(
            not isinstance(name, str) or name not in channels_cfg
            for name in enabled_channels
        ):
            raise ValueError(
                f"logging stack {default_stack!r} must list configured channel names"
            )

        # 2) Register an enabled Slack sink first (ERROR+). A stack may list
        # Slack while the channel is deliberately disabled; in that state a
        # webhook is not required because no outbound handler is constructed.
        slack_options = channels_cfg.get("slack")
        if (
            "slack" in enabled_channels
            and isinstance(slack_options, dict)
            and slack_options.get("ENABLED") is True
        ):
            webhook = slack_cfg.get("WEBHOOK_URL")
            if not webhook:
                raise ValueError(
                    "logging stack enables slack without logging.slack.WEBHOOK_URL"
                )
            slack_level = slack_options.get("LEVEL", "ERROR")
            slack_sink = SlackChannel(slack_cfg, webhook)
            self._safe_add(
                slack_sink,
                level=slack_level,
                backtrace=True,
                diagnose=True,
                enqueue=True,
            )

        add_sig = signature(self._logger.add)

        # 3) Loop through each enabled channel and call logger.add(...)
        for channel_name in enabled_channels:
            opts = channels_cfg[channel_name]
            if not isinstance(opts, dict) or not isinstance(opts.get("ENABLED"), bool):
                raise TypeError(
                    f"logging channel {channel_name!r} must define boolean ENABLED"
                )
            if not opts["ENABLED"]:
                continue

            level = opts.get("LEVEL", "DEBUG")
            fmt = opts.get("FORMAT")
            sink_spec = opts.get("SINK")
            rotation = opts.get("ROTATION")
            retention = opts.get("RETENTION")
            compression = opts.get("COMPRESSION")
            serialize = opts.get("SERIALIZE", False)

            # Build the sink object from channels/
            if channel_name == "console":
                sink_obj = ConsoleChannel("stdout")

            elif sink_spec:
                # Ensure directory exists for any file template
                # e.g. "storage/logs/app_{time:YYYY-MM-DD}.log"
                base_dir = Path(sink_spec.split("{time")[0]).parent
                if base_dir:
                    base_dir.mkdir(parents=True, exist_ok=True)
                sink_obj = FileChannel(sink_spec)

            else:
                sink_obj = None

            # Build add_kwargs for logger.add(...)
            add_kwargs: dict[str, Any] = {"level": level}

            # Use config format for console if available
            if channel_name == "console" and fmt:
                add_kwargs["format"] = fmt
                add_kwargs["colorize"] = True  # Let Loguru handle colors
            elif channel_name == "console":
                # No custom format - use Loguru's default format
                add_kwargs["colorize"] = True  # Let Loguru handle colors and formatting

            # If a custom FORMAT string was provided in config, use it for non-console
            elif fmt and "format" in add_sig.parameters:
                add_kwargs["format"] = fmt

            if rotation and "rotation" in add_sig.parameters:
                add_kwargs["rotation"] = rotation
            if retention and "retention" in add_sig.parameters:
                add_kwargs["retention"] = retention
            if compression and "compression" in add_sig.parameters:
                add_kwargs["compression"] = compression
            if serialize and "serialize" in add_sig.parameters:
                add_kwargs["serialize"] = True
            if "enqueue" in add_sig.parameters:
                add_kwargs["enqueue"] = True
            if "backtrace" in add_sig.parameters:
                add_kwargs["backtrace"] = True
            if "diagnose" in add_sig.parameters:
                add_kwargs["diagnose"] = True

            # 4) **KEY CHANGE**: If sink_obj is FileChannel, pass str(sink_obj) so Loguru
            #    knows it's really a path. If sink_obj is ConsoleChannel, pass the object.
            if sink_obj is not None:
                if isinstance(sink_obj, FileChannel):
                    # Pass a string (the path template) rather than the object itself
                    self._safe_add(str(sink_obj), **add_kwargs)
                else:
                    # ConsoleChannel or SlackChannel just pass the object
                    self._safe_add(sink_obj, **add_kwargs)

    def _safe_add(self, sink: Any, **kwargs: Any) -> None:
        """
        Wraps `logger.add` with a fallback when the OS refuses to allocate the
        POSIX semaphore that `enqueue=True` needs (macOS
        `kern.posix.sem.max` exhausted → `OSError: [Errno 28] No space left on device`).
        Falls back to a non-enqueued sink rather than crashing the whole process.
        """
        try:
            self._logger.add(sink, **kwargs)
        except OSError as exc:
            if exc.errno != 28 or not kwargs.get("enqueue"):
                raise
            fallback = dict(kwargs)
            fallback["enqueue"] = False
            self._logger.add(sink, **fallback)
