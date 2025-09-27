"""
Logging Channel Configurator for the Cara framework.

This module provides utilities for configuring and managing logging channels.
"""

import os
from inspect import signature
from pathlib import Path
from typing import Any, Dict

from cara.configuration import config
from cara.logging.channels import ConsoleChannel, FileChannel, SlackChannel


class ChannelConfigurator:
    """Reads `config("logging")` and adds Loguru handlers for each channel in the active stack."""

    def __init__(self, loguru_logger: Any) -> None:
        self._logger = loguru_logger

    def configure(self) -> None:
        """
        1. Load default stack.
        2. Determine which channels are enabled for that stack.
        3. Instantiate each channel's "sink" and call `_logger.add(...)`.
        """
        default_stack: str = config("logging.default", "daily")
        stacks: Dict[str, Any] = config("logging.stacks", {})
        channels_cfg: Dict[str, Any] = config("logging.channels", {})
        slack_cfg: Dict[str, Any] = config("logging.slack", {})

        # 1) Which channels belong to the default stack?
        if default_stack and default_stack in stacks:
            enabled_channels = stacks[default_stack]
        else:
            # Fallback: enable any channel whose config.ENABLED=True
            enabled_channels = [
                name
                for name, opts in channels_cfg.items()
                if opts.get("ENABLED", False)
            ]

        # 2) If "slack" is in that stack, register a Slack sink first (ERROR+)
        if "slack" in enabled_channels:
            webhook = slack_cfg.get("WEBHOOK_URL") or os.getenv("SLACK_WEBHOOK_URL")
            if webhook:
                slack_level = channels_cfg.get("slack", {}).get("LEVEL", "ERROR")
                slack_sink = SlackChannel(slack_cfg, webhook)
                self._logger.add(
                    slack_sink,
                    level=slack_level,
                    backtrace=True,
                    diagnose=True,
                    enqueue=True,
                )

        add_sig = signature(self._logger.add)

        # 3) Loop through each enabled channel and call logger.add(...)
        for channel_name in enabled_channels:
            opts: Dict[str, Any] = channels_cfg.get(channel_name, {})
            if not opts.get("ENABLED", False):
                continue

            level = opts.get("LEVEL", "DEBUG")
            fmt = opts.get("FORMAT", None)
            sink_spec = opts.get("SINK", None)
            rotation = opts.get("ROTATION", None)
            retention = opts.get("RETENTION", None)
            compression = opts.get("COMPRESSION", None)
            serialize = opts.get("SERIALIZE", False)

            # Build the sink object from channels/
            if channel_name == "console":
                sink_obj = ConsoleChannel("stdout")

            elif sink_spec:
                # Ensure directory exists for any file template
                # e.g. "storage/logs/app_{time:YYYY-MM-DD}.log"
                base_dir = sink_spec.split("{time")[0]
                if base_dir:
                    Path(os.path.dirname(base_dir)).mkdir(parents=True, exist_ok=True)
                sink_obj = FileChannel(sink_spec)

            else:
                sink_obj = None

            # Build add_kwargs for logger.add(...)
            add_kwargs: Dict[str, Any] = {"level": level}

            # Use config format for console if available
            if channel_name == "console" and fmt:
                add_kwargs["format"] = fmt
                add_kwargs["colorize"] = True  # Let Loguru handle colors
            elif channel_name == "console":
                add_kwargs["format"] = "{message}"  # Fallback to pre-formatted message
                add_kwargs["colorize"] = False  # We handle colors ourselves

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
                    self._logger.add(str(sink_obj), **add_kwargs)
                else:
                    # ConsoleChannel or SlackChannel just pass the object
                    self._logger.add(sink_obj, **add_kwargs)
