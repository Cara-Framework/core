"""
Command Listing Command for the Cara framework.

This module provides a CLI command to list all registered commands with enhanced UX.
"""

from collections import defaultdict
from typing import Any, Dict, List

from cara.commands import CommandBase
from cara.decorators import command, get_registered_commands


@command(
    name="commands:list",
    help="List all available CLI commands with enhanced filtering and display options.",
    options={
        "--detailed": "Show detailed information including class names and options",
        "--categorized": "Group commands by category/namespace",
        "--filter=?": "Filter commands by name pattern",
        "--category=?": "Show only commands from specific category",
        "--no-help": "Show only commands without help text",
        "--stats": "Show command statistics",
        "--json": "Output in JSON format",
    },
)
class ListCommandsCommand(CommandBase):
    """List registered CLI commands with enhanced filtering and display options."""

    def handle(
        self,
        filter: str = None,
        category: str = None,
    ):
        """Handle command listing with enhanced options."""
        self.info("📋 Available CLI Commands")

        # Get and analyze commands
        try:
            commands = self._get_command_data()
        except Exception as e:
            self.error(f"❌ Failed to retrieve commands: {e}")
            return

        # Apply filters
        filtered_commands = self._apply_filters(commands, filter, category)

        if not filtered_commands:
            self.warning("⚠️  No commands found matching the specified criteria.")
            return

        # Show statistics if requested
        if self.option("stats"):
            self._show_statistics(commands, filtered_commands)

        # Output in requested format
        if self.option("json"):
            self._output_json(filtered_commands)
        elif self.option("categorized"):
            self._output_categorized(filtered_commands)
        elif self.option("detailed"):
            self._output_detailed(filtered_commands)
        else:
            self._output_basic(filtered_commands)

    def _get_command_data(self) -> List[Dict[str, Any]]:
        """Get command data from registered commands."""
        registered_commands = get_registered_commands()

        if not registered_commands:
            raise RuntimeError("No commands are registered")

        commands = []
        for cmd_class in registered_commands:
            try:
                cmd_info = self._get_command_info(cmd_class)
                commands.append(cmd_info)
            except Exception as e:
                self.warning(f"⚠️  Failed to analyze command {cmd_class}: {e}")

        # Sort by command name
        commands.sort(key=lambda x: x["name"])
        return commands

    def _get_command_info(self, cmd_class) -> Dict[str, Any]:
        """Extract information from a command class."""
        return {
            "name": getattr(cmd_class, "name", "<unknown>"),
            "help": getattr(cmd_class, "help", ""),
            "class_name": cmd_class.__name__,
            "module": cmd_class.__module__,
            "options": getattr(cmd_class, "options", {}),
        }

    def _apply_filters(
        self, commands: List[Dict[str, Any]], name_filter: str, category_filter: str
    ) -> List[Dict[str, Any]]:
        """Apply all requested filters to commands."""
        filtered = commands

        if name_filter:
            filtered = self._filter_by_name(filtered, name_filter)
            self.info(f"🔍 Filtered by name pattern: '{name_filter}'")

        if category_filter:
            filtered = self._filter_by_category(filtered, category_filter)
            self.info(f"🏷️  Filtered by category: '{category_filter}'")

        if self.option("no-help"):
            filtered = self._filter_without_help(filtered)
            self.info("❓ Showing only commands without help text")

        return filtered

    def _filter_by_name(
        self, commands: List[Dict[str, Any]], pattern: str
    ) -> List[Dict[str, Any]]:
        """Filter commands by name pattern."""
        pattern = pattern.lower()
        return [cmd for cmd in commands if pattern in cmd["name"].lower()]

    def _filter_by_category(
        self, commands: List[Dict[str, Any]], category: str
    ) -> List[Dict[str, Any]]:
        """Filter commands by category."""
        category = category.lower()
        return [
            cmd
            for cmd in commands
            if cmd["name"].split(":")[0].lower() == category
            or (category == "general" and ":" not in cmd["name"])
        ]

    def _filter_without_help(
        self, commands: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter commands that don't have help text."""
        return [cmd for cmd in commands if not cmd["help"]]

    def _categorize_commands(
        self, commands: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Categorize commands by their namespace/prefix."""
        categories = defaultdict(list)

        for cmd in commands:
            name = cmd["name"]
            if ":" in name:
                category = name.split(":")[0]
            else:
                category = "general"

            categories[category].append(cmd)

        # Sort commands within each category
        for category in categories:
            categories[category].sort(key=lambda x: x["name"])

        return dict(categories)

    def _get_statistics(self, commands: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about registered commands."""
        categories = self._categorize_commands(commands)

        return {
            "total_commands": len(commands),
            "categories": len(categories),
            "commands_with_help": len([c for c in commands if c["help"]]),
            "commands_with_options": len([c for c in commands if c["options"]]),
            "category_breakdown": {cat: len(cmds) for cat, cmds in categories.items()},
        }

    def _show_statistics(
        self, all_commands: List[Dict[str, Any]], filtered_commands: List[Dict[str, Any]]
    ) -> None:
        """Show command statistics."""
        all_stats = self._get_statistics(all_commands)
        filtered_stats = self._get_statistics(filtered_commands)

        self.info("\n📊 Command Statistics:")
        self.info(f"   Total registered: {all_stats['total_commands']}")
        self.info(f"   Matching filters: {filtered_stats['total_commands']}")
        self.info(f"   Categories: {all_stats['categories']}")
        self.info(f"   With help text: {all_stats['commands_with_help']}")
        self.info(f"   With options: {all_stats['commands_with_options']}")

        if all_stats["category_breakdown"]:
            self.info("\n📂 Category breakdown:")
            for category, count in sorted(all_stats["category_breakdown"].items()):
                self.info(f"   {category}: {count} command(s)")

    def _output_json(self, commands: List[Dict[str, Any]]) -> None:
        """Output commands in JSON format."""
        import json

        self.console.print_json(json.dumps(commands, indent=2))

    def _output_categorized(self, commands: List[Dict[str, Any]]) -> None:
        """Output commands grouped by category."""
        categories = self._categorize_commands(commands)

        for category_name, category_commands in sorted(categories.items()):
            self.info(
                f"\n🏷️  {category_name.title()} Commands ({len(category_commands)} command(s)):"
            )

            headers = ["Command", "Description"]
            rows = [
                [cmd["name"], cmd["help"] or "No description available"]
                for cmd in category_commands
            ]
            self.table(headers, rows)

    def _output_detailed(self, commands: List[Dict[str, Any]]) -> None:
        """Output commands with detailed information."""
        headers = ["Command", "Description", "Class", "Options"]
        rows = []

        for cmd in commands:
            options_count = len(cmd["options"]) if cmd["options"] else 0
            options_text = f"{options_count} option(s)" if options_count > 0 else "None"

            rows.append(
                [
                    cmd["name"],
                    cmd["help"] or "No description available",
                    cmd["class_name"],
                    options_text,
                ]
            )

        self.info(f"📋 Found {len(commands)} command(s) with detailed information:")
        self.table(headers, rows)

    def _output_basic(self, commands: List[Dict[str, Any]]) -> None:
        """Output commands in basic format."""
        headers = ["Command", "Description"]
        rows = [
            [cmd["name"], cmd["help"] or "No description available"] for cmd in commands
        ]

        self.info(f"📋 Found {len(commands)} registered command(s):")
        self.table(headers, rows)
