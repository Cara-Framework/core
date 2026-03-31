"""
Maintenance Mode Deactivation Command for the Cara framework.

This module provides a CLI command to disable maintenance mode with enhanced UX.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="up",
    help="Bring the application out of maintenance mode with enhanced feedback.",
    options={
        "--dry": "Show current maintenance status without making changes",
        "--f|force": "Force disable without confirmation in production",
    },
)
class UpCommand(CommandBase):
    """Disable maintenance mode with enhanced user experience and safety checks."""

    def __init__(self, application=None):
        super().__init__(application)
        self.base_path = Path(paths("base"))
        self.maintenance_file = self.base_path / "MAINTENANCE"

    def handle(self):
        """Handle maintenance mode deactivation with enhanced UX."""
        self.info("🔓 Maintenance Mode Deactivation")

        # Check current status
        if not self._check_maintenance_status():
            return

        # Get maintenance info
        maintenance_info = self._get_maintenance_info()

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(maintenance_info)
            return

        # Production safety check
        if self._requires_production_confirmation():
            return

        # Deactivate maintenance mode
        try:
            self._deactivate_maintenance(maintenance_info)
        except Exception as e:
            self.error(f"❌ {e}")
            return

    def _check_maintenance_status(self) -> bool:
        """Check if maintenance mode is currently active."""
        if not self._is_maintenance_active():
            self.warning("⚠️  Application is not in maintenance mode.")
            self.info("💡 Nothing to do - application is already live!")
            return False

        return True

    def _show_dry_run(self, maintenance_info: Optional[Dict[str, Any]]) -> None:
        """Show current maintenance status in dry run mode."""
        self.info("🔍 DRY RUN MODE - No changes will be made")
        self.info("📋 Current maintenance mode status:")

        if maintenance_info:
            self.info("   Status: ACTIVE")
            self.info(f"   Message: {maintenance_info.get('message', 'N/A')}")
            self.info(f"   Created: {maintenance_info.get('created_at', 'Unknown')}")

            if maintenance_info.get("retry_after"):
                self.info(f"   Retry After: {maintenance_info['retry_after']} seconds")

            if maintenance_info.get("allowed_ips"):
                self.info(f"   Allowed IPs: {', '.join(maintenance_info['allowed_ips'])}")

            if maintenance_info.get("secret"):
                self.info("   Bypass Secret: Configured")

            if maintenance_info.get("file_type") == "legacy":
                self.info("   File Type: Legacy (simple text file)")

        self.info(f"   File Location: {self.maintenance_file}")
        self.info("\n🔓 Would deactivate maintenance mode by removing the file")

    def _requires_production_confirmation(self) -> bool:
        """Check if production confirmation is required."""
        if self._is_production() and not self.option("force"):
            self.warning("⚠️  You are about to disable maintenance mode in PRODUCTION!")
            self.warning("   This will make the application available to all users.")

            if not self._confirm_production_action():
                self.info("❌ Maintenance mode deactivation aborted by user.")
                return True

        return False

    def _deactivate_maintenance(self, maintenance_info: Optional[Dict[str, Any]]) -> None:
        """Deactivate maintenance mode."""
        self.info("📋 Current maintenance configuration:")

        if maintenance_info:
            self.info(f"   Message: {maintenance_info.get('message', 'N/A')}")
            self.info(f"   Active since: {maintenance_info.get('created_at', 'Unknown')}")

            if maintenance_info.get("file_type") == "legacy":
                self.info("   Type: Legacy maintenance file")

        self.info("⚡ Deactivating maintenance mode...")

        try:
            self._remove_maintenance_file()

            self.info("✅ Application is now live (maintenance mode disabled)!")
            self.info(f"🗑️  Maintenance file removed: {self.maintenance_file}")

            self._show_success_tips()

        except Exception as e:
            raise Exception(f"Failed to disable maintenance mode: {e}")

    def _show_success_tips(self) -> None:
        """Show helpful tips after successful deactivation."""
        self.info("\n💡 Success Tips:")
        self.info("   • Application is now accepting all requests")
        self.info("   • Consider monitoring application performance")
        self.info("   • Check logs for any issues during maintenance")
        self.info("   • To enable maintenance mode again: craft down")

    def _is_maintenance_active(self) -> bool:
        """Check if maintenance mode is currently active."""
        return self.maintenance_file.exists()

    def _get_maintenance_info(self) -> Optional[Dict[str, Any]]:
        """Get current maintenance configuration."""
        if not self._is_maintenance_active():
            return None

        try:
            with open(self.maintenance_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception):
            # Fallback for simple text files
            return {
                "message": "Application is in maintenance mode",
                "created_at": "Unknown",
                "file_type": "legacy",
            }

    def _remove_maintenance_file(self) -> None:
        """Remove maintenance file to deactivate maintenance mode."""
        try:
            self.maintenance_file.unlink()
        except Exception as e:
            raise Exception(f"Failed to remove maintenance file: {e}")

    def _is_production(self) -> bool:
        """Check if running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]

    def _confirm_production_action(self) -> bool:
        """Get user confirmation for production actions."""
        while True:
            answer = (
                input(
                    "\n🤔 Are you sure you want to disable maintenance mode in PRODUCTION? (yes/no): "
                )
                .strip()
                .lower()
            )
            if answer in ["yes", "y"]:
                return True
            elif answer in ["no", "n"]:
                return False
            else:
                print("Please answer 'yes' or 'no'")
