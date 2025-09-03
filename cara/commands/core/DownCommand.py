"""
Maintenance Mode Command for the Cara framework.

This module provides a CLI command to enable maintenance mode with enhanced UX.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="down",
    help="Put the application into maintenance mode with advanced options.",
    options={
        "--dry": "Show what would be configured without creating maintenance file",
        "--message=?": "Custom maintenance message to display",
        "--retry=?": "Retry-After header value in seconds (default: 3600)",
        "--allow=?": "Comma-separated list of allowed IP addresses",
        "--secret=?": "Secret key to bypass maintenance mode",
        "--f|force": "Force enable even if already in maintenance mode",
    },
)
class DownCommand(CommandBase):
    """Put application into maintenance mode with enhanced configuration options."""

    def __init__(self, application=None):
        super().__init__(application)
        self.base_path = Path(paths("base"))
        self.maintenance_file = self.base_path / "MAINTENANCE"

    def handle(
        self,
        message: Optional[str] = None,
        retry: Optional[str] = None,
        allow: Optional[str] = None,
        secret: Optional[str] = None,
    ):
        """Handle maintenance mode activation with enhanced UX."""
        self.info("🔧 Maintenance Mode Activation")

        # Check current status
        if self._check_existing_maintenance():
            return

        # Build configuration
        try:
            config = self._build_configuration(message, retry, allow, secret)
        except Exception as e:
            self.error(f"❌ Configuration error: {e}")
            return

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(config)
            return

        # Create maintenance file
        try:
            self._activate_maintenance(config)
        except Exception as e:
            self.error(f"❌ {e}")
            return

    def _check_existing_maintenance(self) -> bool:
        """Check if maintenance mode is already active."""
        if self._file_exists() and not self.option("force"):
            existing_info = self._get_file_info()
            self.warning("⚠️  Application is already in maintenance mode!")

            if existing_info:
                self.info("📋 Current configuration:")
                self.info(f"   Message: {existing_info.get('message', 'N/A')}")
                self.info(f"   Created: {existing_info.get('created_at', 'Unknown')}")
                if existing_info.get("retry_after"):
                    self.info(f"   Retry After: {existing_info['retry_after']} seconds")

            self.info("💡 Use --force to override existing maintenance mode")
            return True

        return False

    def _build_configuration(
        self,
        message: Optional[str],
        retry: Optional[str],
        allow: Optional[str],
        secret: Optional[str],
    ) -> Dict[str, Any]:
        """Build maintenance configuration with validation."""
        config = {
            "enabled": True,
            "created_at": datetime.now().isoformat(),
            "message": message
            or "Application is temporarily unavailable for maintenance.",
            "retry_after": 3600,  # 1 hour default
            "allowed_ips": [],
            "secret": None,
        }

        if retry:
            try:
                retry_seconds = int(retry)
                if retry_seconds < 0:
                    raise ValueError("Retry after must be non-negative")
                config["retry_after"] = retry_seconds
            except ValueError as e:
                raise Exception(f"Invalid retry_after value: {e}")

        if allow:
            ips = [ip.strip() for ip in allow.split(",") if ip.strip()]
            config["allowed_ips"] = ips

        if secret:
            config["secret"] = secret

        return config

    def _show_dry_run(self, config: Dict[str, Any]) -> None:
        """Show what would be configured in dry run mode."""
        self.info("🔍 DRY RUN MODE - No changes will be made")
        self.info("📋 Maintenance configuration that would be created:")
        self.info(f"   Message: {config['message']}")
        self.info(f"   Retry After: {config['retry_after']} seconds")

        if config["allowed_ips"]:
            self.info(f"   Allowed IPs: {', '.join(config['allowed_ips'])}")
        else:
            self.info("   Allowed IPs: None")

        if config["secret"]:
            self.info(f"   Bypass Secret: {'*' * len(config['secret'])}")
        else:
            self.info("   Bypass Secret: None")

        self.info(f"   File Location: {self.maintenance_file}")

    def _activate_maintenance(self, config: Dict[str, Any]) -> None:
        """Activate maintenance mode with the given configuration."""
        self.info("🔧 Configuration:")
        self.info(f"   Message: {config['message']}")
        self.info(f"   Retry After: {config['retry_after']} seconds")

        if config["allowed_ips"]:
            self.info(f"   Allowed IPs: {', '.join(config['allowed_ips'])}")

        if config["secret"]:
            self.info("   Bypass Secret: Configured")

        self.info("⚡ Activating maintenance mode...")

        try:
            self._create_file(config)
            self.info("✅ Application is now in maintenance mode!")
            self.info(f"📁 Maintenance file created: {self.maintenance_file}")

            self._show_usage_tips()

        except Exception as e:
            raise Exception(f"Failed to activate maintenance mode: {e}")

    def _show_usage_tips(self) -> None:
        """Show helpful usage tips after activation."""
        self.info("\n💡 Usage Tips:")
        self.info("   • To disable maintenance mode: craft up")
        self.info("   • To check status: craft down --dry")
        self.info("   • Middleware will block requests with 503 status")
        if self._get_file_info().get("secret"):
            self.info("   • Use ?secret=<your-secret> to bypass maintenance mode")

    def _file_exists(self) -> bool:
        """Check if maintenance file exists."""
        return self.maintenance_file.exists()

    def _get_file_info(self) -> Optional[Dict[str, Any]]:
        """Get maintenance file information."""
        if not self._file_exists():
            return None

        try:
            with open(self.maintenance_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception):
            return {
                "message": "Application is in maintenance mode",
                "created_at": "Unknown",
            }

    def _create_file(self, config: Dict[str, Any]) -> None:
        """Create maintenance file with configuration."""
        try:
            with open(self.maintenance_file, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2, default=str)
        except Exception as e:
            raise Exception(f"Failed to create maintenance file: {e}")
