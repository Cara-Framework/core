"""
Application Key Generation Command for the Cara framework.

This module provides a CLI command to generate and set secure application keys with enhanced UX.
"""

import os
import secrets
from pathlib import Path
from typing import Optional

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="key:generate",
    help="Generate a new secure application key and update the .env file.",
    options={
        "--dry": "Show what key would be generated without updating .env file",
        "--length=?": "Key length in bytes (default: 32, min: 16, max: 256)",
        "--encoding=?": "Key encoding: base64, hex, or raw (default: base64)",
        "--f|force": "Force generation without confirmation in production",
        "--show": "Display the generated key in output",
    },
)
class GenerateKeyCommand(CommandBase):
    """Generate secure application keys with enhanced configuration options."""

    def __init__(self, application=None):
        super().__init__(application)
        self.base_path = Path(paths("base"))
        self.env_file = self.base_path / ".env"

    def handle(
        self,
        length: Optional[str] = None,
        encoding: Optional[str] = None,
    ):
        """Handle application key generation with enhanced UX."""
        self.info("🔐 Application Key Generation")

        # Check .env file
        if not self.env_file.exists():
            self.error(f"❌ .env file not found at {self.env_file}")
            self.info(
                "💡 Create a .env file first or run this command from the project root"
            )
            return

        # Validate and parse parameters
        try:
            key_length = self._parse_length(length)
            key_encoding = self._parse_encoding(encoding)
        except Exception as e:
            self.error(f"❌ Parameter error: {e}")
            return

        # Production safety check
        if self._is_production() and not self.option("force"):
            current_key = self._get_current_key()
            self.warning(
                "⚠️  You are about to generate a new application key in PRODUCTION!"
            )
            self.warning(
                "   This will invalidate all existing sessions and encrypted data!"
            )

            if current_key:
                self.warning(f"   Current key: {current_key[:20]}...")

            if not self._confirm_production():
                self.info("❌ Key generation aborted by user.")
                return

        # Generate key
        try:
            new_key = self._generate_key(key_length, key_encoding)
        except Exception as e:
            self.error(f"❌ Key generation failed: {e}")
            return

        # Get current key for comparison
        current_key = self._get_current_key()

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(new_key, current_key, key_length, key_encoding)
            return

        # Update .env file
        try:
            self._update_env_file(new_key, current_key, key_length, key_encoding)
        except Exception as e:
            self.error(f"❌ {e}")
            return

    def _parse_length(self, length: Optional[str]) -> int:
        """Parse and validate key length parameter."""
        if length is None:
            return 32

        try:
            key_length = int(length)
            if key_length <= 0:
                raise ValueError("Length must be a positive integer")
            if key_length < 16:
                raise ValueError("Length should be at least 16 for security")
            if key_length > 256:
                raise ValueError("Length should not exceed 256")
            return key_length
        except ValueError as e:
            raise Exception(f"Invalid length parameter: {e}")

    def _parse_encoding(self, encoding: Optional[str]) -> str:
        """Parse and validate encoding parameter."""
        if encoding is None:
            return "base64"

        encoding = encoding.lower()
        if encoding not in ["base64", "hex", "raw"]:
            raise Exception(f"Invalid encoding '{encoding}'. Use: base64, hex, or raw")

        return encoding

    def _generate_key(self, length: int = 32, encoding: str = "base64") -> str:
        """Generate a secure random key with specified length and encoding."""
        if encoding == "base64":
            return f"base64:{secrets.token_urlsafe(length)}"
        elif encoding == "hex":
            return f"hex:{secrets.token_hex(length)}"
        elif encoding == "raw":
            return secrets.token_urlsafe(length)
        else:
            raise ValueError(f"Unsupported encoding: {encoding}")

    def _get_current_key(self) -> Optional[str]:
        """Get current APP_KEY value from .env file."""
        if not self.env_file.exists():
            return None

        try:
            with open(self.env_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            for line in lines:
                if line.strip().startswith("APP_KEY="):
                    return line.strip().split("=", 1)[1] if "=" in line else ""
        except Exception:
            pass

        return None

    def _show_dry_run(
        self, new_key: str, current_key: Optional[str], length: int, encoding: str
    ):
        """Show what would be generated in dry run mode."""
        self.info("🔍 DRY RUN MODE - No changes will be made")
        self.info("🔧 Key generation configuration:")
        self.info(f"   Length: {length} bytes")
        self.info(f"   Encoding: {encoding}")
        self.info(f"   .env file: {self.env_file}")

        if current_key:
            self.info(f"   Current key: {current_key[:20]}...")
            self.info("   Action: Would REPLACE existing key")
        else:
            self.info("   Current key: None")
            self.info("   Action: Would ADD new key")

        if self.option("show"):
            self.info(f"\n🔑 Generated key preview: {new_key}")
        else:
            self.info(
                f"\n🔑 Generated key preview: {new_key[:20]}... (use --show to see full key)"
            )

    def _update_env_file(
        self, new_key: str, current_key: Optional[str], length: int, encoding: str
    ):
        """Update the application key in .env file."""
        self.info("🔧 Configuration:")
        self.info(f"   Length: {length} bytes")
        self.info(f"   Encoding: {encoding}")
        self.info(f"   .env file: {self.env_file}")

        if current_key:
            self.info(f"   Current key: {current_key[:20]}...")
            self.info("   Action: Replacing existing key")
        else:
            self.info("   Current key: None")
            self.info("   Action: Adding new key")

        self.info("⚡ Updating application key...")

        try:
            was_replaced = self._update_key_in_file(new_key)

            action = "replaced" if was_replaced else "added"
            self.info(f"✅ Application key {action} successfully!")

            if self.option("show"):
                self.info(f"🔑 New key: {new_key}")
            else:
                self.info(f"🔑 New key: {new_key[:20]}... (use --show to see full key)")

            self.info("\n💡 Important Notes:")
            self.info("   • All existing sessions will be invalidated")
            self.info("   • Encrypted data using the old key may become inaccessible")
            self.info("   • Consider restarting your application servers")
            self.info("   • Backup your .env file before making changes in production")

        except Exception as e:
            raise Exception(f"Failed to update application key: {e}")

    def _update_key_in_file(self, new_key: str) -> bool:
        """Update APP_KEY in .env file, return True if key was replaced, False if added."""
        try:
            with open(self.env_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except Exception as e:
            raise Exception(f"Failed to read .env file: {e}")

        new_lines = []
        key_replaced = False

        for line in lines:
            if line.strip().startswith("APP_KEY="):
                new_lines.append(f"APP_KEY={new_key}\n")
                key_replaced = True
            else:
                new_lines.append(line)

        if not key_replaced:
            # Add new key at the end
            new_lines.append(f"\nAPP_KEY={new_key}\n")

        try:
            with open(self.env_file, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        except Exception as e:
            raise Exception(f"Failed to write .env file: {e}")

        return key_replaced

    def _is_production(self) -> bool:
        """Check if running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]

    def _confirm_production(self) -> bool:
        """Get user confirmation for production key generation."""
        while True:
            answer = (
                input(
                    "\n🤔 Are you sure you want to generate a new key in PRODUCTION? (yes/no): "
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
