"""JWT Token Generation Command for the Cara framework."""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pendulum
from cara.commands import CommandBase
from cara.configuration import config
from cara.decorators import command


@command(
    name="jwt:generate",
    help="Generate JWT tokens for users with enhanced configuration options.",
    options={
        "--user=?": "User ID or email to generate token for (required)",
        "--email=?": "User email (alternative to --user)",
        "--ttl=?": "Token time-to-live in seconds (default: from config)",
        "--payload=?": "Additional JSON payload to include in token",
        "--save=?": "Save token to file (provide file path)",
        "--show-payload": "Show decoded token payload",
        "--no-expiry": "Generate token without expiration",
        "--dry": "Show what would be generated without creating token",
    },
)
class JWTGenerateCommand(CommandBase):
    """Generate JWT tokens for users with enhanced configuration and validation."""

    def handle(
        self,
        user: Optional[str] = None,
        email: Optional[str] = None,
        ttl: Optional[str] = None,
        payload: Optional[str] = None,
        save: Optional[str] = None,
    ):
        """Handle JWT token generation with enhanced options."""
        self.info("🔐 JWT Token Generation")

        # Validate parameters
        try:
            user_identifier = self._validate_parameters(user, email)
            token_ttl = self._parse_ttl(ttl)
            additional_payload = self._parse_payload(payload)
        except ValueError as e:
            self.error(f"❌ Parameter error: {e}")
            return

        # Find user
        try:
            target_user = self._find_user(user_identifier)
            if not target_user:
                self.error(f"❌ User not found: {user_identifier}")
                return
        except Exception as e:
            self.error(f"❌ Error finding user: {e}")
            return

        # Show user information
        self._show_user_info(target_user)

        # Dry run mode
        if self.option("dry"):
            self._show_dry_run(target_user, token_ttl, additional_payload, save)
            return

        # Generate token
        try:
            token_info = self._generate_token(
                target_user, token_ttl, additional_payload
            )
            self._show_token_info(token_info, target_user)

            # Save token if requested
            if save:
                self._save_token(token_info, save)

        except Exception as e:
            self.error(f"❌ Token generation failed: {e}")
            return

    def _validate_parameters(self, user: Optional[str], email: Optional[str]) -> str:
        """Validate and return user identifier."""
        if not user and not email:
            raise ValueError("Either --user or --email parameter is required")

        if user and email:
            raise ValueError("Cannot specify both --user and --email")

        return user or email

    def _parse_ttl(self, ttl: Optional[str]) -> Optional[int]:
        """Parse and validate TTL parameter."""
        if ttl is None:
            return None

        if self.option("no_expiry"):
            self.warning("⚠️  --no-expiry flag overrides --ttl parameter")
            return None

        try:
            ttl_seconds = int(ttl)
            if ttl_seconds <= 0:
                raise ValueError("TTL must be positive")
            if ttl_seconds > 31536000:  # 1 year
                self.warning("⚠️  TTL is very long (over 1 year)")
            return ttl_seconds
        except ValueError as e:
            raise ValueError(f"Invalid TTL value: {e}")

    def _parse_payload(self, payload: Optional[str]) -> Dict[str, Any]:
        """Parse additional payload JSON."""
        if not payload:
            return {}

        try:
            parsed = json.loads(payload)
            if not isinstance(parsed, dict):
                raise ValueError("Payload must be a JSON object")
            return parsed
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

    def _find_user(self, identifier: str):
        """Find user by ID or email."""
        try:
            User = self._resolve_user_model()
            if not User:
                self.error("User model not available.")
                return None

            # Try to find by email first (if it looks like an email)
            if "@" in identifier:
                user = User.where("email", identifier).first()
                if user:
                    return user

            # Try to find by user_id
            user = User.where("user_id", identifier).first()
            if user:
                return user

            # Try to find by primary key
            try:
                user = User.find(identifier)
                if user:
                    return user
            except:
                pass

            return None

        except Exception as e:
            raise Exception(f"Error finding user: {e}")

    def _show_user_info(self, user):
        """Display user information."""
        self.info("👤 Target User Information:")

        # Get user attributes safely
        user_attrs = getattr(user, "__attributes__", {})

        self.info(f"   User ID: {user_attrs.get('user_id', 'N/A')}")
        self.info(f"   Email: {user_attrs.get('email', 'N/A')}")
        self.info(
            f"   Name: {user_attrs.get('first_name', '')} {user_attrs.get('last_name', '')}".strip()
            or "N/A"
        )
        self.info(f"   Provider: {user_attrs.get('provider', 'N/A')}")

        if user_attrs.get("created_at"):
            self.info(f"   Created: {user_attrs.get('created_at')}")

    def _show_dry_run(
        self,
        user,
        ttl: Optional[int],
        additional_payload: Dict[str, Any],
        save_path: Optional[str],
    ):
        """Show what would be generated in dry run mode."""
        self.info("🔍 DRY RUN MODE - No token will be generated")

        # Show JWT configuration
        jwt_config = config("auth.guards.jwt", {})
        self.info("🔧 JWT Configuration:")
        self.info(f"   Algorithm: {jwt_config.get('algorithm', 'HS256')}")
        self.info(f"   Secret: {'*' * len(jwt_config.get('secret', ''))}")

        # Show token parameters
        self.info("📋 Token Parameters:")
        if ttl:
            self.info(f"   TTL: {ttl} seconds ({ttl // 3600}h {(ttl % 3600) // 60}m)")
        elif self.option("no_expiry"):
            self.info("   TTL: No expiration")
        else:
            default_ttl = jwt_config.get("ttl", 3600)
            self.info(f"   TTL: {default_ttl} seconds (default)")

        if additional_payload:
            self.info(
                f"   Additional Payload: {json.dumps(additional_payload, indent=2)}"
            )

        if save_path:
            self.info(f"   Would save to: {save_path}")

    def _generate_token(
        self, user, ttl: Optional[int], additional_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate JWT token for user."""
        self.info("⚡ Generating JWT token...")

        # Get JWT guard
        auth_manager = self.application.make("auth")
        jwt_guard = auth_manager.guard("jwt")

        # Prepare token payload
        now = pendulum.now().int_timestamp

        # Get user's default payload if available
        if hasattr(user, "to_jwt_payload") and callable(
            getattr(user, "to_jwt_payload")
        ):
            payload = user.to_jwt_payload()
        else:
            # Default payload
            payload = {
                "sub": str(
                    user.get_auth_id() if hasattr(user, "get_auth_id") else user.user_id
                ),
            }

        # Add timestamps
        payload["iat"] = now

        # Handle expiration
        if self.option("no_expiry"):
            # Don't add exp claim
            pass
        else:
            # Use custom TTL or default
            token_ttl = ttl or jwt_guard.ttl
            payload["exp"] = now + token_ttl

        # Add additional payload
        payload.update(additional_payload)

        # Generate token using JWT guard
        try:
            import jwt as pyjwt
        except ImportError:
            raise Exception(
                "PyJWT is required for JWT token generation. "
                "Please install it with: pip install PyJWT"
            )

        token = pyjwt.encode(payload, jwt_guard.secret, algorithm=jwt_guard.algorithm)

        return {
            "token": token,
            "payload": payload,
            "algorithm": jwt_guard.algorithm,
            "issued_at": now,
            "expires_at": payload.get("exp"),
        }

    def _show_token_info(self, token_info: Dict[str, Any], user):
        """Display generated token information."""
        self.info("✅ JWT token generated successfully!")
        self.info("")

        # Show token
        self.info("🔑 Generated Token:")
        token = token_info["token"]
        self.info(f"   {token}")

        self.info("")

        # Show token metadata
        self.info("📊 Token Information:")
        self.info(f"   Algorithm: {token_info['algorithm']}")
        self.info(f"   Issued At: {self._format_timestamp(token_info['issued_at'])}")

        if token_info.get("expires_at"):
            expires_at = token_info["expires_at"]
            self.info(f"   Expires At: {self._format_timestamp(expires_at)}")

            # Show time remaining
            now = pendulum.now().int_timestamp
            if expires_at > now:
                remaining = expires_at - now
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                self.info(f"   Time Remaining: {hours}h {minutes}m")
            else:
                self.info("   Status: ⚠️  Already expired")
        else:
            self.info("   Expires At: Never (no expiration)")

        # Show payload if requested
        if self.option("show_payload"):
            self.info("")
            self.info("📋 Token Payload:")
            payload_json = json.dumps(token_info["payload"], indent=2)
            for line in payload_json.split("\n"):
                self.info(f"   {line}")

        # Show usage examples
        self._show_usage_examples(token)

    def _show_usage_examples(self, token: str):
        """Show usage examples for the generated token."""
        self.info("")
        self.info("💡 Usage Examples:")
        self.info("   cURL:")
        self.info(
            f"     curl -H 'Authorization: Bearer {token[:20]}...' http://localhost:8000/api/user/resolve"
        )
        self.info("")
        self.info("   JavaScript:")
        self.info("     const response = await fetch('/api/user/resolve', {")
        self.info("       headers: {")
        self.info(f"         'Authorization': 'Bearer {token[:20]}...'")
        self.info("       }")
        self.info("     });")
        self.info("")
        self.info("   Python:")
        self.info("     headers = {")
        self.info(f"         'Authorization': 'Bearer {token[:20]}...'")
        self.info("     }")
        self.info("     response = requests.get('/api/user/resolve', headers=headers)")

    def _save_token(self, token_info: Dict[str, Any], file_path: str):
        """Save token to file."""
        try:
            save_path = Path(file_path)

            # Create directory if needed
            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Prepare data to save
            save_data = {
                "token": token_info["token"],
                "issued_at": token_info["issued_at"],
                "expires_at": token_info.get("expires_at"),
                "algorithm": token_info["algorithm"],
                "payload": token_info["payload"],
                "generated_at": self._format_timestamp(pendulum.now().int_timestamp),
            }

            # Save as JSON
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(save_data, f, indent=2)

            self.info(f"💾 Token saved to: {save_path}")

        except Exception as e:
            self.error(f"❌ Failed to save token: {e}")

    def _format_timestamp(self, timestamp: int) -> str:
        """Format timestamp for display."""
        try:
            dt = pendulum.from_timestamp(timestamp)
            return dt.to_datetime_string()
        except:
            return str(timestamp)
