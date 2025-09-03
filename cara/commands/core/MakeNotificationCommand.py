"""
Notification Generation Command for the Cara framework.
"""

from pathlib import Path

from cara.commands import CommandBase
from cara.decorators import command
from cara.support import paths


@command(
    name="make:notification",
    help="Generate a new Notification class with enhanced options.",
    options={
        "--dry": "Show what would be generated without creating files",
        "--force": "Overwrite existing notification file",
        "--channels=?": "Comma-separated list of notification channels (mail,database,slack)",
    },
)
class MakeNotificationCommand(CommandBase):
    """Generate Notification classes with enhanced configuration."""

    def handle(self, name: str, channels: str = None):
        """Handle notification generation."""
        self.info("🏗️  Notification Generation")

        try:
            notification_info = self._prepare_notification_info(name, channels)
        except ValueError as e:
            self.error(f"❌ {e}")
            return

        if self._check_existing_file(notification_info):
            return

        if self.option("dry"):
            self._show_dry_run(notification_info)
            return

        try:
            self._generate_notification(notification_info)
        except Exception as e:
            self.error(f"❌ Failed to generate notification: {e}")

    def _prepare_notification_info(self, name: str, channels: str = None) -> dict:
        """Prepare notification information."""
        if not name:
            raise ValueError("Notification name is required")

        class_name = self._clean_class_name(name)
        template_name = self._generate_template_name(class_name)

        # Parse channels
        channel_list = ["mail", "database"]
        if channels:
            channel_list = [ch.strip() for ch in channels.split(",") if ch.strip()]

        notifications_dir = Path(paths("notifications"))
        file_path = notifications_dir / f"{class_name}.py"

        return {
            "class_name": class_name,
            "template_name": template_name,
            "channels": channel_list,
            "file_path": file_path,
            "notifications_dir": notifications_dir,
        }

    def _clean_class_name(self, name: str) -> str:
        """Clean and format class name."""
        if name.endswith(".py"):
            name = name[:-3]

        # Convert to PascalCase preserving existing capitalization
        if "_" in name or " " in name:
            return "".join(word.capitalize() for word in name.replace("_", " ").split())
        else:
            return name[0].upper() + name[1:] if name else ""

    def _generate_template_name(self, class_name: str) -> str:
        """Generate template name from class name."""
        # Convert PascalCase to snake_case
        result = ""
        for i, char in enumerate(class_name):
            if char.isupper() and i > 0:
                result += "_"
            result += char.lower()
        return result

    def _check_existing_file(self, notification_info: dict) -> bool:
        """Check if notification file already exists."""
        if notification_info["file_path"].exists() and not self.option("force"):
            self.warning(
                f"⚠️  Notification already exists: {notification_info['file_path']}"
            )
            self.info("💡 Use --force to overwrite existing notification")
            return True
        return False

    def _show_dry_run(self, notification_info: dict) -> None:
        """Show dry run preview."""
        self.info("🔍 DRY RUN MODE - No files will be created")
        self.info("📋 Notification configuration:")
        self.info(f"   Class Name: {notification_info['class_name']}")
        self.info(f"   Template: {notification_info['template_name']}")
        self.info(f"   Channels: {', '.join(notification_info['channels'])}")
        self.info(f"   File Path: {notification_info['file_path']}")

    def _generate_notification(self, notification_info: dict) -> None:
        """Generate the notification file."""
        self.info("🔧 Configuration:")
        self.info(f"   Class: {notification_info['class_name']}")
        self.info(f"   Template: {notification_info['template_name']}")
        self.info(f"   Channels: {', '.join(notification_info['channels'])}")
        self.info(f"   File: {notification_info['file_path']}")

        notification_info["notifications_dir"].mkdir(parents=True, exist_ok=True)
        code = self._generate_notification_code(notification_info)

        self.info("⚡ Generating notification...")
        try:
            with open(notification_info["file_path"], "w", encoding="utf-8") as f:
                f.write(code)

            self.info("✅ Notification created successfully!")
            self.info(f"📁 Location: {notification_info['file_path']}")
            self._show_usage_tips(notification_info)

        except Exception as e:
            raise Exception(f"Failed to write notification file: {e}")

    def _generate_notification_code(self, notification_info: dict) -> str:
        """Generate the notification class code."""
        stub_path = Path(paths("cara")) / "commands" / "stubs" / "Notification.stub"

        with open(stub_path, "r", encoding="utf-8") as f:
            stub_content = f.read()

        code = stub_content.replace("{{ class_name }}", notification_info["class_name"])
        code = code.replace("{{ template_name }}", notification_info["template_name"])
        code = code.replace(
            "{{ docstring }}",
            f"{notification_info['class_name']}\n\nGenerated by Cara framework make:notification command.",
        )

        # Update channels in via method
        channels_str = ", ".join(f'"{ch}"' for ch in notification_info["channels"])
        code = code.replace('["mail", "database"]', f"[{channels_str}]")

        return code

    def _show_usage_tips(self, notification_info: dict) -> None:
        """Show usage examples."""
        class_name = notification_info["class_name"]

        self.info("\n💡 Usage Tips:")
        self.info(f"   Import: from app.notifications import {class_name}")
        self.info("   Send notification:")
        self.info(f"     user.notify({class_name}())")
        self.info("   Send to specific user:")
        self.info(f"     Notification.send(user, {class_name}())")

        if "mail" in notification_info["channels"]:
            self.info("\n📧 Remember to create mail template:")
            self.info(
                f"   resources/templates/mail/{notification_info['template_name']}.html"
            )
