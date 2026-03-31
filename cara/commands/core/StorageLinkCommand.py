"""
Storage Link Command for the Cara framework.

Laravel-style command to create symbolic links from public/storage to storage/app/public.
This allows serving files from storage/app/public through the web server.
"""

import os

from cara.commands import CommandBase
from cara.decorators import command
from cara.support.paths import public_path, storage_path


@command(
    name="storage:link",
    help="Create symbolic links from public/storage to storage/app/public (Laravel style).",
    options={
        "--force": "Force the creation of symlinks even if they already exist",
        "--relative": "Create relative symbolic links instead of absolute",
    },
)
class StorageLinkCommand(CommandBase):
    """Create Laravel-style storage symbolic links."""

    def __init__(self, application=None):
        super().__init__(application)

    def handle(self):
        """Handle storage link creation with Laravel-style output."""
        self.console.print()
        self.console.print("[bold #e5c07b]╭─ Storage Link ─╮[/bold #e5c07b]")
        self.console.print()

        # Get options
        force = self.option("force")
        relative = self.option("relative")

        # Define link mappings (Laravel style)
        links = self._get_storage_links()

        if not links:
            self.error("× No storage links configured")
            return

        # Show configuration
        self._show_link_config(links, force, relative)

        # Create links
        success_count = 0
        for link_path, target_path in links.items():
            if self._create_link(link_path, target_path, force, relative):
                success_count += 1

        # Show summary
        self._show_summary(success_count, len(links))

    def _get_storage_links(self) -> dict:
        """Get storage link mappings (Laravel style)."""
        return {
            public_path("storage"): storage_path("app/public"),
        }

    def _show_link_config(self, links: dict, force: bool, relative: bool) -> None:
        """Display link configuration."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Links to create:[/white] [dim]{len(links)}[/dim]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Force overwrite:[/white] [{'#30e047' if force else '#E21102'}]{'✓' if force else '×'}[/{'#30e047' if force else '#E21102'}]"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Relative links:[/white] [{'#30e047' if relative else '#E21102'}]{'✓' if relative else '×'}[/{'#30e047' if relative else '#E21102'}]"
        )
        self.console.print("[#e5c07b]└─[/#e5c07b]")

        self.console.print()
        self.console.print("[bold #e5c07b]┌─ Links[/bold #e5c07b]")
        for link_path, target_path in links.items():
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [white]{self._get_relative_path(link_path)}[/white] → [dim]{self._get_relative_path(target_path)}[/dim]"
            )
        self.console.print("[#e5c07b]└─[/#e5c07b]")

    def _create_link(
        self, link_path: str, target_path: str, force: bool, relative: bool
    ) -> bool:
        """Create a symbolic link."""
        try:
            # Ensure target directory exists
            os.makedirs(target_path, exist_ok=True)

            # Check if link already exists
            if os.path.exists(link_path) or os.path.islink(link_path):
                if not force:
                    self.warning(
                        f"⚠ Link already exists: {self._get_relative_path(link_path)}"
                    )
                    return False
                else:
                    # Remove existing link/file
                    if os.path.islink(link_path):
                        os.unlink(link_path)
                    elif os.path.isdir(link_path):
                        os.rmdir(link_path)
                    elif os.path.isfile(link_path):
                        os.remove(link_path)

            # Ensure parent directory exists
            os.makedirs(os.path.dirname(link_path), exist_ok=True)

            # Create the symbolic link
            if relative:
                # Calculate relative path
                rel_target = os.path.relpath(target_path, os.path.dirname(link_path))
                os.symlink(rel_target, link_path)
            else:
                # Create absolute symlink
                os.symlink(target_path, link_path)

            self.success(
                f"✓ Created link: {self._get_relative_path(link_path)} → {self._get_relative_path(target_path)}"
            )
            return True

        except OSError as e:
            self.error(
                f"× Failed to create link {self._get_relative_path(link_path)}: {e}"
            )
            return False
        except Exception as e:
            self.error(
                f"× Unexpected error creating link {self._get_relative_path(link_path)}: {e}"
            )
            return False

    def _get_relative_path(self, path: str) -> str:
        """Get path relative to project root for display."""
        try:
            from cara.support.paths import base_path

            return os.path.relpath(path, base_path())
        except:
            return path

    def _show_summary(self, success_count: int, total_count: int) -> None:
        """Show operation summary."""
        self.console.print()
        self.console.print("[bold #e5c07b]┌─ Summary[/bold #e5c07b]")

        if success_count == total_count:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [#30e047]✓ All {total_count} storage links created successfully[/#30e047]"
            )
        elif success_count > 0:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [#30e047]✓ {success_count}[/#30e047] of [white]{total_count}[/white] links created"
            )
            self.console.print(
                f"[#e5c07b]│[/#e5c07b] [#E21102]× {total_count - success_count}[/#E21102] links failed"
            )
        else:
            self.console.print(
                "[#e5c07b]│[/#e5c07b] [#E21102]× No links were created[/#E21102]"
            )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()
