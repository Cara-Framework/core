"""
Cache Clear Command for the Cara framework.

This module provides a CLI command to clear the application cache with enhanced UX.
"""

import os
from typing import Optional

from cara.cache import Cache
from cara.commands import CommandBase
from cara.decorators import command
from cara.queues import Queue


@command(
    name="cache:clear",
    help="Flush all cache entries with detailed feedback.",
    options={
        "--dry": "Show what would be cleared without actually clearing",
        "--f|force": "Force clear without confirmation in production",
        "--tags=?": "Clear only specific cache tags (comma-separated)",
    },
)
class CacheClearCommand(CommandBase):
    def handle(self, cache: Cache, queue: Queue, tags: Optional[str] = None):
        """Clear application cache with enhanced user experience."""
        self.info("🧹 Cache Clear Operation")

        # Production safety check
        if self._is_production() and not self.option("force"):
            if not self._confirm_production():
                self.info("❌ Cache clear aborted by user.")
                return

        # Parse tags if provided
        tag_list = []
        if tags:
            tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
            self.info(f"🏷️  Target tags: {', '.join(tag_list)}")

        # Dry run mode
        if self.option("dry"):
            self.info("🔍 DRY RUN MODE - No changes will be made")
            self._show_cache_info(cache, tag_list)
            return

        # Show configuration
        self.info("🔧 Configuration:")
        self.info(f"   Cache Driver: {type(cache).__name__}")
        if tag_list:
            self.info(f"   Tags: {', '.join(tag_list)}")
        else:
            self.info("   Scope: All cache entries")

        # Perform cache clear
        try:
            self.info("⚡ Clearing cache...")

            if tag_list:
                # Clear specific tags if supported
                if hasattr(cache, "flush_tags"):
                    cache.flush_tags(tag_list)
                    self.info(f"✅ Cache cleared for tags: {', '.join(tag_list)}")
                else:
                    self.warning(
                        "⚠️  Tag-specific clearing not supported by this cache driver"
                    )
                    self.info("   Clearing entire cache instead...")
                    cache.flush()
                    self.info("✅ Entire cache cleared successfully!")
            else:
                # Clear all cache
                cache.flush()
                self.info("✅ All cache entries cleared successfully!")

            # Additional cleanup suggestions
            self._show_cleanup_suggestions()

        except Exception as e:
            self.error(f"❌ Cache clear failed: {str(e)}")
            self.error("💡 Try checking your cache configuration")
            raise

    def _is_production(self) -> bool:
        """Check if we're running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]

    def _confirm_production(self) -> bool:
        """Confirm cache clear in production environment."""
        self.warning("⚠️  You are about to clear cache in PRODUCTION!")
        self.warning("   This may temporarily impact application performance.")

        while True:
            answer = (
                input("\n🤔 Are you sure you want to continue? (yes/no): ")
                .strip()
                .lower()
            )
            if answer in ["yes", "y"]:
                return True
            elif answer in ["no", "n"]:
                return False
            else:
                self.warning("Please answer 'yes' or 'no'")

    def _show_cache_info(self, cache: Cache, tag_list: list) -> None:
        """Show cache information in dry run mode."""
        self.info("📊 Cache Information:")
        self.info(f"   Driver: {type(cache).__name__}")

        if tag_list:
            self.info(f"   Would clear tags: {', '.join(tag_list)}")
        else:
            self.info("   Would clear: All cache entries")

        # Try to get cache stats if available
        try:
            if hasattr(cache, "get_stats"):
                stats = cache.get_stats()
                self.info(f"   Current entries: {stats.get('entries', 'Unknown')}")
                self.info(f"   Memory usage: {stats.get('memory', 'Unknown')}")
        except:
            self.info("   Cache statistics not available")

    def _show_cleanup_suggestions(self) -> None:
        """Show additional cleanup suggestions."""
        self.info("\n💡 Additional cleanup suggestions:")
        self.info("   • Consider clearing compiled views: php artisan view:clear")
        self.info("   • Clear route cache: php artisan route:clear")
        self.info("   • Clear config cache: php artisan config:clear")
        self.info("   • Restart queue workers if using cached jobs")
