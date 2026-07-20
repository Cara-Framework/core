"""
Cache Clear Command for the Cara framework.

This module provides a CLI command to clear the application cache with enhanced UX.
"""

from __future__ import annotations

from cara.cache import Cache
from cara.commands import CommandBase
from cara.decorators import command


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
    def handle(self, tags: str | None = None):
        """Clear application cache with enhanced user experience."""
        cache = self._resolve_cache()
        self.info("🧹 Cache Clear Operation")

        # Production safety check
        if (
            self._is_production()
            and not self.option("force")
            and not self._confirm_production()
        ):
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

    def _resolve_cache(self) -> Cache:
        """Resolve the cache manager from the application container."""
        if self.application is None:
            raise RuntimeError(
                "Cache manager is not bound — boot the application before running cache:clear"
            )
        return self.application.make(Cache)

    def _confirm_production(self) -> bool:
        """Confirm cache clear in production environment."""
        self.warning("⚠️  You are about to clear cache in PRODUCTION!")
        self.warning("   This may temporarily impact application performance.")
        return self._confirm_yes_no()

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
        except Exception:
            self.info("   Cache statistics not available")

    def _show_cleanup_suggestions(self) -> None:
        """Point at the state this command did NOT clear.

        Three of the four suggestions here were ``php artisan`` invocations
        carried over from Laravel — for a view cache, a route cache and a
        config cache that Cara does not have. A tool that hands the operator
        commands from a different framework, for subsystems that do not
        exist, teaches them to distrust everything else it says.

        What genuinely survives a cache flush is the long-lived processes:
        they hold their own imported job classes and an open broker
        connection, so a flush alone does not reload them.
        """
        self.info("\n💡 Not cleared by this command:")
        self.info(
            "   • Long-lived processes keep imported job classes in memory — "
            "restart queue:work, schedule:work and queue:relay to reload them"
        )
        self.info(
            "   • Generated route files are build output, not cache — "
            "re-run routes:generate if controller docstrings changed"
        )
