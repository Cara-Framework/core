"""
Universal Auto-Reload Mixin for Cara Framework.

This mixin provides hot-reload capabilities for any long-running Cara command,
similar to uvicorn's auto-reload but for all blocking operations.
"""

import importlib
import os
import sys
import time
from typing import List, Optional

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class AutoReloadHandler(FileSystemEventHandler):
    """Enhanced file watcher for universal auto-reload."""

    def __init__(self, command, debounce_delay: float = 0.3):
        self.command = command
        self.last_reload = 0
        self.debounce_delay = debounce_delay
        super().__init__()

    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return

        # Only watch Python files and config files
        if not self._should_watch_file(event.src_path):
            return

        # Debouncing to avoid multiple reloads
        current_time = time.time()
        if current_time - self.last_reload < self.debounce_delay:
            return

        # Skip temp/cache files
        if self._is_temp_file(event.src_path):
            return

        self.last_reload = current_time
        
        # Show which file triggered the reload
        rel_path = os.path.relpath(event.src_path)
        self.command.info(f"🔄 File changed: {rel_path}")
        
        # Trigger reload
        self.command._trigger_auto_reload()

    def _should_watch_file(self, file_path: str) -> bool:
        """Check if file should trigger reload."""
        watch_extensions = ['.py', '.yaml', '.yml', '.json', '.toml', '.env', '.txt']
        return any(file_path.endswith(ext) for ext in watch_extensions)

    def _is_temp_file(self, file_path: str) -> bool:
        """Check if file is temporary and should be ignored."""
        ignore_patterns = [
            "__pycache__", ".pyc", ".pyo", ".tmp", ".swp", 
            ".DS_Store", ".git", ".pytest_cache", "node_modules"
        ]
        return any(pattern in file_path for pattern in ignore_patterns)


class AutoReloadMixin:
    """
    Universal auto-reload mixin for Cara commands.
    
    Usage:
        class MyLongRunningCommand(AutoReloadMixin, CommandBase):
            def handle(self):
                self.enable_auto_reload()  # Enable hot reload
                self._run_main_loop()      # Your main loop
                
            def _run_main_loop(self):
                while not self.shutdown_requested:
                    # Your blocking operation here
                    pass
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdown_requested = False
        self._auto_reload_enabled = False
        self._observer: Optional[Observer] = None
        self._restart_params = ()
        self._restart_kwargs = {}

    def enable_auto_reload(self, watch_paths: Optional[List[str]] = None):
        """Enable auto-reload for this command."""
        if self._auto_reload_enabled:
            return
            
        self._auto_reload_enabled = True
        self.info("🔄 Auto-reload enabled - watching for file changes...")
        
        # Start file watcher
        self._start_file_watcher(watch_paths)

    def disable_auto_reload(self):
        """Disable auto-reload for this command."""
        self._auto_reload_enabled = False
        self._stop_file_watcher()

    def _start_file_watcher(self, watch_paths: Optional[List[str]] = None):
        """Start the file watcher."""
        if self._observer is not None:
            return  # Already watching

        self._observer = Observer()
        handler = AutoReloadHandler(self)

        # Get paths to watch
        paths_to_watch = watch_paths or self._get_default_watch_paths()

        for path in paths_to_watch:
            if path and os.path.isdir(path):
                self._observer.schedule(handler, path=path, recursive=True)
                self.info(f"📁 Watching: {os.path.relpath(path)}")

        if paths_to_watch:
            self._observer.daemon = True
            self._observer.start()

    def _get_default_watch_paths(self) -> List[str]:
        """Get default paths to watch for changes."""
        from cara.support import paths
        
        watch_paths = []
        
        # Application directories to watch
        app_dirs = ["app", "config", "routes", "database", "packages"]
        
        for app_dir in app_dirs:
            try:
                path = paths(app_dir)
                if path and os.path.isdir(path):
                    watch_paths.append(path)
            except Exception:
                continue
                
        return watch_paths

    def _stop_file_watcher(self):
        """Stop the file watcher."""
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join()
            except Exception:
                pass
            finally:
                self._observer = None

    def _trigger_auto_reload(self):
        """Trigger the auto-reload process."""
        if not self._auto_reload_enabled:
            return
            
        self.info("🔄 Reloading command...")
        
        # Set shutdown flag to stop current loop
        self.shutdown_requested = True
        
        # Give current operation time to finish gracefully
        time.sleep(0.5)
        
        # Purge modules and restart
        self._purge_modules_for_reload()
        self._restart_command()

    def _purge_modules_for_reload(self):
        """Purge loaded modules for hot reload."""
        # Invalidate import caches
        importlib.invalidate_caches()
        
        # Modules to purge for hot reload
        purge_patterns = [
            'app.',           # Application modules
            'packages.',      # Package modules  
            'config.',        # Config modules
            'routes.',        # Route modules
            'database.',      # Database modules
        ]
        
        modules_to_remove = []
        for module_name in list(sys.modules.keys()):
            for pattern in purge_patterns:
                if module_name.startswith(pattern):
                    modules_to_remove.append(module_name)
                    break
        
        # Remove modules
        for module_name in modules_to_remove:
            try:
                del sys.modules[module_name]
            except KeyError:
                pass
                
        if modules_to_remove:
            self.info(f"🔄 Purged {len(modules_to_remove)} modules for hot reload")

    def _restart_command(self):
        """Restart the command after reload."""
        try:
            # Reset shutdown flag
            self.shutdown_requested = False
            
            self.info("✅ Command restarted successfully")
            
            # Call the command's main loop again
            if hasattr(self, "_run_main_loop"):
                self._run_main_loop(*self._restart_params, **self._restart_kwargs)
            else:
                self.warning("⚠️  Command doesn't implement _run_main_loop method")
                
        except Exception as e:
            self.error(f"❌ Failed to restart command: {e}")
            self.shutdown_requested = True

    def store_restart_params(self, *args, **kwargs):
        """Store parameters for restart."""
        self._restart_params = args
        self._restart_kwargs = kwargs

    def cleanup_auto_reload(self):
        """Cleanup auto-reload resources."""
        self._stop_file_watcher()
        self._auto_reload_enabled = False 