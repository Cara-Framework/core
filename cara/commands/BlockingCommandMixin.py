"""
Blocking Command Mixin for Cara Framework.

This mixin provides common functionality for long-running blocking commands
like queue workers, schedulers, and servers with consistent UX and auto-reload.
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from .AutoReloadMixin import AutoReloadMixin


class BlockingCommandMixin(AutoReloadMixin, ABC):
    """
    Base mixin for blocking commands with consistent UX.
    
    Provides:
    - Consistent ServeCommand-style startup screens
    - Auto-reload functionality
    - Graceful shutdown handling
    - Statistics tracking
    - Error handling with stack traces
    
    Usage:
        class MyBlockingCommand(BlockingCommandMixin, CommandBase):
            def get_command_name(self) -> str:
                return "My Service"
                
            def prepare_config(self, **kwargs) -> Dict[str, Any]:
                return {"driver": "default"}
                
            def show_config(self, config: Dict[str, Any]):
                # Display config in ServeCommand style
                pass
                
            def run_service(self, config: Dict[str, Any]):
                # Your blocking service logic
                pass
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time: Optional[float] = None
        self.service_stats: Dict[str, Any] = {
            "processed": 0,
            "failed": 0,
            "errors": []
        }

    def handle_blocking_command(self, **params):
        """
        Standard handler for blocking commands.
        
        This method provides the common flow:
        1. Show startup banner
        2. Setup auto-reload
        3. Prepare configuration
        4. Show configuration
        5. Run the service
        6. Handle errors and cleanup
        """
        # Show startup banner
        self.console.print()  # Empty line for spacing
        self.console.print(f"[bold #e5c07b]╭─ {self.get_command_name()} ─╮[/bold #e5c07b]")
        self.console.print()

        # Store parameters for restart
        self.store_restart_params(**params)

        # Setup auto-reload if enabled (default: true for development)
        if self.should_enable_auto_reload():
            self.enable_auto_reload()

        # Start main service loop
        try:
            self._run_main_loop(**params)
        except Exception as e:
            import traceback
            self.error(f"× {self.get_command_name()} error: {e}")
            if self.should_show_stack_trace():
                self.error(f"× Stack trace: {traceback.format_exc()}")
        finally:
            self.cleanup_auto_reload()
            self._show_final_stats()

    def _run_main_loop(self, *args, **kwargs):
        """Main service loop - called by AutoReloadMixin on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, '_restart_params') and self._restart_params:
            params = self._restart_kwargs
        else:
            params = kwargs

        # Prepare configuration
        try:
            config = self.prepare_config(**params)
        except Exception as e:
            self.error(f"× Configuration error: {e}")
            return

        # Show configuration
        self.show_config(config)

        # Show service status
        self._show_service_status()

        # Start service
        self.start_time = time.time()
        self.run_service(config)

    def should_enable_auto_reload(self) -> bool:
        """Check if auto-reload should be enabled."""
        from cara.configuration import config
        return self.option("reload") or config("app.debug", True)

    def should_show_stack_trace(self) -> bool:
        """Check if stack traces should be shown."""
        from cara.configuration import config
        return config("app.debug", False)

    def _show_service_status(self):
        """Display service status in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Service Status[/bold #e5c07b]")
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - {self.get_status_message()}[/#30e047]"
        )
        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()
        
        # Simple ready message
        self.console.print(f"[dim]Press Ctrl+C to stop {self.get_command_name().lower()}[/dim]")
        self.console.print()

    def _show_final_stats(self):
        """Show final service statistics."""
        if not self.start_time:
            return
            
        runtime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(runtime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        self.console.print()
        self.console.print(f"[bold #e5c07b]📊 Final {self.get_command_name()} Statistics:[/bold #e5c07b]")
        self.console.print(f"   Runtime: {runtime}")
        self.console.print(f"   Processed: {self.service_stats['processed']}")
        self.console.print(f"   Failed: {self.service_stats['failed']}")
        
        if self.service_stats['processed'] > 0:
            success_rate = ((self.service_stats['processed'] - self.service_stats['failed']) / self.service_stats['processed']) * 100
            self.console.print(f"   Success Rate: {success_rate:.1f}%")
        
        self.console.print()

    def increment_processed(self):
        """Increment processed counter."""
        self.service_stats['processed'] += 1

    def increment_failed(self, error: str = None):
        """Increment failed counter."""
        self.service_stats['failed'] += 1
        if error:
            self.service_stats['errors'].append(error)

    # Abstract methods that subclasses must implement
    @abstractmethod
    def get_command_name(self) -> str:
        """Return the display name for this command (e.g., 'Queue Worker')."""
        pass

    @abstractmethod
    def prepare_config(self, **kwargs) -> Dict[str, Any]:
        """Prepare and validate service configuration."""
        pass

    @abstractmethod
    def show_config(self, config: Dict[str, Any]):
        """Display service configuration in ServeCommand style."""
        pass

    @abstractmethod
    def run_service(self, config: Dict[str, Any]):
        """Run the main service logic (blocking)."""
        pass

    def get_status_message(self) -> str:
        """Return status message for service status display."""
        return "Running" 