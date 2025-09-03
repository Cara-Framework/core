"""
Lifespan Event Conductor.

This module handles all ASGI lifespan events in the Cara framework. It orchestrates application
startup and shutdown events.
"""

import asyncio
from typing import Any, Dict

from cara.facades import Log


class LifespanConductor:
    """Conducts lifespan event handling through the framework."""

    def __init__(self, application):
        """Initialize conductor with application instance."""
        self.application = application

    async def handle(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        """
        Orchestrate lifespan event handling.

        This is the main entry point for lifespan events. It:
        1. Handles startup events
        2. Handles shutdown events
        3. Manages application lifecycle

        The handler runs in a loop to process multiple lifespan events
        until a shutdown event is received.
        """
        while True:
            # Get event type
            event = await receive()

            if event["type"] == "lifespan.startup":
                try:
                    # Run startup tasks
                    await self._handle_startup()
                    await send({"type": "lifespan.startup.complete"})
                except Exception as e:
                    await send(
                        {
                            "type": "lifespan.startup.failed",
                            "message": str(e),
                        }
                    )

            elif event["type"] == "lifespan.shutdown":
                try:
                    # Run cleanup tasks
                    await self._handle_shutdown()
                    await send({"type": "lifespan.shutdown.complete"})
                    break  # Exit the loop after shutdown
                except Exception as e:
                    await send(
                        {
                            "type": "lifespan.shutdown.failed",
                            "message": str(e),
                        }
                    )
                    break  # Exit even if shutdown failed

    async def _handle_startup(self) -> None:
        """
        Handle application startup tasks.

        This is where you can add:
        - Database connections
        - Cache warming
        - Resource initialization
        - etc.
        """
        # TODO: Implement startup tasks
        pass

    async def _handle_shutdown(self) -> None:
        """Run application shutdown callbacks if any."""
        # Execute callbacks registered on the application instance
        callbacks = getattr(self.application, "_shutdown_callbacks", [])
        for cb in callbacks:
            try:
                # Each callback may be async or sync
                if asyncio.iscoroutinefunction(cb):
                    await cb()
                else:
                    await cb()
            except Exception as e:
                # Log but never fail the shutdown sequence
                Log.error(f"Shutdown callback error: {e}", exc_info=True)
