"""
Base Cast System for Cara ORM

Provides the foundation for all cast types with a registry pattern.
"""

from __future__ import annotations

from .CastRegistry import CastRegistry

# Global registry instance
cast_registry = CastRegistry()
