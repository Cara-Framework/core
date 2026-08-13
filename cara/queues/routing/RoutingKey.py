"""RoutingKey."""

from __future__ import annotations

from dataclasses import dataclass

from cara.exceptions import QueueException


@dataclass
class RoutingKey:
    """Parsed routing key components."""

    domain: str
    subtype: str
    priority: str

    @property
    def key(self) -> str:
        """Get full routing key string."""
        return f"{self.domain}.{self.subtype}.{self.priority}"

    @classmethod
    def parse(cls, routing_key: str) -> RoutingKey:
        """Parse routing key string into components."""
        parts = routing_key.split(".")
        if len(parts) != 3:
            raise QueueException(
                f"Invalid routing key format: {routing_key}. Expected: domain.subtype.priority"
            )

        return cls(domain=parts[0], subtype=parts[1], priority=parts[2])
