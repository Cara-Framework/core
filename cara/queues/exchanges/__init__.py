"""
Queue Exchange System for Cara Framework.

Provides RabbitMQ-style topic exchange routing for job dispatch.
"""

from .TopicExchange import RoutingKey, TopicExchange

__all__ = ["RoutingKey", "TopicExchange"]
