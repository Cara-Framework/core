"""
RabbitMQ Topic Exchange System for Cara Framework.

Implements domain.subtype.priority routing pattern with automatic
queue binding and message routing via routing keys.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from cara.facades import Log


@dataclass
class QueueBinding:
    """Queue binding configuration."""
    queue_name: str
    routing_pattern: str
    domain: str
    subtype: str
    priority: str


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
    def parse(cls, routing_key: str) -> "RoutingKey":
        """Parse routing key string into components."""
        parts = routing_key.split('.')
        if len(parts) != 3:
            raise ValueError(f"Invalid routing key format: {routing_key}. Expected: domain.subtype.priority")
        
        return cls(domain=parts[0], subtype=parts[1], priority=parts[2])


class TopicExchange:
    """
    RabbitMQ Topic Exchange for Cara Framework.
    
    Features:
    - Automatic queue creation and binding
    - Routing key pattern matching
    - Domain-based job categorization
    - Priority-based message routing
    - Singleton pattern to prevent re-initialization
    
    Usage:
        exchange = TopicExchange("cheapa.events")
        
        # Define queue bindings
        exchange.bind_queue("enrichment.default", "enrichment.*.default")
        exchange.bind_queue("enrichment.high", "enrichment.*.high")
        exchange.bind_queue("validation.default", "validation.*.default")
        
        # Dispatch with routing key
        exchange.dispatch_job(
            routing_key="enrichment.product.high",
            job_instance=my_job,
            payload={"product_id": 123}
        )
    """
    
    _instances = {}  # Class-level instances cache
    
    def __new__(cls, exchange_name: str = "default.events"):
        """Singleton pattern to prevent multiple instances."""
        if exchange_name not in cls._instances:
            cls._instances[exchange_name] = super().__new__(cls)
        return cls._instances[exchange_name]
    
    def __init__(self, exchange_name: str = "default.events"):
        """
        Initialize TopicExchange.
        
        Args:
            exchange_name: Name of the RabbitMQ topic exchange
        """
        # Prevent re-initialization
        if hasattr(self, '_initialized'):
            return
            
        self.exchange_name = exchange_name
        self.bindings: Dict[str, QueueBinding] = {}
        self.queue_bindings: Dict[str, List[str]] = {}
        self._queue_patterns: Dict[str, List[str]] = {}
        self._logged_bindings = set()
        
        # Auto-bind standard queues
        self._setup_default_bindings()
        
        Log.info(
            f"TopicExchange initialized: {exchange_name}",
            category="cara.queue.exchange"
        )
        
        self._initialized = True
    
    def _setup_default_bindings(self):
        """Setup queue bindings from app configuration."""
        # Load app-specific bindings from config (required)
        from cara.configuration import config
        app_bindings = config("queue.topic_exchange_bindings", None)
        
        if not app_bindings:
            raise ValueError(
                "TOPIC_EXCHANGE_BINDINGS not found in queue config. "
                "Please define your queue bindings in config/queue.py"
            )
        
        default_bindings = app_bindings
        
        for queue_name, pattern in default_bindings:
            self.bind_queue(queue_name, pattern)
    
    def bind_queue(self, queue_name: str, routing_pattern: str) -> None:
        """
        Bind a queue to a routing pattern.
        
        Args:
            queue_name: Name of the queue to bind
            routing_pattern: Routing pattern (e.g., "enrichment.*.high")
        """
        if queue_name not in self.queue_bindings:
            self.queue_bindings[queue_name] = []
        
        if routing_pattern not in self.queue_bindings[queue_name]:
            self.queue_bindings[queue_name].append(routing_pattern)
            # Only log once when binding is first created
            if not hasattr(self, '_logged_bindings'):
                self._logged_bindings = set()
            
            binding_key = f"{queue_name}->{routing_pattern}"
            if binding_key not in self._logged_bindings:
                # Only log in debug mode to reduce spam
                from cara.configuration import config
                if config("app.debug", False):
                    Log.info(
                        f"Queue bound: {queue_name} -> {routing_pattern}",
                        category="cara.queue.exchange"
                    )
                self._logged_bindings.add(binding_key)
    
    def get_matching_queues(self, routing_key: str) -> List[str]:
        """
        Get queues that match the routing key.
        
        Args:
            routing_key: Full routing key (e.g., "enrichment.product.high")
            
        Returns:
            List of matching queue names
        """
        matching_queues = []
        
        for queue_name, patterns in self.queue_bindings.items():
            for pattern in patterns:
                if self._matches_pattern(routing_key, pattern):
                    matching_queues.append(queue_name)
                    break  # Don't add same queue multiple times
        
        return matching_queues
    
    def _matches_pattern(self, routing_key: str, pattern: str) -> bool:
        """
        Check if routing key matches the pattern.
        
        Supports:
        - * matches exactly one word
        - # matches zero or more words (not implemented for simplicity)
        """
        routing_parts = routing_key.split('.')
        pattern_parts = pattern.split('.')
        
        if len(routing_parts) != len(pattern_parts):
            return False
        
        for routing_part, pattern_part in zip(routing_parts, pattern_parts):
            if pattern_part != '*' and pattern_part != routing_part:
                return False
        
        return True
    
    def dispatch_job(self, routing_key: str, job_instance, payload: Dict = None) -> Optional[str]:
        """
        Dispatch job to appropriate queue based on routing key.
        
        Args:
            routing_key: Routing key (e.g., "enrichment.product.high")
            job_instance: Job instance to dispatch
            payload: Additional payload data
            
        Returns:
            Job ID if successful, None otherwise
        """
        try:
            # Parse routing key
            parsed_key = RoutingKey.parse(routing_key)
            
            # Find matching queues
            matching_queues = self.get_matching_queues(routing_key)
            
            if not matching_queues:
                Log.warning(
                    f"No queues match routing key: {routing_key}",
                    category="cara.queue.exchange"
                )
                return None
            
            # Use first matching queue (highest priority)
            target_queue = self._select_best_queue(matching_queues, parsed_key.priority)
            
            # Set job queue and routing info
            if hasattr(job_instance, 'queue'):
                job_instance.queue = target_queue
            if hasattr(job_instance, 'routing_key'):
                job_instance.routing_key = routing_key
            
            # Dispatch via Queue facade
            from cara.facades import Queue
            job_id = Queue.push(job_instance)
            
            # Log successful dispatch
            from cara.configuration import config
            if config("app.debug", False):
                Log.info(
                    f"Job dispatched: {routing_key} -> {target_queue} [{job_id}]",
                    category="cara.queue.exchange"
                )
            
            return str(job_id)
            
        except Exception as e:
            Log.error(
                f"Job dispatch failed: {routing_key} - {str(e)}",
                category="cara.queue.exchange"
            )
            return None
    
    def _select_best_queue(self, matching_queues: List[str], priority: str) -> str:
        """
        Select the best queue from matching queues based on priority.
        
        Args:
            matching_queues: List of matching queue names
            priority: Requested priority level
            
        Returns:
            Best matching queue name
        """
        # Prefer exact priority match
        for queue in matching_queues:
            if queue.endswith(f".{priority}"):
                return queue
        
        # Fallback to first available queue
        return matching_queues[0]
    
    def get_queue_info(self) -> Dict[str, Dict]:
        """Get information about all bound queues."""
        queue_info = {}
        
        for queue_name, binding in self.bindings.items():
            queue_info[queue_name] = {
                'routing_pattern': binding.routing_pattern,
                'domain': binding.domain,
                'priority': binding.priority,
                'exchange': self.exchange_name
            }
        
        return queue_info
    
    def list_queues_for_domain(self, domain: str) -> List[str]:
        """Get all queues for a specific domain."""
        return [
            queue_name for queue_name, binding in self.bindings.items()
            if binding.domain == domain
        ] 