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
    
    def __init__(self, exchange_name: str = "cheapa.events"):
        """Initialize topic exchange."""
        self.exchange_name = exchange_name
        self.bindings: Dict[str, QueueBinding] = {}
        self._queue_patterns: Dict[str, List[str]] = {}
        
        # Pre-defined domain priorities
        self.priority_levels = {
            'critical': 0,
            'high': 1,
            'default': 2,
            'low': 3,
            'bulk': 4
        }
        
        # Initialize with default bindings
        self._setup_default_bindings()
        
        Log.info(f"TopicExchange initialized: {exchange_name}", category="cara.queue.exchange")
    
    def _setup_default_bindings(self):
        """Setup default queue bindings for common patterns."""
        default_bindings = [
            # Enrichment queues
            ("enrichment.critical", "enrichment.*.critical"),
            ("enrichment.high", "enrichment.*.high"),
            ("enrichment.default", "enrichment.*.default"),
            ("enrichment.low", "enrichment.*.low"),
            
            # Validation queues
            ("validation.critical", "validation.*.critical"),
            ("validation.high", "validation.*.high"),
            ("validation.default", "validation.*.default"),
            ("validation.low", "validation.*.low"),
            
            # Notification queues
            ("notification.email", "notification.email.*"),
            ("notification.sms", "notification.sms.*"),
            ("notification.push", "notification.push.*"),
            
            # Reporting queues
            ("reporting.default", "reporting.*.default"),
            ("reporting.low", "reporting.*.low"),
            ("reporting.bulk", "reporting.*.bulk"),
            
            # System queues
            ("system.critical", "system.*.critical"),
            ("system.maintenance", "system.maintenance.*"),
        ]
        
        for queue_name, pattern in default_bindings:
            self.bind_queue(queue_name, pattern)
    
    def bind_queue(self, queue_name: str, routing_pattern: str) -> None:
        """
        Bind a queue to the exchange with routing pattern.
        
        Args:
            queue_name: Name of the queue (e.g., "enrichment.high")
            routing_pattern: Routing key pattern (e.g., "enrichment.*.high")
        """
        # Parse queue name components
        parts = queue_name.split('.')
        if len(parts) != 2:
            raise ValueError(f"Queue name must follow domain.priority format: {queue_name}")
        
        domain, priority = parts[0], parts[1]
        
        # Create binding
        binding = QueueBinding(
            queue_name=queue_name,
            routing_pattern=routing_pattern,
            domain=domain,
            subtype="*",  # Wildcard for pattern matching
            priority=priority
        )
        
        self.bindings[queue_name] = binding
        
        # Store pattern for matching
        if domain not in self._queue_patterns:
            self._queue_patterns[domain] = []
        self._queue_patterns[domain].append(routing_pattern)
        
        Log.info(
            f"Queue bound: {queue_name} -> {routing_pattern}", 
            category="cara.queue.exchange"
        )
    
    def get_matching_queues(self, routing_key: str) -> List[str]:
        """
        Get all queues that match the given routing key.
        
        Args:
            routing_key: Full routing key (e.g., "enrichment.product.high")
            
        Returns:
            List of matching queue names
        """
        matching_queues = []
        
        for queue_name, binding in self.bindings.items():
            if self._matches_pattern(routing_key, binding.routing_pattern):
                matching_queues.append(queue_name)
        
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