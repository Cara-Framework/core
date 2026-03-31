"""Laravel-style JSON API Resource for transforming models into API responses."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Type, Union

from .MissingValue import MissingValue


class JsonResource:
    """Transform a model or data object into a structured JSON API response.

    Subclass this and override `to_array()` to define the transformation.

    Usage::

        class ProductResource(JsonResource):
            def to_array(self, request) -> dict:
                return {
                    "id": self.resource.public_id,
                    "title": self.resource.title,
                    "price": self.when(
                        self.resource.current_price,
                        lambda: float(self.resource.current_price.price_min),
                    ),
                }

        # Single resource
        return ProductResource(product).to_response(response)

        # Collection
        return ProductResource.collection(products).to_response(response)
    """

    wrap = "data"

    def __init__(self, resource: Any):
        self.resource = resource
        self._additional: Dict[str, Any] = {}
        self._meta: Dict[str, Any] = {}
        self._status: int = 200
        self._headers: Dict[str, str] = {}

    def to_array(self, request=None) -> dict:
        """Transform the resource into a dict.

        Override in subclasses to define the output shape.
        Falls back to model.serialize() if available, otherwise returns the resource as-is.
        """
        if hasattr(self.resource, "serialize"):
            return self.resource.serialize()
        if isinstance(self.resource, dict):
            return self.resource
        return {"data": str(self.resource)}

    def with_status(self, status: int) -> JsonResource:
        """Set the HTTP status code for the response."""
        self._status = status
        return self

    def with_headers(self, headers: Dict[str, str]) -> JsonResource:
        """Set additional response headers."""
        self._headers.update(headers)
        return self

    def additional(self, data: Dict[str, Any]) -> JsonResource:
        """Merge extra top-level keys into the response envelope."""
        self._additional.update(data)
        return self

    def with_meta(self, meta: Dict[str, Any]) -> JsonResource:
        """Add pagination or other meta to the response."""
        self._meta.update(meta)
        return self

    # ── Conditional helpers ───────────────────────────────────────────────

    @staticmethod
    def when(condition: Any, value: Any, default: Any = None) -> Any:
        """Include an attribute only when a condition is truthy.

        Args:
            condition: Evaluated for truthiness.
            value: Returned (or called if callable) when condition is truthy.
            default: Returned when condition is falsy.  ``None`` means
                     the key will still appear with a ``None`` value.
                     Pass ``MissingValue()`` to omit the key entirely.
        """
        if condition:
            return value() if callable(value) else value
        if default is None:
            return None
        return default() if callable(default) else default

    @staticmethod
    def when_loaded(resource: Any, relation: str, value_fn: Callable = None) -> Any:
        """Include a relation only when it has been eager-loaded.

        Args:
            resource: The model instance to check.
            relation: Relation name to check for.
            value_fn: Optional callable that receives the loaded relation
                      and returns the value to include.  If omitted the
                      raw relation value is returned.

        Returns:
            The transformed relation value or ``MissingValue`` to omit.
        """
        related = getattr(resource, relation, MissingValue())
        if isinstance(related, MissingValue):
            return related
        if related is None:
            return None
        if value_fn is not None:
            return value_fn(related)
        return related

    # ── Serialization ─────────────────────────────────────────────────────

    def resolve(self, request=None) -> dict:
        """Build the full response payload with wrapping and meta."""
        data = self._filter_missing(self.to_array(request))

        if self.wrap:
            payload = {self.wrap: data}
        else:
            payload = data if isinstance(data, dict) else {self.wrap or "data": data}

        if self._meta:
            payload["meta"] = self._meta

        if self._additional:
            payload.update(self._additional)

        return payload

    def to_response(self, response) -> Any:
        """Create a framework Response from this resource.

        This is the primary method controllers should call::

            return ProductResource(product).to_response(response)
        """
        payload = self.resolve()
        resp = response.json(payload, self._status, self._headers or None)
        return resp

    # ── Collection factory ────────────────────────────────────────────────

    @classmethod
    def collection(
        cls,
        items: Any,
        meta: Dict[str, Any] = None,
    ) -> ResourceCollection:
        """Create a ResourceCollection using this resource class."""
        from .ResourceCollection import ResourceCollection

        coll = ResourceCollection(items, cls)
        if meta:
            coll.with_meta(meta)
        return coll

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _filter_missing(data: Any) -> Any:
        """Recursively strip MissingValue entries from dicts and lists."""
        if isinstance(data, dict):
            return {
                k: JsonResource._filter_missing(v)
                for k, v in data.items()
                if not isinstance(v, MissingValue)
            }
        if isinstance(data, (list, tuple)):
            return [
                JsonResource._filter_missing(item)
                for item in data
                if not isinstance(item, MissingValue)
            ]
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.resource!r})"
