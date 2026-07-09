"""Laravel-style Resource Collection for transforming lists into API responses."""

from __future__ import annotations

from typing import Any


class ResourceCollection:
    """Transform a collection of items using a JsonResource class.

    Usage::

        return ResourceCollection(items, ExampleResource).to_response(response)
        # or via the class method shortcut:
        return ExampleResource.collection(items).to_response(response)
    """

    wrap = "data"

    def __init__(self, items: Any, resource_class: type | None = None):
        self.items = items if items is not None else []
        self.resource_class = resource_class
        self._additional: dict[str, Any] = {}
        self._meta: dict[str, Any] = {}
        self._status: int = 200
        self._headers: dict[str, str] = {}

    def to_array(self, request=None) -> list:
        """Transform each item using the resource class."""
        if self.resource_class is None:
            return [
                item.serialize() if hasattr(item, "serialize") else item
                for item in self.items
            ]
        return [self.resource_class(item).to_array(request) for item in self.items]

    def with_status(self, status: int) -> ResourceCollection:
        self._status = status
        return self

    def with_headers(self, headers: dict[str, str]) -> ResourceCollection:
        self._headers.update(headers)
        return self

    def additional(self, data: dict[str, Any]) -> ResourceCollection:
        self._additional.update(data)
        return self

    def with_meta(self, meta: dict[str, Any]) -> ResourceCollection:
        self._meta.update(meta)
        return self

    def resolve(self, request=None) -> dict:
        """Build the full response payload."""
        from .JsonResource import JsonResource

        data = [JsonResource._filter_missing(item) for item in self.to_array(request)]

        if self.wrap:
            payload = {self.wrap: data}
        elif self._meta or self._additional:
            # meta/additional need an object envelope — attaching them to a
            # bare list raised TypeError for wrap-opted-out subclasses.
            # Laravel does the same: metadata forces the "data" wrapper.
            payload = {"data": data}
        else:
            return data

        if self._meta:
            payload["meta"] = self._meta

        if self._additional:
            payload.update(self._additional)

        return payload

    def to_response(self, response) -> Any:
        """Create a framework Response from this collection."""
        payload = self.resolve()
        return response.json(payload, self._status, self._headers or None)

    def __repr__(self) -> str:
        resource_name = self.resource_class.__name__ if self.resource_class else "None"
        return f"ResourceCollection({resource_name}, count={len(self.items)})"
