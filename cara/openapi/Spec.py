"""Assemble the OpenAPI document and write it as a checked-in artifact.

The success envelope is framework-owned (``response.envelope`` /
``response.paginated`` always emit ``{data, meta}``), so the per-route variation
is exactly one thing: which resource schema fills ``data``, and whether it is a
row or a page. Everything an application genuinely owns — the document title,
the shape of its ``meta`` and error components, which actions page by cursor —
arrives as an argument. No application module is imported here.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .ControllerContract import ControllerContract
from .ControllerResponse import ControllerResponse
from .EnvelopeNames import EnvelopeNames
from .FormRequestSchemaExtractor import request_query_parameters
from .Inference import resource_ref
from .Routes import openapi_path, path_params
from .SpecInfo import SpecInfo

# Query parameters every cursor-paged operation accepts. Bounds mirror the
# framework's own cursor rules, so a route cannot advertise a page size the
# request layer would reject.
CURSOR_PAGE_PARAMETERS: tuple[dict[str, Any], ...] = (
    {
        "name": "limit",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 1, "maximum": 100},
    },
    {
        "name": "cursor",
        "in": "query",
        "required": False,
        "schema": {"type": "string", "minLength": 1, "maxLength": 4096},
    },
)


def build_spec(
    *,
    info: SpecInfo,
    schemas: dict[str, dict[str, Any]],
    mapping: dict[str, tuple[str, bool]],
    routes: list[dict[str, Any]],
    envelope_components: dict[str, Any],
    meta_mapping: dict[str, str] | None = None,
    cursor_actions: set[str] | frozenset[str] = frozenset(),
    envelope: EnvelopeNames = EnvelopeNames(),
    generation_extra: dict[str, Any] | None = None,
    request_schemas: dict[str, dict[str, Any]] | None = None,
    controller_contracts: dict[str, ControllerContract] | None = None,
    security_schemes: dict[str, dict[str, Any]] | None = None,
    middleware_security: dict[str, list[dict[str, list[str]]]] | None = None,
    middleware_error_statuses: dict[str, tuple[int, ...] | list[int]] | None = None,
) -> dict[str, Any]:
    """Build the OpenAPI document from already-extracted inputs.

    ``schemas`` are the resource component schemas, ``mapping`` resolves
    ``Controller@action -> (resource, is_list)``, ``routes`` is the flattened
    route table, and ``envelope_components`` are the application's own shared
    components (meta / error bodies), appended after the resource schemas in
    the order the application declares them.
    """
    request_schemas = request_schemas or {}
    controller_contracts = controller_contracts or {}
    middleware_security = middleware_security or {}
    middleware_error_statuses = middleware_error_statuses or {}
    meta_mapping = meta_mapping or {}

    components: dict[str, Any] = dict(sorted(schemas.items()))
    components.update(dict(sorted(request_schemas.items())))
    components.update(envelope_components)

    paths: dict[str, Any] = {}
    mapped_routes = 0
    for route in routes:
        op_path = openapi_path(route["path"])
        action_key = f"{route['controller']}@{route['action']}"
        resolved = mapping.get(action_key)
        is_cursor_page = action_key in cursor_actions

        operation: dict[str, Any] = {
            "operationId": route["name"] or action_key,
            "tags": [route["controller"]],
            "summary": f"{route['controller']}.{route['action']}",
        }
        middleware = list(route.get("middleware", []))
        if middleware:
            operation["x-middleware"] = middleware
        if security := _security_requirements(middleware, middleware_security):
            operation["security"] = security

        params = path_params(op_path)
        contract = controller_contracts.get(action_key)
        request_names = contract.requests if contract is not None else ()
        attached_request = False
        query_request = route["method"] in {"GET", "HEAD"} or (
            request_names
            and all(
                request_schemas.get(name, {}).get("x-cara-location") == "query"
                for name in request_names
                if name in request_schemas
            )
        )
        if request_names and query_request:
            seen_query = {parameter["name"] for parameter in params}
            for request_name in request_names:
                schema = request_schemas.get(request_name)
                if schema is None:
                    continue
                attached_request = True
                for parameter in request_query_parameters(schema):
                    if parameter["name"] not in seen_query:
                        params.append(parameter)
                        seen_query.add(parameter["name"])
        elif request_names:
            body_names = [name for name in request_names if name in request_schemas]
            if body_names:
                attached_request = True
                body_request_schema: dict[str, Any]
                if len(body_names) == 1:
                    body_request_schema = resource_ref(body_names[0])
                else:
                    body_request_schema = {
                        "oneOf": [resource_ref(name) for name in body_names]
                    }
                content_types = {
                    content_type
                    for name in body_names
                    for content_type in request_schemas[name].get(
                        "x-cara-content-types", ["application/json"]
                    )
                }
                operation["requestBody"] = {
                    "required": any(
                        bool(request_schemas[name].get("required")) for name in body_names
                    ),
                    "content": {
                        content_type: {"schema": body_request_schema}
                        for content_type in sorted(content_types)
                    },
                }
        if is_cursor_page and not attached_request:
            params.extend(dict(param) for param in CURSOR_PAGE_PARAMETERS)
        if params:
            operation["parameters"] = params

        if resolved is not None:
            resource_name, is_list = resolved
            data_schema: dict[str, Any] = resource_ref(resource_name)
            if is_list:
                data_schema = {"type": "array", "items": resource_ref(resource_name)}
            mapped_routes += 1
        else:
            # Honest: no resource maps to this action (raw dict / no serializer).
            # Still document the envelope shape with a permissive payload.
            data_schema = {"type": "array", "items": {}} if is_cursor_page else {}

        mapped_meta = meta_mapping.get(action_key)
        if is_cursor_page and mapped_meta is not None:
            meta_schema: dict[str, Any] = {
                "allOf": [
                    resource_ref(envelope.cursor_meta),
                    resource_ref(mapped_meta),
                ]
            }
        else:
            meta_schema = resource_ref(
                mapped_meta or (envelope.cursor_meta if is_cursor_page else envelope.meta)
            )

        body_schema = {
            "type": "object",
            "required": ["data", "meta"] if is_cursor_page else ["data"],
            "properties": {
                "data": data_schema,
                "meta": meta_schema,
            },
        }

        variants = (
            contract.responses
            if contract is not None
            else (ControllerResponse(200, "envelope"),)
        )
        responses: dict[str, Any] = {}
        for variant in variants:
            responses[str(variant.status)] = _controller_response(
                variant, body_schema=body_schema
            )
        middleware_statuses = {
            status
            for name in middleware
            for status in _middleware_lookup(middleware_error_statuses, name) or ()
        }
        for status in sorted(middleware_statuses):
            responses.setdefault(
                str(status), _error_response(status, envelope.error, "Middleware error.")
            )
        responses.setdefault(
            "422",
            _error_response(422, envelope.error, "Validation / domain error."),
        )
        operation["responses"] = dict(
            sorted(responses.items(), key=lambda item: int(item[0]))
        )
        paths.setdefault(op_path, {})[route["method"].lower()] = operation

    generation: dict[str, Any] = {
        "resources_introspected": len(schemas),
        "routes_total": len(routes),
        "routes_with_response_schema": mapped_routes,
        "request_schemas_introspected": len(request_schemas),
        "routes_with_request_schema": sum(
            1
            for route in routes
            if (
                contract := controller_contracts.get(
                    f"{route['controller']}@{route['action']}"
                )
            )
            and any(name in request_schemas for name in contract.requests)
        ),
    }
    generation.update(generation_extra or {})

    return {
        "openapi": info.openapi_version,
        "info": {
            "title": info.title,
            "version": info.version,
            "description": info.description,
        },
        "paths": dict(sorted(paths.items())),
        "components": {
            "schemas": components,
            **({"securitySchemes": security_schemes} if security_schemes else {}),
        },
        "x-generation": generation,
    }


def _middleware_lookup(mapping: dict[str, Any], name: str) -> Any:
    """Resolve an exact middleware alias, then its ``family:*`` fallback."""
    return mapping.get(name, mapping.get(name.partition(":")[0] + ":*"))


def _security_requirements(
    middleware: list[str],
    mapping: dict[str, list[dict[str, list[str]]]],
) -> list[dict[str, list[str]]]:
    """Compose middleware requirements using OpenAPI's OR-of-AND grammar."""
    requirements: list[dict[str, list[str]]] = [{}]
    matched = False
    for name in middleware:
        alternatives = _middleware_lookup(mapping, name)
        if not alternatives:
            continue
        matched = True
        combined: list[dict[str, list[str]]] = []
        for current in requirements:
            for alternative in alternatives:
                requirement = {key: list(scopes) for key, scopes in current.items()}
                for scheme, scopes in alternative.items():
                    requirement[scheme] = list(
                        dict.fromkeys(requirement.get(scheme, []) + list(scopes))
                    )
                combined.append(requirement)
        requirements = combined
    if not matched:
        return []

    unique: list[dict[str, list[str]]] = []
    for requirement in requirements:
        if requirement not in unique:
            unique.append(requirement)
    # If one alternative requires every scheme of another plus more, it is
    # redundant. This also simplifies ``auth`` + stricter ``auth.session`` to
    # the session-capable bearer requirement.
    return [
        requirement
        for requirement in unique
        if not any(
            set(other) < set(requirement)
            and all(requirement[key] == scopes for key, scopes in other.items())
            for other in unique
        )
    ]


def _error_response(status: int, error_component: str, description: str) -> dict:
    return {
        "description": description,
        "content": {"application/json": {"schema": resource_ref(error_component)}},
        "x-http-status": status,
    }


def _controller_response(
    variant: ControllerResponse, *, body_schema: dict[str, Any]
) -> dict[str, Any]:
    if variant.kind == "empty":
        return {"description": "No content."}
    if variant.kind == "redirect":
        return {
            "description": "Redirect.",
            "headers": {
                "Location": {
                    "description": "Redirect destination.",
                    "schema": {"type": "string", "format": "uri-reference"},
                }
            },
        }
    if variant.kind == "binary":
        return {
            "description": "File or streaming response.",
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        }
    if variant.kind == "json":
        return {
            "description": "JSON response.",
            "content": {"application/json": {"schema": {}}},
        }
    return {
        "description": "Success envelope.",
        "content": {"application/json": {"schema": body_schema}},
    }


def render(spec: dict[str, Any]) -> str:
    """Serialize a spec to the exact bytes the artifact holds."""
    return json.dumps(spec, indent=2, sort_keys=False) + "\n"


def emit(
    spec: dict[str, Any],
    *,
    out: Path,
    display_root: Path,
    regenerate_hint: str,
    argv: list[str],
) -> int:
    """Run the shared write / ``--check`` / ``--stdout`` command surface.

    ``--check`` is the CI lane: it fails when the committed artifact no longer
    matches what the source produces, which is the whole point of committing a
    generated contract.
    """
    rendered = render(spec)
    generation = spec["x-generation"]

    if "--stdout" in argv:
        sys.stdout.write(rendered)
        return 0

    if "--check" in argv:
        current = out.read_text(encoding="utf-8") if out.exists() else ""
        if current != rendered:
            print(
                f"STALE: {out.name} is out of date with the Resource "
                f"layer. Run: {regenerate_hint}",
                file=sys.stderr,
            )
            return 1
        print(
            f"✓ openapi spec in sync "
            f"({generation['resources_introspected']} resources, "
            f"{generation['routes_with_response_schema']}/"
            f"{generation['routes_total']} routes "
            f"with a response schema)"
        )
        return 0

    out.write_text(rendered, encoding="utf-8")
    print(
        f"✓ wrote {out.relative_to(display_root)}  "
        f"({generation['resources_introspected']} resources introspected, "
        f"{generation['routes_with_response_schema']}/"
        f"{generation['routes_total']} routes "
        f"got a response-body schema)"
    )
    return 0
