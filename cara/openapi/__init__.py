"""Generate an OpenAPI document from the resource layer, without booting the app.

The response shape of an endpoint is not written down anywhere a client can
read: it is whatever the resource class returns at runtime. This package reads
that shape statically — resource serializers, controller-to-resource wiring,
and the generated route table — and assembles a committed
``openapi.generated.json`` a CI lane can diff, so a renamed key fails a build
instead of a browser.

The error half of the same contract is read here too: a client branches on the
``type`` discriminator rather than on a status, and
:class:`~cara.openapi.Errors.ErrorContractExtractor` collects every one an
application can emit — starting from the framework's own exception, handler and
middleware surface, which no application has to name and therefore cannot
forget.

Everything here is framework vocabulary: the ``opt_*`` coercion helpers, the
``Resource(...)`` / ``Resource.collection(...)`` composition contract, the
``{data, meta}`` envelope, the generated route shard layout and the framework's
own error emitters. Everything an application owns — where its resources,
controllers and exceptions live, its document title, its ``meta`` and error
components, its own serialization and pagination helpers — arrives as an
argument.

Usage::

    from cara.openapi import (
        ControllerActionMapper,
        ErrorContractExtractor,
        ResourceSchemaExtractor,
        SpecInfo,
        build_spec,
        cursor_paginated_actions,
        emit,
        parse_routes,
        route_shard_paths,
    )

    schemas = ResourceSchemaExtractor(RESOURCES_DIR).extract()
    mapping = ControllerActionMapper(CONTROLLERS_DIR, set(schemas)).map()
    routes = parse_routes(route_shard_paths(ROOT), base_prefix="/api")
    errors = ErrorContractExtractor(APP_ERROR_ROOTS)
    spec = build_spec(
        info=SpecInfo(title=..., description=...),
        schemas=schemas,
        mapping=mapping,
        routes=routes,
        envelope_components=envelope_components(errors.discriminators()),
    )
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ANY_SCHEMA": (".Inference", "ANY_SCHEMA"),
    "CALL_TYPE": (".Inference", "CALL_TYPE"),
    "CAST_TYPE": (".Inference", "CAST_TYPE"),
    "CURSOR_PAGE_PARAMETERS": (".Spec", "CURSOR_PAGE_PARAMETERS"),
    "ConflictingErrorStatus": (".ConflictingErrorStatus", "ConflictingErrorStatus"),
    "ControllerActionMapper": (".ControllerActionMapper", "ControllerActionMapper"),
    "ControllerContract": (".ControllerContract", "ControllerContract"),
    "ControllerContractExtractor": (
        ".ControllerContractExtractor",
        "ControllerContractExtractor",
    ),
    "ControllerMetaMapper": (".ControllerMetaMapper", "ControllerMetaMapper"),
    "ControllerResponse": (".ControllerResponse", "ControllerResponse"),
    "DYNAMIC_BASE_CALLS": (".Inference", "DYNAMIC_BASE_CALLS"),
    "EnvelopeNames": (".EnvelopeNames", "EnvelopeNames"),
    "ErrorContractExtractor": (".ErrorContractExtractor", "ErrorContractExtractor"),
    "ErrorDiscriminator": (".ErrorDiscriminator", "ErrorDiscriminator"),
    "FRAMEWORK_ERROR_ROOTS": (".Errors", "FRAMEWORK_ERROR_ROOTS"),
    "FormRequestSchemaExtractor": (
        ".FormRequestSchemaExtractor",
        "FormRequestSchemaExtractor",
    ),
    "OPT_TYPE": (".Inference", "OPT_TYPE"),
    "PASSTHROUGH_WRAPPERS": (".Inference", "PASSTHROUGH_WRAPPERS"),
    "ResourceSchemaExtractor": (".ResourceSchemaExtractor", "ResourceSchemaExtractor"),
    "SERIALIZER_METHODS": (".Inference", "SERIALIZER_METHODS"),
    "STATUS_HINTS": (".Errors", "STATUS_HINTS"),
    "SpecInfo": (".SpecInfo", "SpecInfo"),
    "UnknownDeclaredResource": (".UnknownDeclaredResource", "UnknownDeclaredResource"),
    "UntypedErrorResponse": (".UntypedErrorResponse", "UntypedErrorResponse"),
    "build_spec": (".Spec", "build_spec"),
    "const_schema": (".Inference", "const_schema"),
    "controller_action_functions": (".Controllers", "controller_action_functions"),
    "cursor_paginated_actions": (".Controllers", "cursor_paginated_actions"),
    "dict_payload": (".Inference", "dict_payload"),
    "emit": (".Spec", "emit"),
    "infer_value_schema": (".Inference", "infer_value_schema"),
    "openapi_path": (".Routes", "openapi_path"),
    "parse_routes": (".Routes", "parse_routes"),
    "passthrough_var": (".Inference", "passthrough_var"),
    "path_params": (".Routes", "path_params"),
    "render": (".Spec", "render"),
    "request_query_parameters": (
        ".FormRequestSchemaExtractor",
        "request_query_parameters",
    ),
    "resource_ref": (".Inference", "resource_ref"),
    "route_shard_paths": (".Routes", "route_shard_paths"),
    "route_shard_source": (".Routes", "route_shard_source"),
    "unify": (".Inference", "unify"),
}

__all__ = [
    "ANY_SCHEMA",
    "CALL_TYPE",
    "CAST_TYPE",
    "CURSOR_PAGE_PARAMETERS",
    "ConflictingErrorStatus",
    "ControllerActionMapper",
    "ControllerContract",
    "ControllerContractExtractor",
    "ControllerMetaMapper",
    "ControllerResponse",
    "DYNAMIC_BASE_CALLS",
    "EnvelopeNames",
    "ErrorContractExtractor",
    "ErrorDiscriminator",
    "FRAMEWORK_ERROR_ROOTS",
    "FormRequestSchemaExtractor",
    "OPT_TYPE",
    "PASSTHROUGH_WRAPPERS",
    "ResourceSchemaExtractor",
    "SERIALIZER_METHODS",
    "STATUS_HINTS",
    "SpecInfo",
    "UnknownDeclaredResource",
    "UntypedErrorResponse",
    "build_spec",
    "const_schema",
    "controller_action_functions",
    "cursor_paginated_actions",
    "dict_payload",
    "emit",
    "infer_value_schema",
    "openapi_path",
    "parse_routes",
    "passthrough_var",
    "path_params",
    "render",
    "request_query_parameters",
    "resource_ref",
    "route_shard_paths",
    "route_shard_source",
    "unify",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
