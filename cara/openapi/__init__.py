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

from .Controllers import (
    ControllerActionMapper,
    ControllerContract,
    ControllerContractExtractor,
    ControllerResponse,
    UnknownDeclaredResource,
    controller_action_functions,
    cursor_paginated_actions,
)
from .Errors import (
    FRAMEWORK_ERROR_ROOTS,
    STATUS_HINTS,
    ConflictingErrorStatus,
    ErrorContractExtractor,
    ErrorDiscriminator,
    UntypedErrorResponse,
)
from .Inference import (
    ANY_SCHEMA,
    CALL_TYPE,
    CAST_TYPE,
    OPT_TYPE,
    const_schema,
    dict_payload,
    infer_value_schema,
    resource_ref,
    unify,
)
from .Resources import ResourceSchemaExtractor
from .FormRequestSchemaExtractor import (
    FormRequestSchemaExtractor,
    request_query_parameters,
)
from .Routes import (
    openapi_path,
    parse_routes,
    path_params,
    route_shard_paths,
    route_shard_source,
)
from .Spec import (
    CURSOR_PAGE_PARAMETERS,
    EnvelopeNames,
    SpecInfo,
    build_spec,
    emit,
    render,
)

__all__ = [
    "ANY_SCHEMA",
    "CALL_TYPE",
    "CAST_TYPE",
    "CURSOR_PAGE_PARAMETERS",
    "ConflictingErrorStatus",
    "ControllerActionMapper",
    "ControllerContract",
    "ControllerContractExtractor",
    "ControllerResponse",
    "EnvelopeNames",
    "ErrorContractExtractor",
    "ErrorDiscriminator",
    "FRAMEWORK_ERROR_ROOTS",
    "FormRequestSchemaExtractor",
    "OPT_TYPE",
    "ResourceSchemaExtractor",
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
    "path_params",
    "render",
    "request_query_parameters",
    "resource_ref",
    "route_shard_paths",
    "route_shard_source",
    "unify",
]
