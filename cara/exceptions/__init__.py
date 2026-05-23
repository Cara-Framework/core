"""
Cara Framework Exception System.

Central exception registry. Explicit re-exports via ``__all__`` — no implicit
wildcard imports, so shadowed/duplicate names are visible and deterministic.

Several type modules define classes with the same short name (e.g. both
``types.eloquent`` and ``types.model`` declare ``ModelNotFoundException``).
The previous wildcard-import layout resolved those collisions implicitly by
"last import wins". We preserve the same canonical winners here so that
callers using the short name get the same class, but we *also* expose the
shadowed copies under prefixed aliases so they are reachable without
reaching into the ``types.*`` sub-packages.

Canonical winners (preserved from the legacy wildcard order):
  - DriverNotRegisteredException   -> types.storage
  - DriverNotFoundException        -> types.model
  - DriverLibraryNotFoundException -> types.scheduling
  - QueueException                 -> types.queue
  - ModelNotFoundException         -> types.model
  - QueryException                 -> types.model
  - MultipleRecordsFoundException  -> types.model
  - InvalidArgumentException       -> types.model
  - RouteRegistrationException     -> types.routing
  - RouteMiddlewareNotFoundException -> types.routing
"""

from .ExceptionProvider import ExceptionProvider
from .types.application import (
    AppException,
    ControllerMethodNotFoundException,
)
from .types.application import (
    RouteRegistrationException as AppRouteRegistrationException,
)
from .types.authentication import (
    ApiKeyInvalidException,
    AuthenticationConfigurationException,
    AuthenticationException,
    InvalidTokenException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
    UserNotFoundException,
)
from .types.authorization import (
    AuthorizationException,
    AuthorizationFailedException,
)
from .types.base import CaraException
from .types.broadcasting import (
    BroadcastingChannelException,
    BroadcastingConfigurationException,
    BroadcastingConnectionException,
    BroadcastingDriverNotFoundException,
    BroadcastingException,
)
from .types.cache import CacheConfigurationException
from .types.cache import (
    DriverNotRegisteredException as CacheDriverNotRegisteredException,
)
from .types.configuration import (
    ConfigurationException,
    InvalidConfigurationLocationException,
    InvalidConfigurationSetupException,
)
from .types.container import (
    ContainerException,
    GenericContainerException,
    MissingContainerBindingException,
    StrictContainerException,
)
from .types.driver import DriverException
from .types.driver import (
    DriverLibraryNotFoundException as DriverLibraryNotFoundFromDriver,
)
from .types.driver import (
    DriverNotFoundException as DriverNotFoundFromDriver,
)
from .types.driver import (
    QueueException as DriverQueueException,
)
from .types.eloquent import (
    DatabaseUnavailableException,
    HTTP404Exception,
    ConnectionNotRegisteredException,
    ORMException,
)
from .types.eloquent import (
    DriverNotFoundException as EloquentDriverNotFoundException,
)
from .types.eloquent import (
    InvalidArgumentException as EloquentInvalidArgumentException,
)
from .types.eloquent import (
    ModelNotFoundException as EloquentModelNotFoundException,
)
from .types.eloquent import (
    MultipleRecordsFoundException as EloquentMultipleRecordsFoundException,
)
from .types.eloquent import (
    QueryException as EloquentQueryException,
)
from .types.encryption import EncryptionException
from .types.event import (
    EventDispatchCycleException,
    EventNameConflictException,
    ListenerNotFoundException,
)
from .types.http import (
    BadRequestException,
    HttpException,
    MethodNotAllowedException,
    ResponseException,
    RouteNotFoundException,
    ServiceUnavailableException,
)
from .types.http import (
    RouteMiddlewareNotFoundException as HttpRouteMiddlewareNotFoundException,
)
from .types.loader import (
    LoaderException,
    LoaderNotFoundException,
)
from .types.mail import (
    MailConfigurationException,
    MailDriverException,
    MailException,
    MailSendException,
)
from .types.middleware import (
    MiddlewareException,
    MiddlewareNotFoundException,
)

# Canonical shared names come from ``types.model`` (matches legacy last-wins).
from .types.model import (
    DriverNotFoundException,
    InvalidArgumentException,
    ModelException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
    QueryException,
)
from .types.queue import (
    QueueConfigurationException,
    QueueException,
)
from .types.queue import (
    DriverLibraryNotFoundException as QueueDriverLibraryNotFoundException,
)
from .types.queue import (
    DriverNotRegisteredException as QueueDriverNotRegisteredException,
)
from .types.rates import RateLimitConfigurationException
from .types.routing import (
    RouteException,
    RouteMiddlewareNotFoundException,
    RouteRegistrationException,
)
from .types.scheduling import (
    DriverLibraryNotFoundException,
    SchedulingConfigurationException,
    SchedulingException,
)
from .types.storage import (
    DriverNotRegisteredException,
    KeyNotFoundException,
    StorageConfigurationException,
    StorageException,
)
from .types.validation import (
    InvalidRuleFormatException,
    RuleNotFoundException,
    ValidationException,
)
from .types.websocket import WebSocketException

__all__ = [
    "ApiKeyInvalidException",
    "AppException",
    "AppRouteRegistrationException",
    "AuthenticationConfigurationException",
    "AuthenticationException",
    "AuthorizationException",
    "AuthorizationFailedException",
    "BadRequestException",
    "BroadcastingChannelException",
    "BroadcastingConfigurationException",
    "BroadcastingConnectionException",
    "BroadcastingDriverNotFoundException",
    "BroadcastingException",
    "CacheConfigurationException",
    "CacheDriverNotRegisteredException",
    "CaraException",
    "ConfigurationException",
    "ConnectionNotRegisteredException",
    "ContainerException",
    "ControllerMethodNotFoundException",
    "DatabaseUnavailableException",
    "DriverException",
    "DriverLibraryNotFoundException",
    "DriverLibraryNotFoundFromDriver",
    "DriverNotFoundException",
    "DriverNotFoundFromDriver",
    "DriverNotRegisteredException",
    "DriverQueueException",
    "EloquentDriverNotFoundException",
    "EloquentInvalidArgumentException",
    "EloquentModelNotFoundException",
    "EloquentMultipleRecordsFoundException",
    "EloquentQueryException",
    "EncryptionException",
    "EventDispatchCycleException",
    "EventNameConflictException",
    "ExceptionProvider",
    "GenericContainerException",
    "HTTP404Exception",
    "HttpException",
    "HttpRouteMiddlewareNotFoundException",
    "InvalidArgumentException",
    "InvalidConfigurationLocationException",
    "InvalidConfigurationSetupException",
    "InvalidRuleFormatException",
    "InvalidTokenException",
    "KeyNotFoundException",
    "ListenerNotFoundException",
    "LoaderException",
    "LoaderNotFoundException",
    "MailConfigurationException",
    "MailDriverException",
    "MailException",
    "MailSendException",
    "MethodNotAllowedException",
    "MiddlewareException",
    "MiddlewareNotFoundException",
    "MissingContainerBindingException",
    "ModelException",
    "ModelNotFoundException",
    "MultipleRecordsFoundException",
    "ORMException",
    "QueryException",
    "QueueConfigurationException",
    "QueueDriverLibraryNotFoundException",
    "QueueDriverNotRegisteredException",
    "QueueException",
    "RateLimitConfigurationException",
    "ResponseException",
    "RouteException",
    "RouteMiddlewareNotFoundException",
    "RouteNotFoundException",
    "RouteRegistrationException",
    "RuleNotFoundException",
    "SchedulingConfigurationException",
    "SchedulingException",
    "ServiceUnavailableException",
    "StorageConfigurationException",
    "StorageException",
    "StrictContainerException",
    "TokenBlacklistedException",
    "TokenExpiredException",
    "TokenInvalidException",
    "UserNotFoundException",
    "ValidationException",
    "WebSocketException",
]
