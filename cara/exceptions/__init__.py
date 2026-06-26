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

from __future__ import annotations

# NOTE: ``ExceptionProvider`` is intentionally NOT imported at module load
# time — see the lazy ``__getattr__`` at the bottom. Its transitive chain
# (foundation → Application → support → Collection) imports BOTH
# ``cara.exceptions.InvalidArgumentException`` (needs the names below bound
# first) AND ``cara.foundation.Provider`` (a module that is itself only
# partially initialised whenever ``cara.exceptions`` is imported DURING
# foundation/environment boot). Importing ExceptionProvider eagerly — at the
# top OR the bottom — therefore deadlocks one cycle or the other depending on
# the entry point. A PEP 562 lazy import sidesteps both: the provider (and its
# foundation dependency) is only resolved when something actually accesses
# ``cara.exceptions.ExceptionProvider``, never during this module's own load.
from .types.application import (
    AppException,
    ControllerMethodNotFoundException,
)
from .types.application import (
    RouteRegistrationException as AppRouteRegistrationException,
)
from .types.authentication import (
    AccountLockedException,
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
from .types.Base import CaraException
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
from .types.Eloquent import (
    DatabaseUnavailableException,
    Http404Exception,
    ConnectionNotRegisteredException,
    ORMException,
)
from .types.Eloquent import (
    DriverNotFoundException as EloquentDriverNotFoundException,
)
from .types.Eloquent import (
    InvalidArgumentException as EloquentInvalidArgumentException,
)
from .types.Eloquent import (
    ModelNotFoundException as EloquentModelNotFoundException,
)
from .types.Eloquent import (
    MultipleRecordsFoundException as EloquentMultipleRecordsFoundException,
)
from .types.Eloquent import (
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

# Canonical shared names come from ``types.ModelExceptions`` (matches legacy last-wins).
from .types.ModelExceptions import (
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
    "AccountLockedException",
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
    "Http404Exception",
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

# Eager import LAST (after every exception name above is bound). A PEP 562
# lazy ``__getattr__`` does NOT work here: the submodule is also named
# ``ExceptionProvider``, so the first ``from .ExceptionProvider import …``
# registers the MODULE as ``cara.exceptions.ExceptionProvider`` and every later
# ``from cara.exceptions import ExceptionProvider`` then resolves to the module
# (Kernel's provider list got a module → ``issubclass() arg 1 must be a class``).
# Binding the CLASS here overrides that. The foundation circular import this
# used to trigger is now broken inside ``ExceptionProvider.py`` itself (it
# imports ``cara.foundation.Provider`` directly), so this eager line is safe
# regardless of whether ``cara.exceptions`` is loaded during foundation boot.
from .ExceptionProvider import ExceptionProvider  # noqa: E402
