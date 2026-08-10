"""
Cara Framework Exception System.

Central exception registry. Explicit re-exports via ``__all__`` — no implicit
wildcard imports, so every public name is visible and deterministic.

**One short name, one class.** This registry used to carry "canonical
winners preserved from the legacy wildcard order": nine exception short
names were declared in two or three ``types.*`` modules, this barrel bound
one copy, ``cara.exceptions.types`` bound another, and fourteen prefixed
aliases existed purely to keep the shadowed copies reachable. The taxonomy
was three taxonomies held together by an import ordering nobody could see
at a call site — ``except ModelNotFoundException`` written against the
``types`` path never matched what ``Model.find_or_fail`` raised, and the
404 escaped as an unhandled 500. Every duplicate definition has been
deleted and every alias with it (§5: no backward-compat shims — movers
migrate every caller in the same change). ``tests/exceptions/`` pins both
halves: no short name is defined twice under ``types/``, and every shared
name resolves to the same object here and in ``cara.exceptions.types``.
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
    RouteRegistrationException,
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
from .Envelopes import validate_exception_envelopes
from .types.Base import CaraException
from .types.broadcasting import (
    BroadcastingChannelException,
    BroadcastingConfigurationException,
    BroadcastingConnectionException,
    BroadcastingDriverNotFoundException,
    BroadcastingException,
)
from .types.cache import CacheConfigurationException
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
from .types.Eloquent import (
    ConnectionNotRegisteredException,
    DatabaseUnavailableException,
    MigrationException,
    ORMException,
)
from .types.encryption import EncryptionException
from .types.event import (
    EventDispatchCycleException,
    EventNameConflictException,
    ListenerNotFoundException,
)
from .types.http import (
    BadRequestException,
    Http404Exception,
    HttpException,
    InvalidCursor,
    MethodNotAllowedException,
    PayloadTooLargeException,
    ResponseException,
    RouteNotFoundException,
    ServiceUnavailableException,
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
from .types.ModelExceptions import (
    DriverNotFoundException,
    InvalidArgumentException,
    LazyLoadingViolation,
    ModelException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
    QueryException,
)
from .types.queue import (
    IdempotencyOverlapException,
    QueueConfigurationException,
    QueueDriverLibraryNotFoundException,
    QueueException,
)
from .types.rates import RateLimitConfigurationException
from .types.routing import (
    RouteException,
    RouteMiddlewareNotFoundException,
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
    "CaraException",
    "ConfigurationException",
    "ConnectionNotRegisteredException",
    "ContainerException",
    "ControllerMethodNotFoundException",
    "DatabaseUnavailableException",
    "DriverLibraryNotFoundException",
    "DriverNotFoundException",
    "DriverNotRegisteredException",
    "EncryptionException",
    "EventDispatchCycleException",
    "EventNameConflictException",
    "ExceptionProvider",
    "GenericContainerException",
    "Http404Exception",
    "HttpException",
    "IdempotencyOverlapException",
    "InvalidArgumentException",
    "InvalidConfigurationLocationException",
    "InvalidConfigurationSetupException",
    "InvalidCursor",
    "InvalidRuleFormatException",
    "InvalidTokenException",
    "KeyNotFoundException",
    "LazyLoadingViolation",
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
    "MigrationException",
    "MissingContainerBindingException",
    "ModelException",
    "ModelNotFoundException",
    "MultipleRecordsFoundException",
    "ORMException",
    "PayloadTooLargeException",
    "QueryException",
    "QueueConfigurationException",
    "QueueDriverLibraryNotFoundException",
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
    "validate_exception_envelopes",
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
