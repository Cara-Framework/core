"""Exception types package — explicit re-exports.

Every short name here resolves to the SAME class as the identically named
attribute on ``cara.exceptions``. That used to be false for nine names:
this barrel re-exported one copy while the parent barrel re-exported
another, so ``from cara.exceptions.types import ModelNotFoundException``
bound a class nothing ever raised and the ``except`` clause written
against it silently never fired. Duplicate definitions are gone (one
short name, one home) and ``tests/exceptions/test_exception_registry.py``
pins the identity, so a re-introduced copy fails a guard instead of
producing a handler that catches nothing.
"""

from .application import (
    AppException,
    ControllerMethodNotFoundException,
    RouteRegistrationException,
)
from .authentication import (
    ApiKeyInvalidException,
    AuthenticationConfigurationException,
    AuthenticationException,
    InvalidTokenException,
    TokenBlacklistedException,
    TokenExpiredException,
    TokenInvalidException,
    UserNotFoundException,
)
from .authorization import AuthorizationException, AuthorizationFailedException
from .Base import CaraException
from .broadcasting import (
    BroadcastingChannelException,
    BroadcastingConfigurationException,
    BroadcastingConnectionException,
    BroadcastingDriverNotFoundException,
    BroadcastingException,
)
from .cache import CacheConfigurationException
from .configuration import (
    ConfigurationException,
    InvalidConfigurationLocationException,
    InvalidConfigurationSetupException,
)
from .container import (
    ContainerException,
    GenericContainerException,
    MissingContainerBindingException,
    StrictContainerException,
)
from .Eloquent import (
    ConnectionNotRegisteredException,
    DatabaseUnavailableException,
    MigrationException,
    ORMException,
)
from .encryption import EncryptionException
from .event import (
    EventDispatchCycleException,
    EventNameConflictException,
    ListenerNotFoundException,
)
from .http import (
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
from .loader import LoaderException, LoaderNotFoundException
from .mail import (
    MailConfigurationException,
    MailDriverException,
    MailException,
    MailSendException,
)
from .middleware import MiddlewareException, MiddlewareNotFoundException
from .ModelExceptions import (
    DriverNotFoundException,
    InvalidArgumentException,
    LazyLoadingViolation,
    ModelException,
    ModelNotFoundException,
    MultipleRecordsFoundException,
    QueryException,
)
from .queue import (
    IdempotencyOverlapException,
    QueueConfigurationException,
    QueueDriverLibraryNotFoundException,
    QueueException,
)
from .rates import RateLimitConfigurationException
from .routing import RouteException, RouteMiddlewareNotFoundException
from .scheduling import (
    DriverLibraryNotFoundException,
    SchedulingConfigurationException,
    SchedulingException,
)
from .storage import (
    DriverNotRegisteredException,
    KeyNotFoundException,
    StorageConfigurationException,
    StorageException,
)
from .validation import (
    InvalidRuleFormatException,
    RuleNotFoundException,
    ValidationException,
)
from .websocket import WebSocketException

__all__ = [
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
]
