"""
Cara Testing Framework

Laravel-style testing framework for Cara applications with comprehensive testing utilities.
"""

from .Assertions import Assertions
from .Concerns import (
    DatabaseTransactions,
    RefreshDatabase,
    WithFaker,
    WithoutEvents,
    WithoutMiddleware,
    WithoutNotifications,
)
from .DatabaseTestCase import DatabaseTestCase
from .DatabaseTransactions import DatabaseTransactions as DatabaseTransactionManager
from .Fake import (
    FakeCache,
    FakeEvent,
    FakeMailer,
    FakeNotification,
    FakeQueue,
    FakeService,
    FakeStorage,
)
from .FeatureTestCase import FeatureTestCase
from .HttpTestCase import HttpTestCase
from .TestCase import TestCase
from .TestProvider import TestProvider
from .TestResponse import TestResponse
from .UnitTestCase import UnitTestCase

# Testing utilities
from .TestHelpers import TestHelpers
from .FrameworkAssertions import FrameworkAssertions

__all__ = [
    # Core test cases
    "TestCase",
    "FeatureTestCase",
    "UnitTestCase",
    "DatabaseTestCase",
    "HttpTestCase",
    
    # Test utilities
    "TestResponse",
    "TestProvider",
    "TestHelpers",
    
    # Assertions
    "Assertions",
    "FrameworkAssertions",
    
    # Test concerns/mixins
    "DatabaseTransactions",
    "RefreshDatabase", 
    "WithFaker",
    "WithoutEvents",
    "WithoutMiddleware",
    "WithoutNotifications",
    
    # Database testing
    "DatabaseTransactionManager",
    
    # Fake services
    "FakeService",
    "FakeMailer",
    "FakeQueue",
    "FakeNotification",
    "FakeEvent",
    "FakeStorage",
    "FakeCache",
]

# Version info
__version__ = "1.0.0"
__author__ = "Cara Framework Team"
__description__ = "Laravel-style testing framework for Cara applications" 