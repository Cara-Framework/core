"""
Test Provider - Enhanced service provider for Cara testing framework

This file provides service provider registration for testing services with
integration for current Cara framework features.
"""

from datetime import datetime
from typing import Dict, List


class TestProvider:
    """Enhanced service provider for Cara testing framework."""

    def __init__(self, app=None):
        """Initialize test provider."""
        self.app = app
        self.bindings = {}
        self.singletons = {}

    def register(self):
        """Register testing services with enhanced features."""
        self.register_test_services()
        self.register_fake_services()
        self.register_test_utilities()
        self.register_framework_integrations()

    def register_test_services(self):
        """Register core test services."""
        # Test case factory
        self.bind("test.case_factory", self.create_test_case_factory)

        # Test database manager
        self.bind("test.database_manager", self.create_database_manager)

        # Test HTTP client
        self.bind("test.http_client", self.create_http_client)

        # Test assertion manager
        self.bind("test.assertion_manager", self.create_assertion_manager)

        # Test helpers
        self.bind("test.helpers", self.create_test_helpers)

    def register_fake_services(self):
        """Register fake services for testing."""
        from .Fake import (
            FakeCache,
            FakeEvent,
            FakeMailer,
            FakeNotification,
            FakeQueue,
            FakeStorage,
        )

        # Fake services with enhanced features
        self.singleton("fake.mailer", lambda: FakeMailer())
        self.singleton("fake.queue", lambda: FakeQueue())
        self.singleton("fake.notification", lambda: FakeNotification())
        self.singleton("fake.storage", lambda: FakeStorage())
        self.singleton("fake.cache", lambda: FakeCache())
        self.singleton("fake.event", lambda: FakeEvent())

        # Integration with Cara facades
        self.register_facade_fakes()

    def register_facade_fakes(self):
        """Register fake implementations for Cara facades."""
        facade_mappings = {
            "Mail": "fake.mailer",
            "Queue": "fake.queue",
            "Notification": "fake.notification",
            "Storage": "fake.storage",
            "Cache": "fake.cache",
            "Event": "fake.event",
        }

        for facade_name, service_key in facade_mappings.items():
            self.bind(
                f"test.facade.{facade_name.lower()}", lambda: self.make(service_key)
            )

    def register_test_utilities(self):
        """Register enhanced test utilities."""
        # Test data factory
        self.bind("test.factory", self.create_test_factory)

        # Test seeder
        self.bind("test.seeder", self.create_test_seeder)

        # Test mock manager
        self.bind("test.mock_manager", self.create_mock_manager)

        # Test fixtures manager
        self.bind("test.fixtures", self.create_fixtures_manager)

    def register_framework_integrations(self):
        """Register Cara framework-specific integrations."""
        # Tinker integration for testing
        self.bind("test.tinker", self.create_tinker_tester)

        # Command testing
        self.bind("test.command", self.create_command_tester)

        # Middleware testing
        self.bind("test.middleware", self.create_middleware_tester)

        # Route testing
        self.bind("test.route", self.create_route_tester)

    def create_test_case_factory(self):
        """Create enhanced test case factory."""
        from .TestCase import TestCase

        class TestCaseFactory:
            """Factory for creating test cases with enhanced features."""

            def create(self, test_type: str = "unit", **options):
                """Create test case instance with options."""
                if test_type == "feature":
                    from .FeatureTestCase import FeatureTestCase

                    return FeatureTestCase()
                elif test_type == "unit":
                    from .UnitTestCase import UnitTestCase

                    return UnitTestCase()
                elif test_type == "database":
                    from .DatabaseTestCase import DatabaseTestCase

                    return DatabaseTestCase()
                elif test_type == "http":
                    from .HttpTestCase import HttpTestCase

                    return HttpTestCase()
                else:
                    return TestCase()

            def create_with_concerns(self, test_type: str, concerns: List[str]):
                """Create test case with specific concerns/mixins."""
                test_case = self.create(test_type)

                # Apply concerns
                for concern in concerns:
                    if concern == "database_transactions":
                        from .Concerns import DatabaseTransactions

                        test_case.__class__ = type(
                            test_case.__class__.__name__,
                            (test_case.__class__, DatabaseTransactions),
                            {},
                        )
                    elif concern == "with_faker":
                        from .Concerns import WithFaker

                        test_case.__class__ = type(
                            test_case.__class__.__name__,
                            (test_case.__class__, WithFaker),
                            {},
                        )

                return test_case

        return TestCaseFactory()

    def create_database_manager(self):
        """Create enhanced test database manager."""
        from .DatabaseTransactions import DatabaseTransactions

        class TestDatabaseManager:
            """Enhanced manager for test database operations."""

            def __init__(self):
                self.transactions = DatabaseTransactions()
                self.seeders = {}
                self.factories = {}

            async def setup_test_database(self):
                """Set up test database with migrations."""
                await self.transactions.setup_database_transactions()
                await self.run_test_migrations()

            async def cleanup_test_database(self):
                """Clean up test database."""
                await self.transactions.teardown_database_transactions()

            async def refresh_database(self):
                """Refresh test database with fresh state."""
                await self.cleanup_test_database()
                await self.setup_test_database()

            async def run_test_migrations(self):
                """Run test-specific migrations."""
                try:
                    # Integration with Cara migration system
                    from cara.facades import DB

                    # Run migrations in test mode
                    print("[TEST] Running test migrations (mocked)")
                except ImportError:
                    print("[TEST] Migration system not available - using mocked setup")

            def register_seeder(self, name: str, seeder_class):
                """Register database seeder for testing."""
                self.seeders[name] = seeder_class

            def register_factory(self, model_name: str, factory_class):
                """Register model factory for testing."""
                self.factories[model_name] = factory_class

            async def seed_with(self, seeder_names: List[str]):
                """Seed database with specific seeders."""
                for seeder_name in seeder_names:
                    if seeder_name in self.seeders:
                        seeder = self.seeders[seeder_name]()
                        await seeder.run()

        return TestDatabaseManager()

    def create_http_client(self):
        """Create enhanced test HTTP client."""

        class TestHttpClient:
            """Enhanced HTTP client for testing with Cara integration."""

            def __init__(self):
                self.base_url = "http://localhost"
                self.headers = {}
                self.cookies = {}
                self.middleware_stack = []

            async def get(self, url: str, headers: Dict = None):
                """Make GET request with middleware simulation."""
                return await self.make_request("GET", url, headers=headers)

            async def post(self, url: str, data: Dict = None, headers: Dict = None):
                """Make POST request with validation testing."""
                return await self.make_request("POST", url, data=data, headers=headers)

            async def put(self, url: str, data: Dict = None, headers: Dict = None):
                """Make PUT request."""
                return await self.make_request("PUT", url, data=data, headers=headers)

            async def delete(self, url: str, headers: Dict = None):
                """Make DELETE request."""
                return await self.make_request("DELETE", url, headers=headers)

            async def make_request(
                self, method: str, url: str, data: Dict = None, headers: Dict = None
            ):
                """Make HTTP request with enhanced testing features."""
                from .TestResponse import TestResponse

                # Simulate middleware execution
                self.simulate_middleware_stack(method, url)

                # Create enhanced mock response
                response_data = {
                    "status_code": self.determine_status_code(method, url),
                    "headers": self.build_response_headers(headers),
                    "content": self.build_response_content(method, url, data),
                    "json": self.build_json_response(method, url, data),
                }

                return TestResponse(response_data)

            def simulate_middleware_stack(self, method: str, url: str):
                """Simulate middleware execution for testing."""
                # Mock middleware execution
                middleware_list = ["cors", "auth", "throttle"]
                for middleware in middleware_list:
                    if middleware not in self.middleware_stack:
                        self.middleware_stack.append(middleware)

            def determine_status_code(self, method: str, url: str) -> int:
                """Determine appropriate status code for test."""
                if "unauthorized" in url:
                    return 401
                elif "forbidden" in url:
                    return 403
                elif "not-found" in url:
                    return 404
                elif method == "POST" and "create" in url:
                    return 201
                else:
                    return 200

            def build_response_headers(self, request_headers: Dict = None) -> Dict:
                """Build response headers with CORS and security headers."""
                return {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "X-Content-Type-Options": "nosniff",
                    "X-Frame-Options": "DENY",
                    "X-XSS-Protection": "1; mode=block",
                }

            def build_response_content(
                self, method: str, url: str, data: Dict = None
            ) -> str:
                """Build response content based on request."""
                import json

                response_obj = {
                    "message": f"Test response for {method} {url}",
                    "method": method,
                    "url": url,
                    "timestamp": datetime.now().isoformat(),
                }
                if data:
                    response_obj["received_data"] = data
                return json.dumps(response_obj)

            def build_json_response(
                self, method: str, url: str, data: Dict = None
            ) -> Dict:
                """Build JSON response object."""
                response_obj = {
                    "message": f"Test response for {method} {url}",
                    "method": method,
                    "url": url,
                    "timestamp": datetime.now().isoformat(),
                }
                if data:
                    response_obj["received_data"] = data
                return response_obj

        return TestHttpClient()

    def create_assertion_manager(self):
        """Create enhanced assertion manager."""
        from .Assertions import Assertions
        from .FrameworkAssertions import FrameworkAssertions

        class AssertionManager:
            """Enhanced manager for test assertions."""

            def __init__(self):
                self.assertions = Assertions()
                self.framework_assertions = FrameworkAssertions()

            def get_database_assertions(self):
                """Get database assertions."""
                return self.assertions

            def get_http_assertions(self):
                """Get HTTP assertions."""
                return self.assertions

            def get_auth_assertions(self):
                """Get authentication assertions."""
                return self.assertions

            def get_framework_assertions(self):
                """Get Cara framework-specific assertions."""
                return self.framework_assertions

            def get_facade_assertions(self):
                """Get facade-specific assertions."""
                return self.framework_assertions

        return AssertionManager()

    def create_test_helpers(self):
        """Create test helpers instance."""
        from .TestHelpers import TestHelpers

        return TestHelpers()

    def create_test_factory(self):
        """Create enhanced test data factory."""

        class TestFactory:
            """Enhanced factory for creating test data."""

            def __init__(self):
                self.definitions = {}
                self.sequences = {}

            def define(self, model_name: str, factory_func):
                """Define factory for model."""
                self.definitions[model_name] = factory_func

            def define_sequence(self, name: str, sequence_func):
                """Define sequence for generating unique values."""
                self.sequences[name] = sequence_func

            def create(self, model_name: str, attributes: Dict = None, count: int = 1):
                """Create model instance(s)."""
                if model_name not in self.definitions:
                    raise ValueError(f"No factory defined for {model_name}")

                if count == 1:
                    return self._create_single(model_name, attributes)
                else:
                    return [
                        self._create_single(model_name, attributes) for _ in range(count)
                    ]

            def _create_single(self, model_name: str, attributes: Dict = None):
                """Create single model instance."""
                factory_func = self.definitions[model_name]
                instance = factory_func()

                # Apply custom attributes
                if attributes:
                    for key, value in attributes.items():
                        setattr(instance, key, value)

                return instance

            def make(self, model_name: str, count: int = 1, attributes: Dict = None):
                """Make instances without persisting."""
                return self.create(model_name, attributes, count)

            def create_for_user(self, user, model_name: str, attributes: Dict = None):
                """Create model instance associated with user."""
                attributes = attributes or {}
                attributes["user_id"] = getattr(user, "id", user)
                return self.create(model_name, attributes)

        return TestFactory()

    def create_test_seeder(self):
        """Create enhanced test seeder."""

        class TestSeeder:
            """Enhanced seeder for test data."""

            def __init__(self):
                self.seeders = {}
                self.dependencies = {}

            def register(self, name: str, seeder_func, dependencies: List[str] = None):
                """Register seeder with dependencies."""
                self.seeders[name] = seeder_func
                self.dependencies[name] = dependencies or []

            async def seed(self, name: str):
                """Run seeder with dependency resolution."""
                if name not in self.seeders:
                    raise ValueError(f"No seeder registered for {name}")

                # Run dependencies first
                for dependency in self.dependencies[name]:
                    await self.seed(dependency)

                # Run the seeder
                seeder_func = self.seeders[name]
                await seeder_func()

            async def seed_all(self):
                """Run all seeders in dependency order."""
                # Topological sort for dependency resolution
                visited = set()
                for name in self.seeders:
                    if name not in visited:
                        await self._seed_recursive(name, visited)

            async def _seed_recursive(self, name: str, visited: set):
                """Recursively seed with dependencies."""
                if name in visited:
                    return

                for dependency in self.dependencies[name]:
                    await self._seed_recursive(dependency, visited)

                visited.add(name)
                seeder_func = self.seeders[name]
                await seeder_func()

        return TestSeeder()

    def create_mock_manager(self):
        """Create enhanced mock manager."""
        from unittest.mock import Mock, patch

        class MockManager:
            """Enhanced manager for test mocks with Cara integration."""

            def __init__(self):
                self.mocks = {}
                self.patches = {}

            def mock(self, name: str, mock_obj=None):
                """Create or get mock."""
                if name not in self.mocks:
                    self.mocks[name] = mock_obj or Mock()
                return self.mocks[name]

            def mock_facade(self, facade_name: str):
                """Mock Cara facade."""
                mock_obj = Mock()
                patcher = patch(f"cara.facades.{facade_name}", mock_obj)
                patcher.start()
                self.patches[facade_name] = patcher
                self.mocks[facade_name] = mock_obj
                return mock_obj

            def mock_config(self, key: str, value):
                """Mock configuration value."""
                patcher = patch("cara.facades.Config.get")
                mock_config = patcher.start()
                mock_config.return_value = value
                self.patches[f"config_{key}"] = patcher
                return mock_config

            def clear_mocks(self):
                """Clear all mocks."""
                self.mocks.clear()

            def reset_mocks(self):
                """Reset all mocks."""
                for mock_obj in self.mocks.values():
                    if hasattr(mock_obj, "reset_mock"):
                        mock_obj.reset_mock()

            def stop_patches(self):
                """Stop all patches."""
                for patcher in self.patches.values():
                    patcher.stop()
                self.patches.clear()

        return MockManager()

    def create_fixtures_manager(self):
        """Create fixtures manager for test data."""
        import json
        import os

        class FixturesManager:
            """Manager for test fixtures and data."""

            def __init__(self):
                self.fixtures_dir = "tests/fixtures"
                self.loaded_fixtures = {}

            def load_fixture(self, filename: str):
                """Load fixture from file."""
                if filename in self.loaded_fixtures:
                    return self.loaded_fixtures[filename]

                filepath = os.path.join(self.fixtures_dir, filename)
                if os.path.exists(filepath):
                    with open(filepath, "r") as f:
                        if filename.endswith(".json"):
                            data = json.load(f)
                        else:
                            data = f.read()
                    self.loaded_fixtures[filename] = data
                    return data
                return None

            def save_fixture(self, filename: str, data):
                """Save data as fixture."""
                os.makedirs(self.fixtures_dir, exist_ok=True)
                filepath = os.path.join(self.fixtures_dir, filename)

                with open(filepath, "w") as f:
                    if filename.endswith(".json"):
                        json.dump(data, f, indent=2, default=str)
                    else:
                        f.write(str(data))

            def get_user_fixture(self, user_type: str = "default"):
                """Get user fixture by type."""
                return self.load_fixture(f"users/{user_type}.json")

            def get_api_response_fixture(self, endpoint: str):
                """Get API response fixture."""
                return self.load_fixture(f"api_responses/{endpoint}.json")

        return FixturesManager()

    def create_tinker_tester(self):
        """Create tinker command tester."""

        class TinkerTester:
            """Tester for Cara tinker commands."""

            def __init__(self):
                self.command_history = []

            async def execute_command(self, command: str):
                """Execute tinker command for testing."""
                self.command_history.append(command)
                # Mock tinker command execution
                return f"Tinker command executed: {command}"

            def assert_command_executed(self, command: str):
                """Assert that command was executed."""
                assert command in self.command_history, (
                    f"Command '{command}' was not executed"
                )

            def get_command_history(self):
                """Get history of executed commands."""
                return self.command_history.copy()

        return TinkerTester()

    def create_command_tester(self):
        """Create craft command tester."""

        class CommandTester:
            """Tester for Cara craft commands."""

            def __init__(self):
                self.executed_commands = []

            async def execute_craft_command(self, command: str, args: List[str] = None):
                """Execute craft command for testing."""
                full_command = f"craft {command}"
                if args:
                    full_command += " " + " ".join(args)

                self.executed_commands.append(full_command)
                # Mock command execution
                return {"success": True, "output": f"Command executed: {full_command}"}

            def assert_command_success(self, command: str):
                """Assert that command executed successfully."""
                matching_commands = [
                    cmd for cmd in self.executed_commands if command in cmd
                ]
                assert len(matching_commands) > 0, (
                    f"Command '{command}' was not executed successfully"
                )

        return CommandTester()

    def create_middleware_tester(self):
        """Create middleware tester."""

        class MiddlewareTester:
            """Tester for Cara middleware."""

            def __init__(self):
                self.executed_middleware = []

            def simulate_middleware_execution(self, middleware_stack: List[str]):
                """Simulate middleware execution."""
                self.executed_middleware.extend(middleware_stack)

            def assert_middleware_executed(self, middleware_name: str):
                """Assert that middleware was executed."""
                assert middleware_name in self.executed_middleware, (
                    f"Middleware '{middleware_name}' was not executed"
                )

            def assert_middleware_order(self, expected_order: List[str]):
                """Assert middleware execution order."""
                actual_order = [
                    mw for mw in self.executed_middleware if mw in expected_order
                ]
                assert actual_order == expected_order, (
                    f"Expected middleware order {expected_order}, got {actual_order}"
                )

        return MiddlewareTester()

    def create_route_tester(self):
        """Create route tester."""

        class RouteTester:
            """Tester for Cara routes."""

            def __init__(self):
                self.registered_routes = {}

            def register_test_route(
                self,
                name: str,
                path: str,
                methods: List[str],
                middleware: List[str] = None,
            ):
                """Register route for testing."""
                self.registered_routes[name] = {
                    "path": path,
                    "methods": methods,
                    "middleware": middleware or [],
                }

            def assert_route_exists(self, route_name: str):
                """Assert that route exists."""
                assert route_name in self.registered_routes, (
                    f"Route '{route_name}' does not exist"
                )

            def assert_route_has_middleware(self, route_name: str, middleware: str):
                """Assert that route has specific middleware."""
                assert route_name in self.registered_routes, (
                    f"Route '{route_name}' does not exist"
                )
                route_middleware = self.registered_routes[route_name]["middleware"]
                assert middleware in route_middleware, (
                    f"Route '{route_name}' does not have middleware '{middleware}'"
                )

        return RouteTester()

    def bind(self, name: str, factory):
        """Bind service to container."""
        self.bindings[name] = factory

    def singleton(self, name: str, factory):
        """Bind singleton service to container."""
        self.singletons[name] = factory

    def make(self, name: str):
        """Make service instance."""
        if name in self.singletons:
            if not hasattr(self, f"_singleton_{name}"):
                setattr(self, f"_singleton_{name}", self.singletons[name]())
            return getattr(self, f"_singleton_{name}")

        if name in self.bindings:
            return self.bindings[name]()

        raise ValueError(f"Service {name} not found")

    def boot(self):
        """Boot testing services with enhanced features."""
        # Initialize test environment
        self.setup_test_environment()

        # Set up test database
        self.setup_test_database()

        # Register default factories and seeders
        self.register_default_test_data()

    def setup_test_environment(self):
        """Set up enhanced test environment."""
        import os

        # Set testing environment variables
        test_env = {
            "APP_ENV": "testing",
            "TESTING": "true",
            "MAIL_DRIVER": "array",
            "QUEUE_DRIVER": "sync",
            "CACHE_DRIVER": "array",
            "SESSION_DRIVER": "array",
            "DB_CONNECTION": "sqlite",
            "DB_DATABASE": ":memory:",
            "LOG_CHANNEL": "single",
            "LOG_LEVEL": "debug",
        }

        for key, value in test_env.items():
            os.environ[key] = value

    def setup_test_database(self):
        """Set up test database configuration."""
        # This would integrate with actual database setup
        print("[TEST] Test database setup initialized")

    def register_default_test_data(self):
        """Register default factories and seeders for testing."""
        # Register common test factories
        factory = self.make("test.factory")

        # User factory
        factory.define(
            "User",
            lambda: type(
                "MockUser",
                (),
                {
                    "id": 1,
                    "name": "Test User",
                    "email": "test@example.com",
                    "password": "hashed_password",
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                },
            )(),
        )

        # Notification factory
        factory.define(
            "Notification",
            lambda: type(
                "MockNotification",
                (),
                {
                    "id": "test-notification-id",
                    "type": "TestNotification",
                    "notifiable_type": "User",
                    "notifiable_id": "1",
                    "data": {"message": "Test notification"},
                    "read_at": None,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                },
            )(),
        )

        # Job factory
        factory.define(
            "Job",
            lambda: type(
                "MockJob",
                (),
                {
                    "id": "test-job-id",
                    "queue": "default",
                    "payload": {"job": "TestJob", "data": {"test": True}},
                    "attempts": 0,
                    "reserved_at": None,
                    "available_at": datetime.now(),
                    "created_at": datetime.now(),
                },
            )(),
        )
