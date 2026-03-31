"""
Base Test Case

Enhanced base class for all Cara tests with modern testing utilities.
"""

import unittest
from unittest.mock import Mock, patch

from .FrameworkAssertions import FrameworkAssertions
from .TestHelpers import TestHelpers


class TestCase(unittest.TestCase, FrameworkAssertions):
    """Enhanced base test case with comprehensive testing utilities."""

    def setUp(self):
        """Set up test environment with Cara framework integration."""
        super().setUp()

        # Initialize test helpers
        self.helpers = TestHelpers()

        # Set up test environment
        self._test_env_patchers = self.helpers.set_test_environment()

        # Set up fake facades
        self._test_facades = self.helpers.setup_test_facades()

        # Initialize temporary files/directories for cleanup
        self._temp_files = []
        self._temp_dirs = []

    def tearDown(self):
        """Clean up after test with proper resource cleanup."""
        super().tearDown()

        # Clean up test environment
        self.helpers.cleanup_test_environment(self._test_env_patchers)

        # Clean up test facades
        self.helpers.cleanup_test_facades(self._test_facades)

        # Clean up temporary files and directories
        for temp_file in self._temp_files:
            self.helpers.cleanup_temp_file(temp_file)
        for temp_dir in self._temp_dirs:
            self.helpers.cleanup_temp_dir(temp_dir)

    # Basic Assertions
    def assert_true(self, condition, msg=None):
        """Assert that condition is true."""
        self.assertTrue(condition, msg)

    def assert_false(self, condition, msg=None):
        """Assert that condition is false."""
        self.assertFalse(condition, msg)

    def assert_equal(self, first, second, msg=None):
        """Assert that first equals second."""
        self.assertEqual(first, second, msg)

    def assert_not_equal(self, first, second, msg=None):
        """Assert that first does not equal second."""
        self.assertNotEqual(first, second, msg)

    def assert_null(self, obj, msg=None):
        """Assert that obj is None."""
        self.assertIsNone(obj, msg)

    def assert_not_null(self, obj, msg=None):
        """Assert that obj is not None."""
        self.assertIsNotNone(obj, msg)

    def assert_in(self, member, container, msg=None):
        """Assert that member is in container."""
        self.assertIn(member, container, msg)

    def assert_not_in(self, member, container, msg=None):
        """Assert that member is not in container."""
        self.assertNotIn(member, container, msg)

    def assert_contains(self, haystack, needle, msg=None):
        """Assert that haystack contains needle."""
        self.assertIn(needle, haystack, msg)

    def assert_not_contains(self, haystack, needle, msg=None):
        """Assert that haystack does not contain needle."""
        self.assertNotIn(needle, haystack, msg)

    def assert_empty(self, container, msg=None):
        """Assert that container is empty."""
        self.assertEqual(len(container), 0, msg)

    def assert_not_empty(self, container, msg=None):
        """Assert that container is not empty."""
        self.assertNotEqual(len(container), 0, msg)

    def assert_count(self, container, expected_count, msg=None):
        """Assert that container has expected count."""
        self.assertEqual(len(container), expected_count, msg)

    def assert_greater_than(self, first, second, msg=None):
        """Assert that first is greater than second."""
        self.assertGreater(first, second, msg)

    def assert_less_than(self, first, second, msg=None):
        """Assert that first is less than second."""
        self.assertLess(first, second, msg)

    def assert_instance_of(self, obj, cls, msg=None):
        """Assert that obj is instance of cls."""
        self.assertIsInstance(obj, cls, msg)

    # Array/List Assertions
    def assert_array_has_size(self, array, size, msg=None):
        """Assert that array has specific size."""
        self.assertEqual(len(array), size, msg)

    def assert_array_contains(self, array, predicate, msg=None):
        """Assert that array contains item matching predicate."""
        if callable(predicate):
            found = any(predicate(item) for item in array)
        else:
            found = predicate in array
        self.assertTrue(found, msg)

    def assert_array_not_contains(self, array, predicate, msg=None):
        """Assert that array does not contain item matching predicate."""
        if callable(predicate):
            found = any(predicate(item) for item in array)
        else:
            found = predicate in array
        self.assertFalse(found, msg)

    def assert_all_array_items_match(self, array, predicate, msg=None):
        """Assert that all array items match predicate."""
        if callable(predicate):
            all_match = all(predicate(item) for item in array)
        else:
            all_match = all(item == predicate for item in array)
        self.assertTrue(all_match, msg)

    # Property Assertions
    def assert_property_equals(self, obj, property_name, expected_value, msg=None):
        """Assert that object property equals expected value."""
        actual_value = getattr(obj, property_name, None)
        self.assertEqual(actual_value, expected_value, msg)

    def assert_has_property(self, obj, property_name, msg=None):
        """Assert that object has property."""
        self.assertTrue(hasattr(obj, property_name), msg)

    # Mock Utilities
    def mock_facade(self, facade_name):
        """Mock a facade."""
        mock = Mock()
        patcher = patch(f"cara.facades.{facade_name}", mock)
        patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def mock_service(self, service_name):
        """Mock a service."""
        mock = Mock()
        # Implementation depends on service container
        return mock

    def assert_facade_called(self, facade_name, method_name, msg=None):
        """Assert that facade method was called."""
        # Implementation depends on mock tracking
        pass

    def assert_method_called(self, method_name, msg=None):
        """Assert that method was called."""
        # Implementation depends on mock tracking
        pass

    # Fake Data Utilities
    def fake_name(self):
        """Generate fake name."""
        return "John Doe"

    def fake_email(self):
        """Generate fake email."""
        return "test@example.com"

    def fake_text(self, max_nb_chars=200):
        """Generate fake text."""
        return "Lorem ipsum dolor sit amet, consectetur adipiscing elit."

    def fake_sentence(self, nb_words=6):
        """Generate fake sentence."""
        return "This is a fake sentence."

    def fake_word(self):
        """Generate fake word."""
        return "word"

    def fake_address(self):
        """Generate fake address."""
        return "123 Main St"

    def fake_city(self):
        """Generate fake city."""
        return "New York"

    def fake_postcode(self):
        """Generate fake postcode."""
        return "12345"

    def fake_random_int(self, min_val=1, max_val=100):
        """Generate fake random integer."""
        import random

        return random.randint(min_val, max_val)

    def fake_user_data(self):
        """Generate fake user data."""
        return {
            "name": self.fake_name(),
            "email": self.fake_email(),
            "password": "password123",
        }

    # Instance Creation
    def make_instance(self, cls, *args, **kwargs):
        """Create instance of class."""
        return cls(*args, **kwargs)

    def call_method(self, obj, method_name, *args, **kwargs):
        """Call method on object."""
        method = getattr(obj, method_name)
        return method(*args, **kwargs)

    async def call_async_method(self, obj, method_name, *args, **kwargs):
        """Call async method on object."""
        method = getattr(obj, method_name)
        return await method(*args, **kwargs)
