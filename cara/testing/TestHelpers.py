"""
Test Helpers - Modern testing utilities for Cara framework

This file provides helper methods and utilities for testing Cara applications.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union
from unittest.mock import Mock, patch


class TestHelpers:
    """Modern testing helpers for Cara framework."""

    # File and Path Helpers
    @staticmethod
    def create_temp_file(content: str = "", suffix: str = ".txt") -> str:
        """Create temporary file with content."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            f.write(content)
            return f.name

    @staticmethod
    def create_temp_dir() -> str:
        """Create temporary directory."""
        return tempfile.mkdtemp()

    @staticmethod
    def cleanup_temp_file(filepath: str) -> None:
        """Clean up temporary file."""
        try:
            os.unlink(filepath)
        except (OSError, FileNotFoundError):
            pass

    @staticmethod
    def cleanup_temp_dir(dirpath: str) -> None:
        """Clean up temporary directory."""
        import shutil

        try:
            shutil.rmtree(dirpath)
        except (OSError, FileNotFoundError):
            pass

    # JSON Helpers
    @staticmethod
    def load_json_fixture(filename: str) -> Dict[str, Any]:
        """Load JSON fixture from tests/fixtures directory."""
        fixture_path = os.path.join("tests", "fixtures", filename)
        if os.path.exists(fixture_path):
            with open(fixture_path, "r") as f:
                return json.load(f)
        return {}

    @staticmethod
    def save_json_fixture(filename: str, data: Dict[str, Any]) -> None:
        """Save data as JSON fixture."""
        fixture_dir = os.path.join("tests", "fixtures")
        os.makedirs(fixture_dir, exist_ok=True)
        fixture_path = os.path.join(fixture_dir, filename)
        with open(fixture_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    # Mock Helpers
    @staticmethod
    def mock_cara_facade(facade_name: str) -> Mock:
        """Mock a Cara facade."""
        mock = Mock()
        patcher = patch(f"cara.facades.{facade_name}", mock)
        patcher.start()
        return mock, patcher

    @staticmethod
    def mock_cara_config(config_key: str, value: Any) -> Mock:
        """Mock Cara configuration value."""
        patcher = patch("cara.facades.Config.get")
        mock_config = patcher.start()
        mock_config.return_value = value
        return mock_config, patcher

    @staticmethod
    def mock_environment_variable(var_name: str, value: str) -> Mock:
        """Mock environment variable."""
        patcher = patch.dict(os.environ, {var_name: value})
        patcher.start()
        return patcher

    # Time Helpers
    @staticmethod
    def freeze_time(frozen_time: Union[str, datetime]) -> Mock:
        """Freeze time for testing."""
        if isinstance(frozen_time, str):
            frozen_time = datetime.fromisoformat(frozen_time)

        patcher = patch("datetime.datetime")
        mock_datetime = patcher.start()
        mock_datetime.now.return_value = frozen_time
        mock_datetime.utcnow.return_value = frozen_time
        return mock_datetime, patcher

    @staticmethod
    def travel_to_future(days: int = 1, hours: int = 0, minutes: int = 0) -> datetime:
        """Get future datetime for testing."""
        return datetime.now() + timedelta(days=days, hours=hours, minutes=minutes)

    @staticmethod
    def travel_to_past(days: int = 1, hours: int = 0, minutes: int = 0) -> datetime:
        """Get past datetime for testing."""
        return datetime.now() - timedelta(days=days, hours=hours, minutes=minutes)

    # Data Generation Helpers
    @staticmethod
    def generate_test_user_data(override: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate test user data."""
        data = {
            "name": "Test User",
            "email": "test@example.com",
            "password": "password123",
            "email_verified_at": datetime.now(),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        if override:
            data.update(override)
        return data

    @staticmethod
    def generate_test_notification_data(
        override: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Generate test notification data."""
        data = {
            "id": "test-notification-id",
            "type": "TestNotification",
            "notifiable_type": "User",
            "notifiable_id": "1",
            "data": {"message": "Test notification"},
            "read_at": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        if override:
            data.update(override)
        return data

    @staticmethod
    def generate_test_job_data(override: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate test job data."""
        data = {
            "id": "test-job-id",
            "queue": "default",
            "payload": {"job": "TestJob", "data": {"test": True}},
            "attempts": 0,
            "reserved_at": None,
            "available_at": datetime.now(),
            "created_at": datetime.now(),
        }
        if override:
            data.update(override)
        return data

    # HTTP Testing Helpers
    @staticmethod
    def create_test_request_data(
        method: str = "GET",
        path: str = "/",
        headers: Dict[str, str] = None,
        data: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Create test request data."""
        return {
            "method": method.upper(),
            "path": path,
            "headers": headers or {},
            "data": data or {},
            "query_string": "",
            "content_type": "application/json",
        }

    @staticmethod
    def create_test_response_data(
        status_code: int = 200,
        content: Union[str, Dict] = None,
        headers: Dict[str, str] = None,
    ) -> Dict[str, Any]:
        """Create test response data."""
        if content is None:
            content = {"message": "Test response"}

        if isinstance(content, dict):
            content_str = json.dumps(content)
            json_data = content
        else:
            content_str = str(content)
            try:
                json_data = json.loads(content_str)
            except json.JSONDecodeError:
                json_data = {}

        return {
            "status_code": status_code,
            "content": content_str,
            "json": json_data,
            "headers": headers or {"Content-Type": "application/json"},
            "text": content_str,
        }

    # Database Testing Helpers
    @staticmethod
    def create_test_database_record(table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create test database record with timestamps."""
        record = data.copy()
        if "created_at" not in record:
            record["created_at"] = datetime.now()
        if "updated_at" not in record:
            record["updated_at"] = datetime.now()
        return record

    @staticmethod
    def assert_database_record_exists(
        records: List[Dict], search_criteria: Dict[str, Any]
    ) -> bool:
        """Assert that database record exists in test data."""
        for record in records:
            if all(record.get(key) == value for key, value in search_criteria.items()):
                return True
        return False

    # Assertion Helpers
    @staticmethod
    def assert_arrays_equal_unordered(
        array1: List[Any], array2: List[Any], msg: str = None
    ) -> bool:
        """Assert that two arrays contain the same elements regardless of order."""
        return sorted(array1) == sorted(array2)

    @staticmethod
    def assert_dict_contains_subset(
        subset: Dict[str, Any], full_dict: Dict[str, Any], msg: str = None
    ) -> bool:
        """Assert that dictionary contains all key-value pairs from subset."""
        for key, value in subset.items():
            if key not in full_dict or full_dict[key] != value:
                return False
        return True

    @staticmethod
    def assert_string_contains_all(
        string: str, substrings: List[str], msg: str = None
    ) -> bool:
        """Assert that string contains all specified substrings."""
        return all(substring in string for substring in substrings)

    # Performance Testing Helpers
    @staticmethod
    def measure_execution_time(func, *args, **kwargs) -> tuple:
        """Measure function execution time."""
        import time

        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    @staticmethod
    async def measure_async_execution_time(func, *args, **kwargs) -> tuple:
        """Measure async function execution time."""
        import time

        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    # Cara Framework Specific Helpers
    @staticmethod
    def create_test_cara_application():
        """Create test Cara application instance."""
        try:
            from cara.foundation import Application

            app = Application()
            app.bind("testing", True)
            return app
        except ImportError:
            return Mock()

    @staticmethod
    def setup_test_facades():
        """Set up test facades with mocks."""
        facades = {}
        facade_names = [
            "Auth",
            "Cache",
            "Config",
            "DB",
            "Event",
            "Hash",
            "Log",
            "Mail",
            "Notification",
            "Queue",
            "Session",
            "Storage",
            "View",
        ]

        for facade_name in facade_names:
            mock, patcher = TestHelpers.mock_cara_facade(facade_name)
            facades[facade_name] = {"mock": mock, "patcher": patcher}

        return facades

    @staticmethod
    def cleanup_test_facades(facades: Dict[str, Dict]):
        """Clean up test facades."""
        for facade_data in facades.values():
            if "patcher" in facade_data:
                facade_data["patcher"].stop()

    # Test Environment Helpers
    @staticmethod
    def set_test_environment():
        """Set up test environment variables."""
        test_env = {
            "APP_ENV": "testing",
            "TESTING": "true",
            "MAIL_DRIVER": "array",
            "QUEUE_DRIVER": "sync",
            "CACHE_DRIVER": "array",
            "SESSION_DRIVER": "array",
            "DB_CONNECTION": "sqlite",
            "DB_DATABASE": ":memory:",
        }

        patchers = []
        for key, value in test_env.items():
            patcher = TestHelpers.mock_environment_variable(key, value)
            patchers.append(patcher)

        return patchers

    @staticmethod
    def cleanup_test_environment(patchers: List):
        """Clean up test environment."""
        for patcher in patchers:
            patcher.stop()

    # Utility Methods
    @staticmethod
    def deep_merge_dicts(dict1: Dict, dict2: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = dict1.copy()
        for key, value in dict2.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = TestHelpers.deep_merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    @staticmethod
    def flatten_dict(d: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        """Flatten nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(TestHelpers.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def get_class_methods(cls) -> List[str]:
        """Get all methods of a class."""
        return [
            method
            for method in dir(cls)
            if callable(getattr(cls, method)) and not method.startswith("_")
        ]

    @staticmethod
    def is_json_serializable(obj: Any) -> bool:
        """Check if object is JSON serializable."""
        try:
            json.dumps(obj, default=str)
            return True
        except (TypeError, ValueError):
            return False
