"""
Test Response

Wrapper for HTTP responses with testing assertions.
"""

import json


class TestResponse:
    """Test response wrapper with assertion methods."""

    def __init__(self, response_data):
        """Initialize test response."""
        self.status_code = response_data.get("status_code", 200)
        self.headers = response_data.get("headers", {})
        self.content = response_data.get("content", "")
        self.text = response_data.get("text", self.content)

        # Parse JSON if content is JSON string
        try:
            if isinstance(self.content, str):
                self.json = json.loads(self.content)
            else:
                self.json = response_data.get("json", {})
        except (json.JSONDecodeError, TypeError):
            self.json = {}

    # Status Code Assertions
    def assert_ok(self, msg=None):
        """Assert 200 OK status."""
        assert self.status_code == 200, msg or f"Expected 200, got {self.status_code}"

    def assert_created(self, msg=None):
        """Assert 201 Created status."""
        assert self.status_code == 201, msg or f"Expected 201, got {self.status_code}"

    def assert_accepted(self, msg=None):
        """Assert 202 Accepted status."""
        assert self.status_code == 202, msg or f"Expected 202, got {self.status_code}"

    def assert_no_content(self, msg=None):
        """Assert 204 No Content status."""
        assert self.status_code == 204, msg or f"Expected 204, got {self.status_code}"

    def assert_bad_request(self, msg=None):
        """Assert 400 Bad Request status."""
        assert self.status_code == 400, msg or f"Expected 400, got {self.status_code}"

    def assert_unauthorized(self, msg=None):
        """Assert 401 Unauthorized status."""
        assert self.status_code == 401, msg or f"Expected 401, got {self.status_code}"

    def assert_forbidden(self, msg=None):
        """Assert 403 Forbidden status."""
        assert self.status_code == 403, msg or f"Expected 403, got {self.status_code}"

    def assert_not_found(self, msg=None):
        """Assert 404 Not Found status."""
        assert self.status_code == 404, msg or f"Expected 404, got {self.status_code}"

    def assert_method_not_allowed(self, msg=None):
        """Assert 405 Method Not Allowed status."""
        assert self.status_code == 405, msg or f"Expected 405, got {self.status_code}"

    def assert_unprocessable(self, msg=None):
        """Assert 422 Unprocessable Entity status."""
        assert self.status_code == 422, msg or f"Expected 422, got {self.status_code}"

    def assert_too_many_requests(self, msg=None):
        """Assert 429 Too Many Requests status."""
        assert self.status_code == 429, msg or f"Expected 429, got {self.status_code}"

    def assert_server_error(self, msg=None):
        """Assert 500 Internal Server Error status."""
        assert self.status_code == 500, msg or f"Expected 500, got {self.status_code}"

    def assert_status(self, expected_status, msg=None):
        """Assert specific status code."""
        assert self.status_code == expected_status, (
            msg or f"Expected {expected_status}, got {self.status_code}"
        )

    # JSON Assertions
    def assert_json(self, expected_json, msg=None):
        """Assert JSON response matches expected."""
        assert self.json == expected_json, (
            msg or f"JSON mismatch: expected {expected_json}, got {self.json}"
        )

    def assert_json_structure(self, expected_structure, path=None, msg=None):
        """Assert JSON has expected structure."""
        data = self.json

        if path:
            # Navigate to nested path
            for key in path.split("."):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    assert False, msg or f"Path {path} not found in JSON"

        if isinstance(expected_structure, list):
            # Check if all keys are present
            if isinstance(data, dict):
                for key in expected_structure:
                    assert key in data, msg or f"Key '{key}' not found in JSON structure"
            else:
                assert False, (
                    msg or f"Expected dict at path {path or 'root'}, got {type(data)}"
                )
        elif isinstance(expected_structure, dict):
            # Recursively check nested structure
            for key, nested_structure in expected_structure.items():
                assert key in data, msg or f"Key '{key}' not found in JSON"
                self.assert_json_structure(
                    nested_structure, f"{path}.{key}" if path else key, msg
                )

    def assert_json_path(self, path, expected_value, msg=None):
        """Assert JSON path has expected value."""
        data = self.json

        for key in path.split("."):
            if isinstance(data, dict) and key in data:
                data = data[key]
            elif isinstance(data, list) and key.isdigit():
                index = int(key)
                if 0 <= index < len(data):
                    data = data[index]
                else:
                    assert False, (
                        msg or f"Index {index} out of range for array at path {path}"
                    )
            else:
                assert False, msg or f"Path {path} not found in JSON"

        assert data == expected_value, (
            msg or f"Path {path}: expected {expected_value}, got {data}"
        )

    def assert_json_has_key(self, key, msg=None):
        """Assert JSON has key."""
        assert key in self.json, msg or f"Key '{key}' not found in JSON"

    def assert_json_missing_key(self, key, msg=None):
        """Assert JSON does not have key."""
        assert key not in self.json, (
            msg or f"Key '{key}' found in JSON but should be missing"
        )

    def assert_json_count(self, expected_count, path=None, msg=None):
        """Assert JSON array/object has expected count."""
        data = self.json

        if path:
            for key in path.split("."):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    assert False, msg or f"Path {path} not found in JSON"

        if isinstance(data, (list, dict)):
            actual_count = len(data)
            assert actual_count == expected_count, (
                msg or f"Expected count {expected_count}, got {actual_count}"
            )
        else:
            assert False, msg or f"Cannot count non-array/object at path {path or 'root'}"

    # Validation Error Assertions
    def assert_json_validation_errors(self, expected_fields, msg=None):
        """Assert JSON contains validation errors for expected fields."""
        assert "errors" in self.json, msg or "No 'errors' key found in JSON response"

        errors = self.json["errors"]
        for field in expected_fields:
            assert field in errors, (
                msg or f"Validation error for field '{field}' not found"
            )

    def assert_json_validation_error(self, field, expected_message=None, msg=None):
        """Assert JSON contains validation error for specific field."""
        assert "errors" in self.json, msg or "No 'errors' key found in JSON response"

        errors = self.json["errors"]
        assert field in errors, msg or f"Validation error for field '{field}' not found"

        if expected_message:
            field_errors = errors[field]
            if isinstance(field_errors, list):
                assert expected_message in field_errors, (
                    msg
                    or f"Expected message '{expected_message}' not found in field errors"
                )
            else:
                assert field_errors == expected_message, (
                    msg or f"Expected message '{expected_message}', got '{field_errors}'"
                )

    # Content Assertions
    def assert_contains(self, expected_text, msg=None):
        """Assert response content contains text."""
        assert expected_text in self.text, (
            msg or f"Text '{expected_text}' not found in response"
        )

    def assert_not_contains(self, unexpected_text, msg=None):
        """Assert response content does not contain text."""
        assert unexpected_text not in self.text, (
            msg or f"Text '{unexpected_text}' found in response but should not be"
        )

    def assert_see(self, text, msg=None):
        """Assert response contains visible text."""
        self.assert_contains(text, msg)

    def assert_dont_see(self, text, msg=None):
        """Assert response does not contain visible text."""
        self.assert_not_contains(text, msg)

    # Header Assertions
    def assert_header(self, header_name, expected_value=None, msg=None):
        """Assert response has header with optional value check."""
        assert header_name in self.headers, (
            msg or f"Header '{header_name}' not found in response"
        )

        if expected_value is not None:
            actual_value = self.headers[header_name]
            assert actual_value == expected_value, (
                msg
                or f"Header '{header_name}': expected '{expected_value}', got '{actual_value}'"
            )

    def assert_header_missing(self, header_name, msg=None):
        """Assert response does not have header."""
        assert header_name not in self.headers, (
            msg or f"Header '{header_name}' found in response but should be missing"
        )

    # Cookie Assertions
    def assert_cookie(self, cookie_name, expected_value=None, msg=None):
        """Assert response sets cookie with optional value check."""
        set_cookie = self.headers.get("Set-Cookie", "")
        assert cookie_name in set_cookie, (
            msg or f"Cookie '{cookie_name}' not set in response"
        )

        if expected_value is not None:
            # Simple check - in real implementation would parse cookie properly
            assert f"{cookie_name}={expected_value}" in set_cookie, (
                msg or f"Cookie '{cookie_name}' value mismatch"
            )

    def assert_cookie_expired(self, cookie_name, msg=None):
        """Assert response expires cookie."""
        set_cookie = self.headers.get("Set-Cookie", "")
        assert f"{cookie_name}=" in set_cookie and "expires=" in set_cookie, (
            msg or f"Cookie '{cookie_name}' not expired"
        )

    # Redirect Assertions
    def assert_redirect(self, expected_url=None, msg=None):
        """Assert response is redirect with optional URL check."""
        assert self.status_code in [301, 302, 303, 307, 308], (
            msg or f"Expected redirect status, got {self.status_code}"
        )

        if expected_url is not None:
            location = self.headers.get("Location")
            assert location == expected_url, (
                msg or f"Redirect location: expected '{expected_url}', got '{location}'"
            )

    # File Download Assertions
    def assert_download(self, expected_filename=None, msg=None):
        """Assert response is file download."""
        content_disposition = self.headers.get("Content-Disposition", "")
        assert "attachment" in content_disposition, (
            msg or "Response is not a file download"
        )

        if expected_filename:
            assert f'filename="{expected_filename}"' in content_disposition, (
                msg or "Download filename mismatch"
            )

    # Utility Methods
    def dump(self):
        """Dump response for debugging."""
        print(f"Status: {self.status_code}")
        print(f"Headers: {self.headers}")
        print(f"Content: {self.content}")
        if self.json:
            print(f"JSON: {json.dumps(self.json, indent=2)}")

    def __str__(self):
        """String representation of response."""
        return (
            f"TestResponse(status={self.status_code}, content_length={len(self.content)})"
        )

    def __repr__(self):
        """Detailed representation of response."""
        return f"TestResponse(status_code={self.status_code}, headers={self.headers}, content='{self.content[:100]}...')"
