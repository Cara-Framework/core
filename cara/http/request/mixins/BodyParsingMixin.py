"""
Body Parsing Mixin for HTTP Request.

This mixin provides functionality for parsing request bodies including JSON, form data,
and multipart file uploads with Laravel-like validation and error handling.
"""

import json
from typing import Any, Dict, Optional

from multipart import MultipartParser
from multipart.multipart import parse_options_header

from cara.exceptions import BadRequestException
from cara.http.request import UploadedFile


class BodyParsingMixin:
    """
    Mixin providing body parsing functionality for HTTP requests.

    Handles JSON, form data, and file uploads with proper error handling, validation,
    and caching following Laravel patterns.
    """

    # Maximum body size (10MB by default)
    MAX_BODY_SIZE = 10 * 1024 * 1024

    # Maximum number of files
    MAX_FILES = 20

    # Maximum file size (10MB by default)
    MAX_FILE_SIZE = 10 * 1024 * 1024

    async def _read_body(self) -> bytes:
        """
        Read and cache the raw request body from ASGI receive.

        Raises BadRequestException if the body has already been consumed or fails.
        """
        if self._body is not None:
            return self._body

        if self._body_consumed:
            raise BadRequestException("Request body stream already consumed")

        try:
            body = b""
            more = True
            total_size = 0

            while more:
                message = await self.receive()
                chunk = message.get("body", b"")

                # Check body size limit
                total_size += len(chunk)
                if total_size > self.MAX_BODY_SIZE:
                    raise BadRequestException(
                        f"Request body too large. Maximum size: {self.MAX_BODY_SIZE} bytes"
                    )

                body += chunk
                more = message.get("more_body", False)

            self._body = body
            self._body_consumed = True
            return body
        except Exception as exc:
            self._body_consumed = True
            if isinstance(exc, BadRequestException):
                raise
            raise BadRequestException(f"Failed to read request body: {exc}") from exc

    async def json(self) -> Dict[str, Any]:
        """
        Parse and cache JSON body.

        Returns empty dict on empty body. Raises BadRequestException on invalid JSON.
        """
        if self._json_data is not None:
            return self._json_data

        raw = await self._read_body()
        if not raw:
            self._json_data = {}
            return self._json_data

        try:
            self._json_data = json.loads(raw)
            return self._json_data
        except json.JSONDecodeError as exc:
            raise BadRequestException(f"Invalid JSON body: {exc}") from exc

    def _validate_multipart_structure(self, content_type: str) -> Optional[bytes]:
        """Validate multipart content type and extract boundary."""
        if "multipart/form-data" not in content_type:
            return None

        try:
            params = parse_options_header(content_type.encode())[1]
            boundary = params.get(b"boundary")
            if not boundary:
                raise BadRequestException("Missing boundary in multipart/form-data")
            return boundary
        except Exception as exc:
            raise BadRequestException(
                f"Invalid multipart content-type header: {exc}"
            ) from exc

    def _validate_uploaded_file(
        self, name: str, filename: str, content: bytes, content_type: str
    ) -> None:
        """Validate uploaded file before creating UploadedFile instance."""
        # Check file size
        if len(content) > self.MAX_FILE_SIZE:
            raise BadRequestException(
                f"File '{filename}' exceeds maximum size of {self.MAX_FILE_SIZE} bytes"
            )

        # Check if file is empty
        if len(content) == 0:
            raise BadRequestException(f"File '{filename}' is empty")

        # Validate filename
        if not filename or filename.strip() == "":
            raise BadRequestException("Filename cannot be empty")

        # Check for dangerous filenames
        dangerous_names = [".", "..", ""]
        if filename in dangerous_names:
            raise BadRequestException(f"Invalid filename: '{filename}'")

        # Check for path traversal in filename
        if "/" in filename or "\\" in filename or ".." in filename:
            raise BadRequestException(
                f"Filename contains invalid characters: '{filename}'"
            )

    async def _parse_multipart(self) -> None:
        """
        Parse multipart/form‐data body into files and form parameters.

        Caches results so subsequent calls do not re‐parse.
        Enhanced with Laravel-like validation and error handling.
        """
        if self._files is not None:
            return

        self._files = {}
        self._form_params = {}
        content_type = self.header("content-type", "")

        # Validate and extract boundary
        boundary = self._validate_multipart_structure(content_type)
        if boundary is None:
            self._form_params = {}
            return

        raw = await self._read_body()
        if not raw:
            return

        try:
            # Storage for current part being processed
            current_part = {}
            file_count = 0

            def on_part_begin():
                nonlocal current_part
                current_part = {
                    "headers": {},
                    "name": None,
                    "filename": None,
                    "content_type": None,
                    "data": b"",
                }

            def on_header_field(data, start, end):
                current_part["_header_field"] = data[start:end].decode("utf-8").lower()

            def on_header_value(data, start, end):
                field = current_part.get("_header_field", "")
                value = data[start:end].decode("utf-8")
                current_part["headers"][field] = value

                # Parse Content-Disposition header
                if field == "content-disposition":
                    disp_parts = value.split(";")
                    for part in disp_parts[1:]:  # Skip the first part (form-data)
                        if "=" in part:
                            key, val = part.strip().split("=", 1)
                            val = val.strip('"')  # Remove quotes
                            if key.strip() == "name":
                                current_part["name"] = val
                            elif key.strip() == "filename":
                                current_part["filename"] = val
                elif field == "content-type":
                    current_part["content_type"] = value

            def on_part_data(data, start, end):
                current_part["data"] += data[start:end]

            def on_part_end():
                nonlocal current_part, file_count
                name = current_part.get("name")
                filename = current_part.get("filename")
                content = current_part.get("data", b"")

                if not name:
                    return  # Skip parts without names

                if filename:
                    # This is a file upload
                    file_count += 1
                    if file_count > self.MAX_FILES:
                        raise BadRequestException(
                            f"Too many files uploaded. Maximum: {self.MAX_FILES}"
                        )

                    # Validate file before creating UploadedFile
                    self._validate_uploaded_file(
                        name,
                        filename,
                        content,
                        current_part.get("content_type", "application/octet-stream"),
                    )

                    # Create UploadedFile instance
                    uploaded_file = UploadedFile(
                        name=name,
                        filename=filename,
                        content_type=current_part.get(
                            "content_type", "application/octet-stream"
                        ),
                        content=content,
                    )

                    # Validate the created file
                    if not uploaded_file.is_valid():
                        raise BadRequestException(f"Invalid file upload: '{filename}'")

                    self._files[name] = uploaded_file
                else:
                    # This is a form field
                    try:
                        self._form_params[name] = content.decode("utf-8")
                    except UnicodeDecodeError:
                        # Fallback to latin-1 for binary data in forms
                        try:
                            self._form_params[name] = content.decode("latin-1")
                        except UnicodeDecodeError:
                            # Last resort: base64 encode
                            import base64

                            self._form_params[name] = base64.b64encode(content).decode(
                                "ascii"
                            )

            # Create parser with callbacks
            parser = MultipartParser(
                boundary,
                callbacks={
                    "on_part_begin": on_part_begin,
                    "on_header_field": on_header_field,
                    "on_header_value": on_header_value,
                    "on_part_data": on_part_data,
                    "on_part_end": on_part_end,
                },
            )

            # Write data to parser
            parser.write(raw)
            parser.finalize()

        except Exception as exc:
            if isinstance(exc, BadRequestException):
                raise
            raise BadRequestException(f"Failed to parse multipart data: {exc}") from exc

    async def form(self) -> Dict[str, Any]:
        """
        Return form parameters parsed from body.

        If multipart, also populates self._files.
        """
        if self._form_params is None:
            await self._parse_multipart()
        return self._form_params or {}

    async def files(self) -> Dict[str, UploadedFile]:
        """
        Return uploaded files; triggers multipart parsing if needed.

        Returns a dict of field_name -> UploadedFile instances.
        """
        if self._files is None:
            await self._parse_multipart()
        return self._files or {}

    async def file(self, name: str) -> Optional[UploadedFile]:
        """
        Get a specific uploaded file by field name.

        Args:
            name: The form field name

        Returns:
            UploadedFile instance or None if not found
        """
        files = await self.files()
        return files.get(name)

    async def has_file(self, name: str) -> bool:
        """
        Check if a file was uploaded for the given field name.

        Args:
            name: The form field name

        Returns:
            True if file exists and is valid
        """
        file = await self.file(name)
        return file is not None and file.is_valid()

    async def validate_file(self, field_name: str, **rules) -> Optional[str]:
        """
        Simple file validation - Laravel style.

        Args:
            field_name: Form field name
            **rules: max_size, required, image

        Returns:
            Error message or None
        """
        file = await self.file(field_name)

        # Required check
        if rules.get("required", False) and not file:
            return f"The {field_name} field is required."

        if not file:
            return None

        # Size check
        if "max_size" in rules and file.size > rules["max_size"]:
            max_mb = rules["max_size"] / (1024 * 1024)
            return f"The {field_name} may not be greater than {max_mb:.1f}MB."

        # Image check
        if rules.get("image", False) and not file.is_image():
            return f"The {field_name} must be an image."

        return None

    async def all(self) -> Dict[str, Any]:
        """
        Combine all inputs (query, form, JSON) with priority: JSON > form > query.

        Returns a flat dict with Laravel-like behavior.
        """
        # Get query params with proper flattening
        result: Dict[str, Any] = {}
        for key, value in self._input.all_as_values().items():
            if isinstance(value, list) and len(value) == 1 and not key.endswith("[]"):
                result[key] = value[0]  # Flatten single-item lists
            else:
                result[key] = value

        # Check content type to avoid race condition between form() and json()
        content_type = self.header("content-type", "").lower()

        if (
            "multipart/form-data" in content_type
            or "application/x-www-form-urlencoded" in content_type
        ):
            # Handle as form data
            try:
                form_data = await self.form()
                if form_data:
                    result.update(form_data)
            except BadRequestException:
                # Log error but don't break the request
                pass
        elif "application/json" in content_type:
            # Handle as JSON data
            try:
                json_data = await self.json()
                if isinstance(json_data, dict):
                    result.update(json_data)
            except BadRequestException:
                # Invalid JSON → ignore, do not break overall parsing
                pass
        else:
            # Try JSON first, then form as fallback
            try:
                json_data = await self.json()
                if isinstance(json_data, dict):
                    result.update(json_data)
            except BadRequestException:
                # If JSON fails, try form data
                try:
                    form_data = await self.form()
                    if form_data:
                        result.update(form_data)
                except BadRequestException:
                    # Both failed, continue with query params only
                    pass

        return result
