"""
Uploaded File Handler for Cara Framework.

Laravel-style file upload handling with clean, minimal API.
"""

import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass
class UploadedFile:
    """
    Laravel-style uploaded file handler.

    Provides clean API for file uploads:
    - store('directory') - Auto filename
    - store_as('directory', 'filename') - Custom filename
    """

    name: str
    filename: str
    content_type: str
    content: bytes

    def __str__(self) -> str:
        return f"UploadedFile({self.filename}, {self.size} bytes)"

    @property
    def size(self) -> int:
        """File size in bytes."""
        return len(self.content)

    @property
    def extension(self) -> str:
        """File extension without dot."""
        if not self.filename:
            return ""
        return Path(self.filename).suffix.lower().lstrip(".")

    @property
    def mime_type(self) -> str:
        """MIME type."""
        return self.content_type or "application/octet-stream"

    def is_valid(self) -> bool:
        """Check if file is valid."""
        return bool(self.filename and self.content and len(self.content) > 0)

    def is_image(self) -> bool:
        """Check if file is image."""
        return self.mime_type.startswith("image/")

    def store(self, directory: str) -> str:
        """
        Store file with auto-generated filename.

        Laravel: $file->store('receipts')

        Returns: 'receipts/abc123.jpg'
        """

        # Generate unique filename
        extension = self.extension or "bin"
        unique_filename = f"{uuid.uuid4().hex}.{extension}"

        # Store file
        return self._store_file(directory, unique_filename)

    def store_as(self, directory: str, filename: str) -> str:
        """
        Store file with custom filename.

        Laravel: $file->storeAs('receipts', 'custom.jpg')

        Returns: 'receipts/custom.jpg'
        """
        return self._store_file(directory, filename)

    def _store_file(self, directory: str, filename: str) -> str:
        """Internal file storage method."""
        from cara.support.paths import paths

        # Get storage path
        storage_base = paths("storage")
        full_directory = Path(storage_base) / directory

        # Create directory
        full_directory.mkdir(parents=True, exist_ok=True)

        # Write file
        full_path = full_directory / filename
        with open(full_path, "wb") as f:
            f.write(self.content)

        # Return Laravel-style relative path
        return f"{directory}/{filename}"
