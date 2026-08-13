"""ImageProcessor."""

from __future__ import annotations

import os

from PIL import ExifTags
from PIL import Image as PILImage

from cara.exceptions import InvalidArgumentException


class ImageProcessor:
    """
    Fluent image processor - Laravel Intervention style.

    Usage: Image.make(source).orientate().resize(300, 300).save(path)
    """

    # Hard pixel cap — refused at load time so a small file that
    # decodes to ``100_000 × 100_000`` (≈10⁴ MB of RGB) can't OOM
    # the worker. 25M pixels covers any legitimate avatar / logo /
    # banner with comfortable headroom (a 5K × 5K banner is 25M).
    # Pillow's default ``MAX_IMAGE_PIXELS`` is ~89M but only WARNS —
    # too lax and too quiet for a public-internet upload surface.
    MAX_PIXEL_COUNT = 25_000_000

    # Allowlist of decoder formats accepted on load. Restricts the
    # blast radius of future Pillow CVEs (the Pillow 9.x BMP RLE
    # crash, the CVE-2023-4863 WebP CVE family, the PCX OOB read)
    # to the four formats real avatar / logo uploads actually use.
    # BMP / TIFF / ICO are NOT in this set — none of those formats
    # has a legitimate use case in a client upload field, and each
    # has CVE history.
    ALLOWED_FORMATS = frozenset({"JPEG", "PNG", "GIF", "WEBP"})

    def __init__(self, image: PILImage.Image):
        self.image = image
        self._orientated = False

    def orientate(self) -> ImageProcessor:
        """Auto-rotate based on EXIF data."""
        if self._orientated:
            return self

        try:
            if hasattr(self.image, "_getexif") and self.image._getexif():
                for orientation in ExifTags.TAGS:
                    if ExifTags.TAGS[orientation] == "Orientation":
                        break

                exif = self.image._getexif()
                if exif and orientation in exif:
                    if exif[orientation] == 3:
                        self.image = self.image.rotate(180, expand=True)
                    elif exif[orientation] == 6:
                        self.image = self.image.rotate(270, expand=True)
                    elif exif[orientation] == 8:
                        self.image = self.image.rotate(90, expand=True)
        except AttributeError, KeyError, IndexError, TypeError:
            pass

        self._orientated = True
        return self

    def resize(self, width: int, height: int) -> ImageProcessor:
        """Resize image maintaining aspect ratio."""
        self.image.thumbnail((width, height), PILImage.Resampling.LANCZOS)
        return self

    def save(self, path: str, quality: int = 85) -> str:
        """Save image to path.

        Refuses path-traversal forms (``..`` segments) and NUL-byte
        injection. Callers compose paths like
        ``f"/var/uploads/{user_filename}"`` — a forgetful caller
        that doesn't sanitise the user-supplied filename component
        would otherwise honour ``filename="../../../etc/passwd"``:
        ``os.makedirs`` creates the parent dirs, ``PIL.save`` writes
        JPEG bytes over the target. Refusing at the helper boundary
        means every consumer of the framework benefits from one
        consistent defence regardless of how they assemble their
        path string.
        """
        if not isinstance(path, str) or not path:
            raise InvalidArgumentException("save() requires a non-empty path string")
        # NUL bytes in paths historically truncate the target on
        # ancient libc filesystems; on modern ones the kernel returns
        # an error but the helper should reject ahead of the syscall
        # so the error message identifies the cause.
        if "\x00" in path:
            raise InvalidArgumentException("save() path must not contain NUL bytes")
        # Path-segment scan — refuse any ``..`` segment regardless of
        # whether the OS would resolve it inside or outside the
        # caller's intended base directory. Catches both ``a/../b``
        # and ``a\\..\\b`` (Windows separator).
        parts = path.replace("\\", "/").split("/")
        if any(part == ".." for part in parts):
            raise InvalidArgumentException(
                f"save() path must not contain '..' segments (traversal): {path!r}"
            )

        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Convert RGBA to RGB if needed (for JPEG compatibility)
        if self.image.mode in ("RGBA", "LA", "P"):
            # Create white background for transparency
            background = PILImage.new("RGB", self.image.size, (255, 255, 255))
            if self.image.mode == "P":
                self.image = self.image.convert("RGBA")
            background.paste(
                self.image,
                mask=self.image.split()[-1]
                if self.image.mode in ("RGBA", "LA")
                else None,
            )
            self.image = background

        # Save image
        self.image.save(path, "JPEG", quality=quality)
        return path

    def thumbnail(self, width: int, height: int) -> ImageProcessor:
        """Create thumbnail with simple resize - no cropping."""
        # Resize to fit within max dimensions while maintaining aspect ratio
        max_width = 140
        max_height = 120

        current_width, current_height = self.image.size

        # Calculate scaling factor to fit within max dimensions
        scale_x = max_width / current_width
        scale_y = max_height / current_height
        scale = min(scale_x, scale_y, 1.0)  # Don't upscale

        if scale < 1.0:
            new_width = int(current_width * scale)
            new_height = int(current_height * scale)
            self.image = self.image.resize(
                (new_width, new_height), PILImage.Resampling.LANCZOS
            )

        return self

    def scaled_down_original(
        self, max_width: int = 800, max_height: int = 1200
    ) -> ImageProcessor:
        """
        Create a scaled-down version of the original image.

        This maintains the original aspect ratio but reduces the size
        for better performance on mobile devices.
        """
        current_width, current_height = self.image.size

        # Calculate scaling factor to fit within max dimensions
        scale_x = max_width / current_width
        scale_y = max_height / current_height
        scale = min(scale_x, scale_y, 1.0)  # Don't upscale

        if scale < 1.0:
            new_width = int(current_width * scale)
            new_height = int(current_height * scale)
            self.image = self.image.resize(
                (new_width, new_height), PILImage.Resampling.LANCZOS
            )

        return self
