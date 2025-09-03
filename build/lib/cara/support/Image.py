"""
Image Processing for Cara Framework.

Laravel Intervention Image style fluent API.
"""

import os
from io import BytesIO
from typing import Union

from PIL import ExifTags
from PIL import Image as PILImage


class ImageProcessor:
    """
    Fluent image processor - Laravel Intervention style.

    Usage: Image.make(source).orientate().resize(300, 300).save(path)
    """

    def __init__(self, image: PILImage.Image):
        self.image = image
        self._orientated = False

    def orientate(self) -> "ImageProcessor":
        """Auto-rotate based on EXIF data."""
        if self._orientated:
            return self

        try:
            if hasattr(self.image, "_getexif") and self.image._getexif():
                for orientation in ExifTags.TAGS.keys():
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
        except (AttributeError, KeyError, IndexError, TypeError):
            pass

        self._orientated = True
        return self

    def resize(self, width: int, height: int) -> "ImageProcessor":
        """Resize image maintaining aspect ratio."""
        self.image.thumbnail((width, height), PILImage.Resampling.LANCZOS)
        return self

    def save(self, path: str, quality: int = 85) -> str:
        """Save image to path."""
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

    def thumbnail(self, width: int, height: int) -> "ImageProcessor":
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
    ) -> "ImageProcessor":
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


class Image:
    """
    Image facade - Laravel Intervention style.

    Usage: Image.make(source).orientate().resize(300, 300).save(path)
    """

    @staticmethod
    def make(source: Union[str, bytes]) -> ImageProcessor:
        """
        Create image processor from source.

        Args:
            source: File path or bytes
        """
        if isinstance(source, str):
            image = PILImage.open(source)
        elif isinstance(source, bytes):
            image = PILImage.open(BytesIO(source))
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

        return ImageProcessor(image)
