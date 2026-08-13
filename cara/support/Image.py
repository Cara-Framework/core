"""
Image Processing for Cara Framework.

Laravel Intervention Image style fluent API.
"""

from __future__ import annotations

from io import BytesIO

from PIL import Image as PILImage

from cara.exceptions import InvalidArgumentException

from .ImageProcessor import ImageProcessor


class Image:
    """
    Image facade - Laravel Intervention style.

    Usage: Image.make(source).orientate().resize(300, 300).save(path)
    """

    @staticmethod
    def make(source: str | bytes) -> ImageProcessor:
        """
        Create image processor from source.

        Args:
            source: File path or bytes

        Raises:
            ValueError: when the source format isn't on
              ``ImageProcessor.ALLOWED_FORMATS`` or the declared
              dimensions exceed ``ImageProcessor.MAX_PIXEL_COUNT``.
              Both gates fire BEFORE any pixel data is decoded so
              a small decompression-bomb buffer cannot wedge the
              worker on memory.
        """
        if isinstance(source, str):
            with PILImage.open(source) as opened:
                fmt = (opened.format or "").upper()
                image = opened.copy()
        elif isinstance(source, bytes):
            with PILImage.open(BytesIO(source)) as opened:
                fmt = (opened.format or "").upper()
                image = opened.copy()
        else:
            raise InvalidArgumentException(f"Unsupported source type: {type(source)}")

        # Format allowlist — gate on the Pillow-identified decoder,
        # not the file extension (which the uploader controls). The
        # header bytes have been read at this point but no pixel
        # data has been decoded yet, so the check is cheap.
        if fmt not in ImageProcessor.ALLOWED_FORMATS:
            allowed = ", ".join(sorted(ImageProcessor.ALLOWED_FORMATS))
            raise InvalidArgumentException(
                f"Unsupported image format {fmt!r}; allowed: {allowed}"
            )

        # Pixel-count guard — declared dimensions only, no full
        # decode. A 100_000 × 100_000 PNG-bomb header is rejected
        # here before ``.load()`` ever runs.
        width, height = image.size
        if width * height > ImageProcessor.MAX_PIXEL_COUNT:
            raise InvalidArgumentException(
                f"Image exceeds the {ImageProcessor.MAX_PIXEL_COUNT}-"
                f"pixel cap (got {width}x{height} = {width * height} pixels)"
            )

        return ImageProcessor(image)
