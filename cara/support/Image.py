"""
Image Processing for Cara Framework.

Laravel Intervention Image style fluent API.
"""

from __future__ import annotations

from io import BytesIO

from PIL import Image as PILImage

from cara.exceptions import InvalidArgumentException

from .ImageProcessor import ImageProcessor

#: Bytes read to identify a format: the longest signature below (WebP's
#: ``RIFF????WEBP``) is 12.
_HEADER_BYTES = 16
_SIGNATURES = (
    ("JPEG", lambda h: h.startswith(b"\xff\xd8\xff")),
    ("PNG", lambda h: h.startswith(b"\x89PNG\r\n\x1a\n")),
    ("GIF", lambda h: h[:6] in (b"GIF87a", b"GIF89a")),
    ("WEBP", lambda h: h[:4] == b"RIFF" and h[8:12] == b"WEBP"),
)


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
            with open(source, "rb") as handle:
                header = handle.read(_HEADER_BYTES)
                fmt = Image._allowed_format(header)
                handle.seek(0)
                return Image._decode(handle, fmt)
        if isinstance(source, bytes):
            fmt = Image._allowed_format(source[:_HEADER_BYTES])
            return Image._decode(BytesIO(source), fmt)
        raise InvalidArgumentException(f"Unsupported source type: {type(source)}")

    @staticmethod
    def _allowed_format(header: bytes) -> str:
        """The allowlisted format the header's magic bytes declare, or raise.

        Decided from the raw header, BEFORE Pillow sees the stream: handing an
        unvetted buffer to ``PILImage.open`` lets every registered plugin
        parse it (the BMP / PCX / TIFF decoders the allowlist exists to keep
        away), and the format used to be checked only after ``copy()`` had
        decoded every pixel.
        """
        for fmt, matches in _SIGNATURES:
            if matches(header) and fmt in ImageProcessor.ALLOWED_FORMATS:
                return fmt
        allowed = ", ".join(sorted(ImageProcessor.ALLOWED_FORMATS))
        raise InvalidArgumentException(f"Unsupported image format; allowed: {allowed}")

    @staticmethod
    def _decode(stream, fmt: str) -> ImageProcessor:
        """Open with the one sniffed decoder, check the size, then decode."""
        with PILImage.open(stream, formats=[fmt]) as opened:
            # Pixel-count guard — declared dimensions only, no full
            # decode. A 100_000 × 100_000 PNG-bomb header is rejected
            # here before any pixel is decoded.
            width, height = opened.size
            if width * height > ImageProcessor.MAX_PIXEL_COUNT:
                raise InvalidArgumentException(
                    f"Image exceeds the {ImageProcessor.MAX_PIXEL_COUNT}-"
                    f"pixel cap (got {width}x{height} = {width * height} pixels)"
                )
            image = opened.copy()
        return ImageProcessor(image)
