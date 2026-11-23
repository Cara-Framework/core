"""``Image.make`` decides the format from the magic bytes before any decode.

The live defect: the source went straight to ``PILImage.open`` (every
registered plugin — BMP, PCX, TIFF … — free to parse the buffer) and then to
``copy()``, which decodes every pixel; only AFTER that were the format
allow-list and the pixel cap checked. A disguised or disallowed upload, or a
decompression bomb, was fully decoded before being refused.
"""

from __future__ import annotations

import importlib
from io import BytesIO

import pytest
from PIL import Image as PILImage

from cara.exceptions import InvalidArgumentException
from cara.support.ImageProcessor import ImageProcessor

image_module = importlib.import_module("cara.support.Image")


def _encoded(fmt: str, size: tuple[int, int] = (4, 3)) -> bytes:
    buffer = BytesIO()
    PILImage.new("RGB", size, "red").save(buffer, format=fmt)
    return buffer.getvalue()


@pytest.fixture
def opens(monkeypatch) -> list[list[str] | None]:
    """Records every ``PILImage.open`` the module makes, and its ``formats``."""
    seen: list[list[str] | None] = []
    real_open = PILImage.open

    def spy(stream, mode="r", formats=None):
        seen.append(formats)
        return real_open(stream, mode, formats)

    monkeypatch.setattr(image_module.PILImage, "open", spy)
    return seen


@pytest.fixture
def copies(monkeypatch) -> list[int]:
    counted: list[int] = []
    real_copy = PILImage.Image.copy

    def spy(self):
        counted.append(1)
        return real_copy(self)

    monkeypatch.setattr(PILImage.Image, "copy", spy)
    return counted


def test_a_disallowed_format_is_refused_without_opening_it(opens):
    with pytest.raises(InvalidArgumentException, match="Unsupported image format"):
        image_module.Image.make(_encoded("BMP"))

    assert opens == []


def test_a_disguised_file_is_judged_by_its_bytes_not_its_name(tmp_path, opens):
    disguised = tmp_path / "avatar.png"
    disguised.write_bytes(_encoded("BMP"))

    with pytest.raises(InvalidArgumentException, match="Unsupported image format"):
        image_module.Image.make(str(disguised))

    assert opens == []


def test_a_recognised_format_off_the_allow_list_is_refused(opens, monkeypatch):
    monkeypatch.setattr(ImageProcessor, "ALLOWED_FORMATS", frozenset({"PNG"}))

    with pytest.raises(InvalidArgumentException):
        image_module.Image.make(_encoded("GIF"))

    assert opens == []


def test_an_allowed_image_opens_with_only_its_sniffed_decoder(opens):
    processor = image_module.Image.make(_encoded("PNG"))

    assert isinstance(processor, ImageProcessor)
    assert opens == [["PNG"]]


def test_the_pixel_cap_is_enforced_before_any_pixel_is_copied(copies, monkeypatch):
    monkeypatch.setattr(ImageProcessor, "MAX_PIXEL_COUNT", 10)

    with pytest.raises(InvalidArgumentException, match="pixel cap"):
        image_module.Image.make(_encoded("PNG", (4, 3)))

    assert copies == []
