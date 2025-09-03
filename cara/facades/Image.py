"""
Image Facade for Cara Framework.

Provides Laravel-style facade access to image processing utilities.
"""

from .Facade import Facade


class Image(metaclass=Facade):
    key = "image"
