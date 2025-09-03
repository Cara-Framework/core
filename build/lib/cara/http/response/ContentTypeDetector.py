"""
Content Type Detection Module.

Smart content-type detection for HTTP responses when explicit methods are not used.
Laravel-inspired fallback mechanism for automatic content-type assignment.
"""

import json
from typing import Union


class ContentTypeDetector:
    """
    Smart content-type detector for HTTP responses.

    Provides Laravel-style fallback content-type detection when explicit methods
    like json(), html(), text() are not used. Analyzes content to determine
    the most appropriate content-type.
    """

    @staticmethod
    def detect(content: Union[str, bytes]) -> str:
        """
        Detect content type based on content analysis.

        Laravel-style smart detection with fallback to text/plain.

        Args:
            content: Content to analyze

        Returns:
            str: Detected content type with charset
        """
        if not content:
            return "text/plain; charset=utf-8"

        # Convert bytes to string for analysis
        if isinstance(content, bytes):
            try:
                content_str = content.decode("utf-8", errors="ignore").strip()
            except UnicodeDecodeError:
                return "application/octet-stream"
        else:
            content_str = content.strip()

        # HTML Detection (Laravel priority)
        if ContentTypeDetector._is_html(content_str):
            return "text/html; charset=utf-8"

        # XML Detection
        if ContentTypeDetector._is_xml(content_str):
            return "application/xml; charset=utf-8"

        # JSON Detection (Laravel does this for arrays)
        if ContentTypeDetector._is_json(content_str):
            return "application/json; charset=utf-8"

        # CSS Detection
        if ContentTypeDetector._is_css(content_str):
            return "text/css; charset=utf-8"

        # JavaScript Detection
        if ContentTypeDetector._is_javascript(content_str):
            return "application/javascript; charset=utf-8"

        # SVG Detection
        if ContentTypeDetector._is_svg(content_str):
            return "image/svg+xml; charset=utf-8"

        # Default to plain text (Laravel default for strings)
        return "text/plain; charset=utf-8"

    @staticmethod
    def _is_html(content: str) -> bool:
        """
        Check if content appears to be HTML.

        Args:
            content: Content to check

        Returns:
            bool: True if content appears to be HTML
        """
        content_lower = content.lower()

        # Check for HTML document declarations
        if content_lower.startswith("<!doctype html"):
            return True
        if content_lower.startswith("<html"):
            return True

        # Check for common HTML tags
        html_indicators = [
            "<head>",
            "<body>",
            "<div>",
            "<span>",
            "<p>",
            "<h1>",
            "<h2>",
            "<h3>",
            "<h4>",
            "<h5>",
            "<h6>",
            "<title>",
            "<meta",
            "<link",
            "<script",
            "<style",
        ]

        return any(indicator in content_lower for indicator in html_indicators)

    @staticmethod
    def _is_xml(content: str) -> bool:
        """
        Check if content appears to be XML.

        Args:
            content: Content to check

        Returns:
            bool: True if content appears to be XML
        """
        content_stripped = content.strip()

        # Check for XML declaration
        if content_stripped.startswith("<?xml"):
            return True

        # Check for XML-like structure (starts with < and has closing tags)
        if (
            content_stripped.startswith("<")
            and not ContentTypeDetector._is_html(content)
            and not ContentTypeDetector._is_svg(content)
        ):
            # Simple heuristic: if it has matching opening/closing tags
            return "</" in content_stripped

        return False

    @staticmethod
    def _is_json(content: str) -> bool:
        """
        Check if content appears to be valid JSON.

        Args:
            content: Content to check

        Returns:
            bool: True if content is valid JSON
        """
        try:
            json.loads(content)
            return True
        except (json.JSONDecodeError, ValueError):
            return False

    @staticmethod
    def _is_css(content: str) -> bool:
        """
        Check if content appears to be CSS.

        Args:
            content: Content to check

        Returns:
            bool: True if content appears to be CSS
        """
        content_lower = content.lower()

        # CSS-specific keywords and patterns
        css_indicators = [
            "@media",
            "@keyframes",
            "@import",
            "@charset",
            "body{",
            "html{",
            ".class",
            "#id",
            ":",
            "font-family:",
            "color:",
            "background:",
            "margin:",
            "padding:",
            "width:",
            "height:",
            "display:",
        ]

        # Check for CSS selectors and properties
        has_css_indicators = any(
            indicator in content_lower for indicator in css_indicators
        )

        # Check for CSS-like structure (selector { property: value; })
        has_css_structure = "{" in content and "}" in content and ":" in content

        return has_css_indicators or has_css_structure

    @staticmethod
    def _is_javascript(content: str) -> bool:
        """
        Check if content appears to be JavaScript.

        Args:
            content: Content to check

        Returns:
            bool: True if content appears to be JavaScript
        """
        # JavaScript-specific keywords
        js_keywords = [
            "function",
            "var ",
            "let ",
            "const ",
            "=>",
            "console.",
            "document.",
            "window.",
            "alert(",
            "if (",
            "for (",
            "while (",
            "return ",
            "new ",
            "class ",
            "extends ",
            "import ",
            "export ",
            "async ",
            "await ",
            "Promise",
            "JSON.",
        ]

        return any(keyword in content for keyword in js_keywords)

    @staticmethod
    def _is_svg(content: str) -> bool:
        """
        Check if content appears to be SVG.

        Args:
            content: Content to check

        Returns:
            bool: True if content appears to be SVG
        """
        content_lower = content.lower().strip()

        # Check for SVG opening tag
        if "<svg" in content_lower:
            return True

        # Check for XML declaration followed by SVG
        if content_lower.startswith("<?xml") and "<svg" in content_lower:
            return True

        return False

    @staticmethod
    def get_charset_from_content_type(content_type: str) -> str:
        """
        Extract charset from content-type header.

        Args:
            content_type: Content-type header value

        Returns:
            str: Charset or 'utf-8' as default
        """
        if "charset=" in content_type.lower():
            try:
                charset_part = content_type.lower().split("charset=")[1]
                charset = charset_part.split(";")[0].strip()
                return charset
            except (IndexError, AttributeError):
                pass
        return "utf-8"

    @staticmethod
    def add_charset_if_missing(content_type: str, charset: str = "utf-8") -> str:
        """
        Add charset to content-type if not present.

        Args:
            content_type: Content-type string
            charset: Charset to add (default: utf-8)

        Returns:
            str: Content-type with charset
        """
        if "charset=" not in content_type.lower():
            return f"{content_type}; charset={charset}"
        return content_type
