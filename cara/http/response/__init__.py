"""
HTTP Response Module - Laravel-inspired modular response system.

Exports:
- Response: Main response class (Laravel-style orchestrator)
- BaseResponse: Core ASGI functionality
- ResponseFactory: Factory methods for different response types
- HeaderManager: Robust header management
- ContentTypeDetector: Smart content-type detection
- StreamingResponse: Streaming capabilities
- ResponseProvider: DI provider (existing)
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BaseResponse": (".BaseResponse", "BaseResponse"),
    "ContentTypeDetector": (".ContentTypeDetector", "ContentTypeDetector"),
    "HeaderManager": (".HeaderManager", "HeaderManager"),
    "Response": (".Response", "Response"),
    "ResponseFactory": (".ResponseFactory", "ResponseFactory"),
    "ResponseProvider": (".ResponseProvider", "ResponseProvider"),
    "StreamingResponse": (".StreamingResponse", "StreamingResponse"),
}

__all__ = [
    "BaseResponse",
    "ContentTypeDetector",
    "HeaderManager",
    "Response",
    "ResponseFactory",
    "ResponseProvider",
    "StreamingResponse",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
