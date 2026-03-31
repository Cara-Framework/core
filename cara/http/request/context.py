"""
HTTP Request Context Utilities for the Cara framework.

This module provides context management utilities for HTTP requests, such as storing and retrieving
request-scoped data.
"""

import contextvars

current_request = contextvars.ContextVar("current_request")
