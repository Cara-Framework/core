"""Authoritative lifetime ceiling for authentication credentials and evidence."""

from datetime import timedelta

AUTH_SECURITY_MAX_WINDOW = timedelta(days=30)


__all__ = ["AUTH_SECURITY_MAX_WINDOW"]
