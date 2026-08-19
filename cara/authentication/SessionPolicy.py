"""Authoritative lifetime ceiling for authentication credentials and evidence."""

from datetime import timedelta

AUTH_SECURITY_MAX_WINDOW = timedelta(days=30)
AUTH_REFRESH_REPLAY_WINDOW = 32


__all__ = ["AUTH_REFRESH_REPLAY_WINDOW", "AUTH_SECURITY_MAX_WINDOW"]
