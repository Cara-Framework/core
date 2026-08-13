"""
Mail Drivers Package.

This package contains various mail drivers for sending emails
through different protocols and services.
"""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ArrayDriver": (".ArrayDriver", "ArrayDriver"),
    "LogDriver": (".LogDriver", "LogDriver"),
    "MailgunDriver": (".MailgunDriver", "MailgunDriver"),
    "PERMANENT_SMTP_ERRORS": (".SmtpDriver", "PERMANENT_SMTP_ERRORS"),
    "SmtpDriver": (".SmtpDriver", "SmtpDriver"),
    "TRANSIENT_SMTP_ERRORS": (".SmtpDriver", "TRANSIENT_SMTP_ERRORS"),
}

__all__ = [
    "ArrayDriver",
    "LogDriver",
    "MailgunDriver",
    "PERMANENT_SMTP_ERRORS",
    "SmtpDriver",
    "TRANSIENT_SMTP_ERRORS",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
