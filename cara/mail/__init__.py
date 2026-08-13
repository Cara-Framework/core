"""Mail — layer barrel (generated, DOCTRINE §5.1)."""

from cara._LazyExports import _install_lazy_exports

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ArrayDriver": (".drivers", "ArrayDriver"),
    "LogDriver": (".drivers", "LogDriver"),
    "Mail": (".Mail", "Mail"),
    "MailContract": (".contracts", "MailContract"),
    "MailMessage": (".MailMessage", "MailMessage"),
    "MailPendingSend": (".MailPendingSend", "MailPendingSend"),
    "MailProvider": (".MailProvider", "MailProvider"),
    "Mailable": (".Mailable", "Mailable"),
    "MailgunDriver": (".drivers", "MailgunDriver"),
    "PERMANENT_SMTP_ERRORS": (".drivers", "PERMANENT_SMTP_ERRORS"),
    "SendMailableJob": (".jobs", "SendMailableJob"),
    "SmtpDriver": (".drivers", "SmtpDriver"),
    "TRANSIENT_SMTP_ERRORS": (".drivers", "TRANSIENT_SMTP_ERRORS"),
    "clear_cache": (".JinjaRenderer", "clear_cache"),
    "render_mail_view": (".JinjaRenderer", "render_mail_view"),
    "validate_custom_header": (".Mailable", "validate_custom_header"),
}

__all__ = [
    "ArrayDriver",
    "LogDriver",
    "Mail",
    "MailContract",
    "MailMessage",
    "MailPendingSend",
    "MailProvider",
    "Mailable",
    "MailgunDriver",
    "PERMANENT_SMTP_ERRORS",
    "SendMailableJob",
    "SmtpDriver",
    "TRANSIENT_SMTP_ERRORS",
    "clear_cache",
    "render_mail_view",
    "validate_custom_header",
]

_install_lazy_exports(__name__, _LAZY_EXPORTS)
