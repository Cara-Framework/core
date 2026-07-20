"""
Mail Channel for Cara Notifications.

This module provides email notification channel functionality,
integrating with Cara's mail system.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any
from urllib.parse import urlencode

from cara.notifications.channels.BaseChannel import BaseChannel


class MailChannel(BaseChannel):
    """
    Mail channel for sending notifications via email.

    This channel integrates with Cara's mail system to send email notifications.
    """

    channel_name = "mail"

    def __init__(
        self,
        mail_manager,
        from_address: str | None = None,
        from_name: str | None = None,
        reply_to: str | None = None,
    ):
        """
        Initialize mail channel.

        Args:
            mail_manager: Mail manager instance
            from_address: Default from email address
            from_name: Default from name
            reply_to: Default reply-to address
        """
        self.mail_manager = mail_manager
        self.from_address = from_address
        self.from_name = from_name
        self.reply_to = reply_to

    def send(self, notifiable, notification) -> bool:
        """
        Send the notification via mail.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Check if this is a wrapper notification (from Laravel-style routing)
            if hasattr(notification, "data") and hasattr(notification, "original"):
                # Use the wrapper's data directly
                mail_message = notification.data
            else:
                # Regular notification - try to get mail representation
                mail_message = None
                if hasattr(notification, "to_mail"):
                    mail_message = notification.to_mail(notifiable)

            if mail_message is None:
                return False

            # Get the recipient email - try from mail data first, then from notifiable
            recipient = mail_message.get("to") if isinstance(mail_message, dict) else None
            if not recipient:
                recipient = self._get_recipient(notifiable, notification)
                if not recipient:
                    return False

            # If mail_message is a string, create a simple email
            if isinstance(mail_message, str):
                message = self.mail_manager.to(recipient)
                message.subject(f"Notification: {notification.__class__.__name__}")
                message.text(mail_message)
                view_data: dict[str, Any] = {}
                self._inject_default_urls(view_data, notifiable)
                self._apply_headers(message, {}, view_data)

                # Apply default settings
                if self.from_address:
                    message.from_(self.from_address, self.from_name)
                if self.reply_to:
                    message.reply_to(self.reply_to)

                return message.send()

            # If mail_message is a dict, use it to build the email
            elif isinstance(mail_message, dict):
                message = self.mail_manager.to(recipient)
                view_data = dict(mail_message.get("data") or {})
                self._inject_default_urls(view_data, notifiable)

                if "subject" in mail_message:
                    message.subject(mail_message["subject"])

                if "text" in mail_message:
                    message.text(mail_message["text"])

                if "html" in mail_message:
                    message.html(mail_message["html"])

                if "view" in mail_message:
                    message.view(mail_message["view"], view_data)

                self._apply_headers(
                    message,
                    mail_message.get("headers") or {},
                    view_data,
                )

                # Use message from or fallback to channel defaults
                from_addr = mail_message.get("from", self.from_address)
                if from_addr:
                    message.from_(
                        from_addr, mail_message.get("from_name", self.from_name)
                    )

                reply_to_addr = mail_message.get("reply_to", self.reply_to)
                if reply_to_addr:
                    message.reply_to(reply_to_addr)

                if "attachments" in mail_message:
                    for attachment in mail_message["attachments"]:
                        message.attach(attachment["name"], attachment["path"])

                return message.send()

            # If mail_message has fluent API (like MailMessage)
            elif hasattr(mail_message, "to_dict"):
                message = self.mail_manager.to(recipient)
                mail_data = mail_message.to_dict()
                view_data = {}
                self._inject_default_urls(view_data, notifiable)
                self._apply_headers(
                    message,
                    mail_data.get("headers") or {},
                    view_data,
                )

                if mail_data.get("subject"):
                    message.subject(mail_data["subject"])

                # Build message content
                content_parts = []
                if mail_data.get("greeting"):
                    content_parts.append(mail_data["greeting"])

                content_parts.extend(mail_data.get("lines", []))

                if mail_data.get("salutation"):
                    content_parts.append(mail_data["salutation"])

                if content_parts:
                    message.text("\n\n".join(content_parts))

                # Apply settings
                from_addr = mail_data.get("from_address", self.from_address)
                from_name = mail_data.get("from_name", self.from_name)
                if from_addr:
                    message.from_(from_addr, from_name)

                reply_to_addr = mail_data.get("reply_to", self.reply_to)
                if reply_to_addr:
                    message.reply_to(reply_to_addr)

                for cc_addr in mail_data.get("cc", []):
                    message.cc(cc_addr)

                for bcc_addr in mail_data.get("bcc", []):
                    message.bcc(bcc_addr)

                for attachment in mail_data.get("attachments", []):
                    if "path" in attachment:
                        message.attach(attachment.get("name", ""), attachment["path"])

                return message.send()

            return False

        except Exception as e:
            self._emit_error("Mail channel error", e)
            return False

    def _emit_error(self, message: str, error: Exception) -> None:
        """Emit mail notification errors via Log facade with stderr fallback."""
        try:
            from cara.facades import Log

            Log.error("%s: %s", message, error, category='cara.notifications.mail', exc_info=True)
        except (ImportError, RuntimeError):
            import sys

            print(f"[MailChannel] {message}: {error}", file=sys.stderr)

    def _get_recipient(self, notifiable, notification) -> str | None:
        """
        Get the recipient email address.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            Email address or None
        """
        # Try to get routing information from notifiable
        if hasattr(notifiable, "route_notification_for"):
            email = notifiable.route_notification_for("mail")
            if email:
                return email

        # Fallback to common email attributes
        if hasattr(notifiable, "email"):
            return notifiable.email
        elif hasattr(notifiable, "email_address"):
            return notifiable.email_address

        return None

    def _inject_default_urls(self, view_data: dict[str, Any], notifiable: Any) -> None:
        """Stamp the mail-template render context with default URLs.

        Every link comes from host-application config — the framework owns no
        product route. Keys read:

        ``app.frontend_url``
            Brand root, framework-neutral.
        ``app.preferences_url``
            Human notification-preferences page.
        ``app.unsubscribe_confirm_url``
            Human unsubscribe confirmation PAGE, if the product ships one.
        ``app.unsubscribe_url``
            RFC 8058 one-click POST processor (machine endpoint); rendered as
            ``unsubscribe_one_click_url``.

        The human-visible ``unsubscribe_url`` is resolved best-first:

        1. a non-blank value the notification supplied in ``to_mail()['data']``;
        2. the signed confirmation page, when the product declares one;
        3. the signed processor itself — a UI-less processor that answers GET
           is a legitimate human destination, and a product whose opt-out is a
           single HMAC-gated endpoint (no confirmation page) relies on exactly
           this rather than shipping mail with no visible link at all;
        4. the preferences page, which needs no signature and carries the real
           opt-out controls.

        Signed links carry an HMAC over the opaque user identity and email; a
        link that cannot be signed is never emitted as a signed link. Because
        an unsubscribe affordance is a legal requirement, an unmintable link is
        reported to the operator rather than silently dropped: honest-null on
        this particular key renders as ``href="#"`` in the shipped mail, which
        is indistinguishable from a working opt-out during review.

        Blank means "not supplied": a present-but-empty value from a caller is
        replaced, not preserved, so ``{"unsubscribe_url": ""}`` cannot survive
        into the template. Other keys (``frontend_url``, ``preferences_url``)
        stay honest-null — unset config injects nothing and the templates carry
        their own ``default('#')``.
        """
        try:
            from cara.configuration import config
        except Exception:
            return

        def setting(key: str) -> str:
            try:
                return (config(key, "") or "").rstrip("/")
            except Exception:
                return ""

        def offer(key: str, value: str) -> None:
            """Fill a gap without overwriting a real caller-supplied value."""
            existing = view_data.get(key)
            if isinstance(existing, str) and existing.strip():
                return
            if value:
                view_data[key] = value

        offer("frontend_url", setting("app.frontend_url"))

        preferences_url = setting("app.preferences_url")
        offer("preferences_url", preferences_url)

        confirm_url = setting("app.unsubscribe_confirm_url")
        processor_url = setting("app.unsubscribe_url")

        user_public_id = getattr(notifiable, "public_id", None)
        email = getattr(notifiable, "email", None) or getattr(
            notifiable,
            "email_address",
            None,
        )
        try:
            secret = config("app.unsubscribe_secret", "") or ""
        except Exception:
            secret = ""

        query = ""
        if user_public_id and email and secret:
            token = hmac.new(
                secret.encode("utf-8"),
                f"{user_public_id}:{email}".encode(),
                hashlib.sha256,
            ).hexdigest()
            query = urlencode({"user": user_public_id, "token": token})
        elif confirm_url or processor_url:
            # The product declared an unsubscribe endpoint, so a link was meant
            # to be here and could not be minted. Reaching an operator matters
            # more than a tidy log: the reader falls back to the preferences
            # page below, which is a weaker opt-out than the one intended.
            missing = [
                name
                for name, present in (
                    ("app.unsubscribe_secret", secret),
                    ("notifiable.public_id", user_public_id),
                    ("notifiable.email", email),
                )
                if not present
            ]
            self._emit_error(
                "Unsubscribe link could not be signed; missing "
                + ", ".join(missing),
                RuntimeError("unsubscribe link unsignable"),
            )

        if query and confirm_url:
            composed = f"{confirm_url}?{query}"
        elif query and processor_url:
            composed = f"{processor_url}?{query}"
        else:
            composed = preferences_url
        offer("unsubscribe_url", composed)

        if not (query and processor_url):
            return  # No one-click endpoint to advertise.

        # RFC 8058 headers are built from this value, so it must never point
        # somewhere the reader cannot see. The framework's own confirm-page and
        # processor are two faces of one config-declared endpoint, so pairing
        # them is honest; but when the notification supplies its OWN visible
        # link, a mail client would otherwise opt the user out via a URL that
        # never appeared in the message.
        if view_data.get("unsubscribe_url") == composed:
            view_data["unsubscribe_one_click_url"] = f"{processor_url}?{query}"

    @staticmethod
    def _apply_headers(
        message: Any,
        explicit_headers: dict[str, str],
        view_data: dict[str, Any],
    ) -> None:
        """Apply caller headers plus RFC 8058 one-click unsubscribe metadata."""
        headers = dict(explicit_headers)
        one_click_url = view_data.get("unsubscribe_one_click_url")
        if one_click_url:
            headers.setdefault("List-Unsubscribe", f"<{one_click_url}>")
            headers.setdefault(
                "List-Unsubscribe-Post",
                "List-Unsubscribe=One-Click",
            )
        if headers:
            message.headers(headers)
