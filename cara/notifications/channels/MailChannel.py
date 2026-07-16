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

        Reads ``app.frontend_url`` / ``app.unsubscribe_secret`` from
        the framework config so the host application can centralise
        the values without every notification class repeating them.
        Honors any value the notification already supplied in
        ``to_mail()['data']`` — those wins; we only fill in the gaps.

        ``unsubscribe_url`` points to the human confirmation page, while
        ``unsubscribe_one_click_url`` points directly to the RFC 8058 POST
        processor. Both carry an HMAC over the opaque user identity and email.
        """
        try:
            from cara.configuration import config
        except Exception:
            return

        try:
            frontend_url = (config("app.frontend_url", "") or "").rstrip("/")
        except Exception:
            frontend_url = ""
        if not frontend_url:
            return  # No base URL configured — leave templates to their own defaults.

        view_data.setdefault("frontend_url", frontend_url)
        view_data.setdefault(
            "preferences_url",
            f"{frontend_url}/account#notifications",
        )

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

        if user_public_id and email and secret:
            token = hmac.new(
                secret.encode("utf-8"),
                f"{user_public_id}:{email}".encode(),
                hashlib.sha256,
            ).hexdigest()
            query = urlencode({"user": user_public_id, "token": token})
            view_data.setdefault(
                "unsubscribe_url",
                f"{frontend_url}/unsubscribe?{query}",
            )
            try:
                processor_url = (
                    config("app.unsubscribe_url", "") or ""
                ).rstrip("/")
            except Exception:
                processor_url = ""
            if processor_url:
                view_data["unsubscribe_one_click_url"] = (
                    f"{processor_url}?{query}"
                )
        else:
            view_data.setdefault(
                "unsubscribe_url",
                f"{frontend_url}/account#notifications",
            )

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
