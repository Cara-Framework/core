"""
Mail Channel for Cara Notifications.

This module provides email notification channel functionality,
integrating with Cara's mail system.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

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

                # Apply default settings
                if self.from_address:
                    message.from_(self.from_address, self.from_name)
                if self.reply_to:
                    message.reply_to(self.reply_to)

                return message.send()

            # If mail_message is a dict, use it to build the email
            elif isinstance(mail_message, dict):
                message = self.mail_manager.to(recipient)

                if "subject" in mail_message:
                    message.subject(mail_message["subject"])

                if "text" in mail_message:
                    message.text(mail_message["text"])

                if "html" in mail_message:
                    message.html(mail_message["html"])

                if "view" in mail_message:
                    # Templates assume ``unsubscribe_url`` / ``preferences_url``
                    # / ``frontend_url`` exist in the render context;
                    # Jinja's ``{{ unsubscribe_url | default('#') }}``
                    # fallback in the stock notification templates
                    # paints a dead ``#`` link when the notification's
                    # ``to_mail()`` payload forgets to set them, which
                    # is a CAN-SPAM / GDPR liability (the law calls for
                    # a working unsubscribe affordance, not a no-op
                    # anchor). Enrich the data dict with sensible
                    # defaults derived from the framework config +
                    # notifiable before render. Existing keys win, so
                    # notifications that DO supply their own URLs are
                    # untouched.
                    view_data = dict(mail_message.get("data") or {})
                    self._inject_default_urls(view_data, notifiable)
                    message.view(mail_message["view"], view_data)

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

            Log.error(
                f"{message}: {error}",
                category="cara.notifications.mail",
                exc_info=True,
            )
        except ImportError:
            pass

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

        ``unsubscribe_url`` carries an HMAC token per
        ``(user_id, email)`` matching the existing
        ``EmailChannel.send`` token shape, so the unsubscribe handler
        can verify both delivery paths with the same secret. When the
        secret isn't configured we fall back to the user-facing
        preferences page rather than emitting a token-less link, so
        an unsubscribed user still has a working route.
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
            f"{frontend_url}/profile/preferences",
        )

        if "unsubscribe_url" in view_data and view_data["unsubscribe_url"]:
            return

        user_id = getattr(notifiable, "id", None)
        email = getattr(notifiable, "email", None) or getattr(
            notifiable,
            "email_address",
            None,
        )
        try:
            secret = config("app.unsubscribe_secret", "") or ""
        except Exception:
            secret = ""

        if user_id is not None and email and secret:
            token = hmac.new(
                secret.encode("utf-8"),
                f"{user_id}:{email}".encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            view_data["unsubscribe_url"] = (
                f"{frontend_url}/unsubscribe?user={user_id}&token={token}"
            )
        else:
            # No HMAC available — point at the manual preferences page
            # rather than a token-less ``/unsubscribe`` URL the handler
            # would reject. A real link beats a dead ``#`` either way.
            view_data["unsubscribe_url"] = f"{frontend_url}/profile/preferences#email"
