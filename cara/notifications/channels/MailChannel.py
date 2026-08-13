"""
Mail Channel for Cara Notifications.

This module provides email notification channel functionality,
integrating with Cara's mail system.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from cara.exceptions import ConfigurationException, InvalidArgumentException
from cara.notifications.channels.BaseChannel import BaseChannel
from cara.notifications.UnsubscribeToken import mint as mint_unsubscribe_token


class MailChannel(BaseChannel):
    """
    Mail channel for sending notifications via email.

    This channel integrates with Cara's mail system to send email notifications.
    """

    channel_name = "mail"
    _LINK_SETTINGS = frozenset(
        {
            "app.frontend_url",
            "app.preferences_url",
            "app.unsubscribe_confirm_url",
            "app.unsubscribe_secret",
            "app.unsubscribe_url",
        }
    )

    def __init__(
        self,
        mail_manager,
        from_address: str | None = None,
        from_name: str | None = None,
        reply_to: str | None = None,
        *,
        link_settings: dict[str, str | None],
    ):
        """
        Initialize mail channel.

        Args:
            mail_manager: Mail manager instance
            from_address: Default from email address
            from_name: Default from name
            reply_to: Default reply-to address
        """
        if not callable(getattr(mail_manager, "to", None)):
            raise ConfigurationException("MailChannel requires a mail manager with to().")
        self.mail_manager = mail_manager
        self.from_address = self._optional_text(from_address, "from_address")
        self.from_name = self._optional_text(from_name, "from_name")
        self.reply_to = self._optional_text(reply_to, "reply_to")
        if not isinstance(link_settings, dict):
            raise ConfigurationException(
                "MailChannel link_settings must be a dictionary."
            )
        unknown = set(link_settings) - self._LINK_SETTINGS
        if unknown:
            raise ConfigurationException(
                "Unknown MailChannel link settings: " + ", ".join(sorted(unknown))
            )
        self.link_settings: dict[str, str] = {}
        for key, value in link_settings.items():
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ConfigurationException(
                    f"MailChannel setting {key!r} must be a non-empty string or None."
                )
            self.link_settings[key] = value.strip()

    def send(self, notifiable, notification) -> bool:
        """
        Send the notification via mail.

        Args:
            notifiable: The notifiable entity
            notification: The notification instance

        Returns:
            True if sent successfully, False otherwise
        """
        renderer = getattr(notification, "to_mail", None)
        if not callable(renderer):
            raise InvalidArgumentException("Mail notifications must implement to_mail().")
        mail_message = renderer(notifiable)
        if mail_message is None:
            raise InvalidArgumentException(
                "A notification routed to mail must render a mail payload."
            )

        recipient = mail_message.get("to") if isinstance(mail_message, dict) else None
        recipient = recipient or self._get_recipient(notifiable, notification)
        recipient = self._required_text(recipient, "recipient")

        if isinstance(mail_message, str):
            message = self.mail_manager.to(recipient)
            message.subject(f"Notification: {type(notification).__name__}")
            message.text(self._required_text(mail_message, "text"))
            view_data: dict[str, Any] = {}
            self._inject_default_urls(view_data, notifiable)
            self._apply_headers(message, {}, view_data)
            if self.from_address:
                message.from_(self.from_address, self.from_name)
            if self.reply_to:
                message.reply_to(self.reply_to)
            return self._delivery_result(message.send())

        if isinstance(mail_message, dict):
            message = self.mail_manager.to(recipient)
            raw_view_data = mail_message.get("data", {})
            if not isinstance(raw_view_data, dict):
                raise InvalidArgumentException(
                    "Mail notification data must be a dictionary."
                )
            view_data = dict(raw_view_data)
            self._inject_default_urls(view_data, notifiable)

            for key, method in (
                ("subject", message.subject),
                ("text", message.text),
                ("html", message.html),
            ):
                if key in mail_message:
                    method(self._required_text(mail_message[key], key))
            if "view" in mail_message:
                message.view(self._required_text(mail_message["view"], "view"), view_data)

            headers = mail_message.get("headers", {})
            if not isinstance(headers, dict) or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in headers.items()
            ):
                raise InvalidArgumentException(
                    "Mail notification headers must be a string dictionary."
                )
            self._apply_headers(message, headers, view_data)

            from_addr = mail_message.get("from", self.from_address)
            if from_addr is not None:
                message.from_(
                    self._required_text(from_addr, "from"),
                    self._optional_text(
                        mail_message.get("from_name", self.from_name), "from_name"
                    ),
                )
            reply_to_addr = mail_message.get("reply_to", self.reply_to)
            if reply_to_addr is not None:
                message.reply_to(self._required_text(reply_to_addr, "reply_to"))
            self._attach_all(message, mail_message.get("attachments", []))
            return self._delivery_result(message.send())

        to_dict = getattr(mail_message, "to_dict", None)
        if callable(to_dict):
            mail_data = to_dict()
            if not isinstance(mail_data, dict):
                raise InvalidArgumentException(
                    "MailMessage.to_dict() must return a dictionary."
                )
            message = self.mail_manager.to(recipient)
            view_data = {}
            self._inject_default_urls(view_data, notifiable)
            headers = mail_data.get("headers", {})
            if not isinstance(headers, dict):
                raise InvalidArgumentException("Mail headers must be a dictionary.")
            self._apply_headers(message, headers, view_data)

            if mail_data.get("subject") is not None:
                message.subject(self._required_text(mail_data["subject"], "subject"))
            content_parts = [
                self._required_text(value, "mail line")
                for value in (
                    [mail_data["greeting"]] if mail_data.get("greeting") else []
                )
                + list(mail_data.get("lines", []))
                + ([mail_data["salutation"]] if mail_data.get("salutation") else [])
            ]
            if content_parts:
                message.text("\n\n".join(content_parts))

            from_addr = mail_data.get("from_address", self.from_address)
            if from_addr is not None:
                message.from_(
                    self._required_text(from_addr, "from_address"),
                    self._optional_text(
                        mail_data.get("from_name", self.from_name), "from_name"
                    ),
                )
            reply_to_addr = mail_data.get("reply_to", self.reply_to)
            if reply_to_addr is not None:
                message.reply_to(self._required_text(reply_to_addr, "reply_to"))
            for field, method in (("cc", message.cc), ("bcc", message.bcc)):
                values = mail_data.get(field, [])
                if not isinstance(values, list):
                    raise InvalidArgumentException(f"Mail {field} must be a list.")
                for value in values:
                    method(self._required_text(value, field))
            self._attach_all(message, mail_data.get("attachments", []))
            return self._delivery_result(message.send())

        raise InvalidArgumentException(
            "Mail notifications must render a string, dictionary, or MailMessage."
        )

    @staticmethod
    def _delivery_result(result: Any) -> bool:
        if not isinstance(result, bool):
            raise InvalidArgumentException("Mail delivery must return a boolean result.")
        return result

    @classmethod
    def _attach_all(cls, message: Any, attachments: Any) -> None:
        if not isinstance(attachments, list):
            raise InvalidArgumentException("Mail attachments must be a list.")
        for attachment in attachments:
            if not isinstance(attachment, dict) or set(attachment) != {"name", "path"}:
                raise InvalidArgumentException(
                    "Mail attachments require exactly name and path."
                )
            message.attach(
                cls._required_text(attachment["name"], "attachment name"),
                cls._required_text(attachment["path"], "attachment path"),
            )

    @staticmethod
    def _required_text(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise InvalidArgumentException(f"Mail {field} must be a non-empty string.")
        return value.strip()

    @classmethod
    def _optional_text(cls, value: Any, field: str) -> str | None:
        if value is None:
            return None
        return cls._required_text(value, field)

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

        def setting(key: str) -> str:
            return self.link_settings.get(key, "").rstrip("/")

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
        secret = self.link_settings.get("app.unsubscribe_secret", "")

        query = ""
        if user_public_id and email and secret:
            # One frozen wire format, shared with whatever verifies the click
            # — see cara.notifications.UnsubscribeToken.
            token = mint_unsubscribe_token(user_public_id, email, secret)
            query = urlencode({"user": user_public_id, "token": token})
        elif confirm_url or processor_url:
            missing = [
                name
                for name, present in (
                    ("app.unsubscribe_secret", secret),
                    ("notifiable.public_id", user_public_id),
                    ("notifiable.email", email),
                )
                if not present
            ]
            raise ConfigurationException(
                "Unsubscribe link could not be signed; missing " + ", ".join(missing)
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
