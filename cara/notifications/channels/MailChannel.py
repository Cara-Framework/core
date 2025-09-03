"""
Mail Channel for Cara Notifications.

This module provides email notification channel functionality,
integrating with Cara's mail system.
"""

from typing import Optional

from cara.notifications.channels import BaseChannel


class MailChannel(BaseChannel):
    """
    Mail channel for sending notifications via email.

    This channel integrates with Cara's mail system to send email notifications.
    """

    channel_name = "mail"

    def __init__(
        self,
        mail_manager,
        from_address: str = None,
        from_name: str = None,
        reply_to: str = None,
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
                    message.view(mail_message["view"], mail_message.get("data", {}))

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
            # Log error (could integrate with Cara's logging)
            print(f"Mail channel error: {e}")
            return False

    def _get_recipient(self, notifiable, notification) -> Optional[str]:
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
