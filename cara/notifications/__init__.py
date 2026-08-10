from .BaseNotification import BaseNotification
from .Notifiable import Notifiable
from .Notification import Notification
from .NotificationProvider import NotificationProvider

# ``UnsubscribeToken`` is deliberately NOT re-exported here. Exporting a
# name that is also a submodule is the barrel-shadowing trap
# ``tests/http/test_http_lazy_exports.py`` guards: the exported name starts
# handing back the MODULE for whoever imports in the unlucky order. Its two
# functions are imported from the defining module —
# ``from cara.notifications.UnsubscribeToken import mint`` — by both the
# framework's own MailChannel and every product verifier.
__all__ = [
    "BaseNotification",
    "Notifiable",
    "Notification",
    "NotificationProvider",
]
