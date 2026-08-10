"""Framework-owned tables, declared as models.

Cara writes three tables of its own: the ``failed_job`` dead-letter table
(``cara.queues.drivers``), and the queue delivery ledger pair
(``cara.queues.delivery.QueueJobDeliveryStore``). For as long as these lived
only as hand-written migrations, every product had to carry a private copy of
their DDL under a ``MODEL_LESS`` escape marker — the single exception that kept
"the migration directory is a function of the models" from being literally
true.

Declaring them here closes that hole. The product's ``ModelDiscoverer`` scans
the framework symlink like any other source tree, finds these classes, and
``make:migration`` generates their create files exactly as it does for product
models. One rule, no exceptions: every table's schema lives in a model, every
migration file is generated.

These classes are SCHEMA DECLARATIONS first. The runtime write path stays in
the stores (raw SQL is at home in a store/repository); nothing in the
framework routes writes through the ORM layer of these classes, and products
must not either — the ledger's compare-and-set lease protocol cannot be
expressed as model saves.
"""

from cara.models.FailedJob import FailedJob
from cara.models.QueueJobDelivery import QueueJobDelivery
from cara.models.QueueJobDeliveryHookRetryAudit import QueueJobDeliveryHookRetryAudit

__all__ = [
    "FailedJob",
    "QueueJobDelivery",
    "QueueJobDeliveryHookRetryAudit",
]
