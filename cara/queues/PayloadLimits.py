"""Queue wire-size limits shared by producers and consumers."""

# Quorum queues replicate every byte to each member and the durable outbox
# stores the same signed envelope in PostgreSQL. Jobs therefore carry IDs and
# compact primitives, never documents/blobs.
MAX_AMQP_JOB_PAYLOAD_BYTES = 256 * 1024

__all__ = ["MAX_AMQP_JOB_PAYLOAD_BYTES"]
