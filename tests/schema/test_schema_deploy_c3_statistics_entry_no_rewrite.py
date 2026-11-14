"""The ledger's statistics entry must not rewrite the outbox on production's PostgreSQL.

Found 2026-11-14 (overnight schema-deploy c3) running the EVOLVE checks on a
private PostgreSQL 17 (production pins ``postgres:17.10``; Cloud SQL is
planned on 17; every local lane runs 18). ``QueueJobDelivery.__indexes__``
entry ``queue_job_delivery_queue_state_statistics`` clears the sample of
``status`` / ``publish_status`` with ``pg_clear_attribute_stats`` on 18 and,
"older servers", by re-asserting each column's own type, which its comment
calls "no rewrite, same type". On 17 that ``ALTER COLUMN ... TYPE`` is not
metadata-only: on the 100k copy (``queue_job_delivery`` 103,537 rows, 165 MB)
it took 1.5 s under ACCESS EXCLUSIVE, gave the heap a new relfilenode and
rebuilt all 21 indexes. The ledger is the outbox every job dispatch, relay
claim and webhook acceptance writes; the handbook sizes it at ~600 GB at the
1k tier. Whenever the entry derives on a populated ledger (its target reset
during an incident, a restore that dropped it), the plan's one "locking"
line on a hot table — which deploy.sh asks the operator to approve as a lock
window — is a full table and index rewrite that stops the outbox for as
long as it takes. The PostgreSQL 18 branch is the only one the local lanes
run, so nothing here exercised the branch production takes.

Run against PostgreSQL 17 to see it red (17.9 on :5517 here); on 18 the
first branch is taken and it passes.
"""

from __future__ import annotations

import os

import pytest

from cara.models.QueueJobDelivery import QueueJobDelivery

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

_ENTRY = "queue_job_delivery_queue_state_statistics"


def _nodes(cursor) -> dict[str, int]:
    cursor.execute(
        "SELECT c.relname, c.relfilenode FROM pg_class c "
        "WHERE c.oid = 'pg_temp.queue_job_delivery'::regclass "
        "OR c.oid IN (SELECT indexrelid FROM pg_index "
        "WHERE indrelid = 'pg_temp.queue_job_delivery'::regclass)"
    )
    return {name: node for name, node in cursor.fetchall()}


def test_clearing_the_state_sample_leaves_the_ledger_and_its_indexes_in_place():
    import psycopg2

    (entry,) = [e for e in QueueJobDelivery.__indexes__ if e["name"] == _ENTRY]
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version_num")
            version = int(cursor.fetchone()[0])
            cursor.execute(
                "CREATE TEMP TABLE queue_job_delivery "
                "(LIKE public.queue_job_delivery INCLUDING ALL)"
            )
            cursor.execute(
                "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
                "signed_envelope, tenant_mode, queue, priority, status, "
                "completed_at, expires_at, available_at, publish_status, "
                "publish_retry_at, published_at, created_at, updated_at) "
                "SELECT 'c3-' || g, g, repeat('a', 64), '{}'::jsonb, 'central', "
                "'sync', 'default', 'completed', NOW(), NOW() + INTERVAL '1 day', "
                "NOW(), 'published', NOW(), NOW(), NOW(), NOW() "
                "FROM generate_series(1, 2000) g"
            )
            cursor.execute("ANALYZE queue_job_delivery")
            before = _nodes(cursor)
            assert len(before) > 10, before

            cursor.execute(entry["up"])

            after = _nodes(cursor)
            rewritten = sorted(name for name in before if after.get(name) != before[name])
            assert rewritten == [], (
                f"on server {version} the statistics entry rewrote "
                f"{len(rewritten)} of {len(before)} ledger relations: {rewritten}"
            )
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg_temp.queue_job_delivery")
        connection.close()
