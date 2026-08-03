"""Postgres access: schema, writes from the webhook, and dashboard aggregates."""
from __future__ import annotations
import json
from contextlib import contextmanager

from psycopg import connect
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config

_pool: ConnectionPool | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS resolve_events (
    id              BIGSERIAL PRIMARY KEY,
    conversation_id TEXT        NOT NULL,
    resolved_at     TIMESTAMPTZ NOT NULL,
    conv_created_at TIMESTAMPTZ,
    resolution_secs INTEGER,
    agent_id        TEXT,
    agent_name      TEXT,
    issue_type      TEXT,
    priority        TEXT,
    order_id        TEXT,
    store           TEXT,
    fulfillment     TEXT,
    channel_id      TEXT,
    group_id        TEXT,
    -- LLM review of the transcript (NULL when no key configured / call failed)
    llm_issue       TEXT,
    llm_resolution  TEXT,
    llm_sentiment   TEXT,
    llm_escalated   BOOLEAN,
    llm_root_cause  TEXT,
    llm_summary     TEXT,
    raw             JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    enriched_at     TIMESTAMPTZ,
    enrich_attempts INTEGER NOT NULL DEFAULT 0,
    enrich_error    TEXT,
    -- A conversation can be reopened and resolved again; each resolve is its own row.
    UNIQUE (conversation_id, resolved_at)
);
CREATE INDEX IF NOT EXISTS idx_resolve_resolved_at ON resolve_events (resolved_at DESC);
CREATE INDEX IF NOT EXISTS idx_resolve_pending     ON resolve_events (enriched_at) WHERE enriched_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_resolve_issue       ON resolve_events (issue_type);

-- Cache of order_id -> (store, fulfillment) so we hit Metabase once per order.
CREATE TABLE IF NOT EXISTS order_dim (
    order_id    TEXT PRIMARY KEY,
    store       TEXT,
    fulfillment TEXT,
    n_batches   INTEGER,
    is_cross_store BOOLEAN,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Additive migration. Every column is nullable, so this is safe to re-run and
-- safe against rows written before the column existed.
ALTER TABLE resolve_events
    ADD COLUMN IF NOT EXISTS conversation_url        TEXT,
    -- Salted digest, not a phone number or name. Enough to count distinct
    -- customers; useless to anyone who obtains the database.
    ADD COLUMN IF NOT EXISTS customer_ref            TEXT,
    -- Who actually clicked Resolve. Differs from agent_id (the ASSIGNED agent)
    -- whenever a chat was transferred -- crediting the wrong person otherwise.
    ADD COLUMN IF NOT EXISTS resolver_agent_id       TEXT,
    ADD COLUMN IF NOT EXISTS resolver_agent_name     TEXT,
    ADD COLUMN IF NOT EXISTS first_response_secs     INTEGER,
    ADD COLUMN IF NOT EXISTS msg_total               INTEGER,
    ADD COLUMN IF NOT EXISTS msg_user                INTEGER,
    ADD COLUMN IF NOT EXISTS msg_agent               INTEGER,
    ADD COLUMN IF NOT EXISTS msg_bot                 INTEGER,
    ADD COLUMN IF NOT EXISTS sla_response_breached   BOOLEAN,
    ADD COLUMN IF NOT EXISTS sla_resolution_breached BOOLEAN,
    ADD COLUMN IF NOT EXISTS n_batches               INTEGER,
    ADD COLUMN IF NOT EXISTS is_cross_store          BOOLEAN,
    -- 1 = first time this conversation was resolved, 2+ = it was reopened and
    -- resolved again. Derived at insert, so no reopen webhook is required.
    ADD COLUMN IF NOT EXISTS resolve_seq             INTEGER,

    -- Counted, not judged: everything below is derived from the transcript in
    -- code, so it costs nothing and never varies between runs.
    ADD COLUMN IF NOT EXISTS msg_private                 INTEGER,
    ADD COLUMN IF NOT EXISTS max_dead_air_secs           INTEGER,
    ADD COLUMN IF NOT EXISTS handoffs                    INTEGER,
    ADD COLUMN IF NOT EXISTS agent_inactive              BOOLEAN,
    ADD COLUMN IF NOT EXISTS repeated_canned             INTEGER,
    ADD COLUMN IF NOT EXISTS closed_without_confirmation BOOLEAN,
    ADD COLUMN IF NOT EXISTS personalised                BOOLEAN,

    -- Judged by the model from the conversation alone.
    ADD COLUMN IF NOT EXISTS llm_sentiment_start      TEXT,
    ADD COLUMN IF NOT EXISTS llm_answered_ask         TEXT,
    ADD COLUMN IF NOT EXISTS llm_commitment           TEXT,
    ADD COLUMN IF NOT EXISTS llm_empathy              TEXT,
    ADD COLUMN IF NOT EXISTS llm_proactive            TEXT,
    ADD COLUMN IF NOT EXISTS llm_escalation_risk      BOOLEAN,
    ADD COLUMN IF NOT EXISTS llm_escalation_handled   BOOLEAN;

ALTER TABLE order_dim
    ADD COLUMN IF NOT EXISTS n_batches      INTEGER,
    ADD COLUMN IF NOT EXISTS is_cross_store BOOLEAN;

-- Drop the contact details we briefly stored before switching to a digest.
ALTER TABLE resolve_events
    DROP COLUMN IF EXISTS customer_phone,
    DROP COLUMN IF EXISTS customer_name,
    DROP COLUMN IF EXISTS user_id;

CREATE INDEX IF NOT EXISTS idx_resolve_customer ON resolve_events (customer_ref);
CREATE INDEX IF NOT EXISTS idx_resolve_order    ON resolve_events (order_id);
CREATE INDEX IF NOT EXISTS idx_resolve_llm      ON resolve_events (llm_issue);

-- Agent sign-in/sign-out, from the agent_availability webhooks Freshchat was
-- already sending and we were discarding. This is the only real attendance
-- record in the system; everything else is inferred from resolve activity.
--
-- `raw` is stored on every row because the payload shape is undocumented. If the
-- extracted columns turn out to be wrong, they can be rebuilt from raw instead of
-- the events being lost.
CREATE TABLE IF NOT EXISTS agent_availability_events (
    id          BIGSERIAL PRIMARY KEY,
    agent_id    TEXT,
    -- normalised available / unavailable / unknown
    status      TEXT        NOT NULL,
    -- exactly what Freshchat sent, for when the normaliser needs correcting
    raw_status  TEXT,
    -- when the change happened per the payload; NULL if it did not say
    changed_at  TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    action      TEXT,
    raw         JSONB       NOT NULL,
    -- Freshchat retries. A replay must collapse onto the row it already wrote
    -- rather than becoming a second sign-in.
    dedupe_key  TEXT        NOT NULL UNIQUE
);
-- Freshchat tracks login separately from availability, and agents do sit logged
-- in while UNAVAILABLE. Captured so "at work" can eventually be told apart from
-- "ready for chats". NULL means the payload did not say, which is not the same
-- as logged out.
ALTER TABLE agent_availability_events
    ADD COLUMN IF NOT EXISTS login_status BOOLEAN;

-- Every message an agent sent, from the Extract API's Message-Sent event.
--
-- This is the attendance signal that actually works. Resolves were a bad proxy:
-- on 1 Aug it reported Ansila Lissie as 0.0 hours and Sumayya A as absent, while
-- they had sent 125 and 113 messages across full shifts. An agent texts all
-- through a shift even on chats a colleague closes.
--
-- One row per message, ~900/day, so a month is ~28k rows -- small enough to keep
-- raw rather than pre-aggregating, which keeps the hour-wise view exact.
CREATE TABLE IF NOT EXISTS agent_message_events (
    id              BIGSERIAL PRIMARY KEY,
    agent_id        TEXT        NOT NULL,
    agent_name      TEXT,
    conversation_id TEXT,
    sent_at         TIMESTAMPTZ NOT NULL,
    source          TEXT        NOT NULL,
    -- Message-Sent carries no message id (interaction_id is per interaction, not
    -- per message), so the key is derived from the message's own content. Makes
    -- re-running an overlapping ingest a no-op instead of double counting.
    dedupe_key      TEXT        NOT NULL UNIQUE,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_amsg_agent_time ON agent_message_events (agent_id, sent_at);
CREATE INDEX IF NOT EXISTS idx_amsg_sent       ON agent_message_events (sent_at DESC);

-- Ticket inflow. The webhook never sends conversation_create -- the by_action
-- tally has only message_create, assignment, resolution, reopen, waiting and
-- availability -- so tickets RAISED are invisible in real time and have to come
-- from the Extract API's Conversation-Created event.
CREATE TABLE IF NOT EXISTS ticket_created_events (
    conversation_id TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL,
    user_id         TEXT,
    channel_name    TEXT,
    reopened        BOOLEAN,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tcreated_at ON ticket_created_events (created_at DESC);
-- 'extract' = hourly batch (authoritative), 'webhook' = spotted live the moment
-- any event mentioned an unknown conversation, with created_at read from the
-- conversation itself so the ticket is filed on the day it was really raised.
ALTER TABLE ticket_created_events
    ADD COLUMN IF NOT EXISTS source TEXT;
CREATE INDEX IF NOT EXISTS idx_avail_agent_time
    ON agent_availability_events (agent_id, COALESCE(changed_at, received_at));
CREATE INDEX IF NOT EXISTS idx_avail_received
    ON agent_availability_events (received_at DESC);
"""


def init() -> None:
    """Open the pool and apply the schema.

    ``min_size=0`` matters on free-tier Postgres: holding a connection open 24/7
    keeps the database permanently "active", which burns metered compute hours
    (Neon) or a scarce connection slot (Supabase). With min_size 0 the pool
    drops idle connections and lets the database go quiet between bursts --
    our load is ~150 webhooks/day, so it is idle most of the time.
    """
    global _pool
    if not config.DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    _pool = ConnectionPool(
        config.DATABASE_URL,
        min_size=0,
        max_size=config.DB_MAX_CONNECTIONS,
        max_idle=60,  # seconds before an idle connection is closed
        kwargs={"row_factory": dict_row},
    )
    with _pool.connection() as conn:
        conn.execute(SCHEMA)


@contextmanager
def cursor():
    assert _pool is not None, "db.init() not called"
    with _pool.connection() as conn:
        with conn.cursor() as cur:
            yield cur


def insert_event(ev: dict) -> int | None:
    """Insert a resolve event. Returns row id, or None if it was a duplicate.

    ``resolve_seq`` counts prior resolves of the same conversation, so a value
    of 2+ means the ticket was reopened and closed again.
    """
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO resolve_events
                (conversation_id, resolved_at, raw, resolver_agent_id, resolve_seq)
            VALUES (%(conversation_id)s, %(resolved_at)s, %(raw)s, %(resolver_agent_id)s,
                    1 + (SELECT count(*) FROM resolve_events e
                          WHERE e.conversation_id = %(conversation_id)s))
            ON CONFLICT (conversation_id, resolved_at) DO NOTHING
            RETURNING id
            """,
            {
                "conversation_id": ev["conversation_id"],
                "resolved_at": ev["resolved_at"],
                "resolver_agent_id": ev.get("resolver_agent_id"),
                "raw": json.dumps(ev.get("raw") or {}),
            },
        )
        row = cur.fetchone()
        return row["id"] if row else None


def insert_availability(ev: dict) -> int | None:
    """Store an agent availability change. Returns row id, or None on a replay."""
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_availability_events
                (agent_id, status, raw_status, login_status, changed_at, action,
                 raw, dedupe_key)
            VALUES (%(agent_id)s, %(status)s, %(raw_status)s, %(login_status)s,
                    %(changed_at)s, %(action)s, %(raw)s, %(dedupe_key)s)
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING id
            """,
            {
                "agent_id": ev.get("agent_id"),
                "status": ev["status"],
                "raw_status": ev.get("raw_status"),
                "login_status": ev.get("login_status"),
                "changed_at": ev.get("changed_at"),
                "action": ev.get("action"),
                "raw": json.dumps(ev.get("raw") or {}),
                "dedupe_key": ev["dedupe_key"],
            },
        )
        row = cur.fetchone()
        return row["id"] if row else None


def availability_events(since_days: int) -> list[dict]:
    """Availability events oldest-first, so sessions can be paired in order.

    Ordered on COALESCE(changed_at, received_at): if the payload does not carry
    its own timestamp, arrival order is the best available proxy for sequence.
    """
    with cursor() as cur:
        cur.execute(
            """
            SELECT agent_id, status, raw_status, action,
                   COALESCE(changed_at, received_at) AS effective_at
            FROM agent_availability_events
            WHERE COALESCE(changed_at, received_at) >= now() - make_interval(days => %(d)s)
              AND agent_id IS NOT NULL
            ORDER BY agent_id, effective_at
            """,
            {"d": since_days},
        )
        return cur.fetchall()


def availability_samples(limit: int = 20) -> list[dict]:
    """Most recent raw payloads, for working out the undocumented shape."""
    with cursor() as cur:
        cur.execute(
            """
            SELECT id, agent_id, status, raw_status, action, changed_at, received_at, raw
            FROM agent_availability_events
            ORDER BY received_at DESC
            LIMIT %(n)s
            """,
            {"n": limit},
        )
        return cur.fetchall()


_SHIFT_DAY_MSG = "((sent_at AT TIME ZONE %(tz)s) - make_interval(hours => %(shift_hour)s))::date"


def insert_agent_messages(rows: list[dict]) -> int:
    """Bulk insert agent messages. Returns how many were new.

    ON CONFLICT DO NOTHING on dedupe_key, so an ingest window that overlaps a
    previous run costs nothing -- which is what makes the hourly catch-up safe
    to run with deliberate overlap.
    """
    if not rows:
        return 0
    with cursor() as cur:
        cur.executemany(
            """
            INSERT INTO agent_message_events
                (agent_id, agent_name, conversation_id, sent_at, source, dedupe_key)
            VALUES (%(agent_id)s, %(agent_name)s, %(conversation_id)s, %(sent_at)s,
                    %(source)s, %(dedupe_key)s)
            ON CONFLICT (dedupe_key) DO NOTHING
            """,
            rows,
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


def message_activity(days: int, tz: str, shift_hour: int) -> list[dict]:
    """Per agent per shift day: first message, last message, span, count."""
    with cursor() as cur:
        cur.execute(
            f"""
            SELECT agent_id,
                   MAX(agent_name)                        AS agent_name,
                   {_SHIFT_DAY_MSG}                       AS shift_day,
                   MIN(sent_at)                           AS first_at,
                   MAX(sent_at)                           AS last_at,
                   COUNT(*)                               AS messages,
                   COUNT(DISTINCT conversation_id)         AS conversations
            FROM agent_message_events
            WHERE {_SHIFT_DAY_MSG} > (
                    (now() AT TIME ZONE %(tz)s - make_interval(hours => %(shift_hour)s))::date
                    - make_interval(days => %(days)s))
            GROUP BY agent_id, 3
            ORDER BY 3 DESC, 6 DESC
            """,
            {"tz": tz, "shift_hour": shift_hour, "days": days},
        )
        return cur.fetchall()


def message_hourly(days: int, tz: str, shift_hour: int) -> list[dict]:
    """Per agent per shift day per local hour: message count.

    Local hour, not UTC: a shift-coverage view is only meaningful in the hours
    the team actually works.
    """
    with cursor() as cur:
        cur.execute(
            f"""
            SELECT agent_id,
                   {_SHIFT_DAY_MSG}                                        AS shift_day,
                   EXTRACT(HOUR FROM (sent_at AT TIME ZONE %(tz)s))::int    AS hour,
                   COUNT(*)                                                AS messages
            FROM agent_message_events
            WHERE {_SHIFT_DAY_MSG} > (
                    (now() AT TIME ZONE %(tz)s - make_interval(hours => %(shift_hour)s))::date
                    - make_interval(days => %(days)s))
            GROUP BY agent_id, 2, 3
            ORDER BY 2 DESC, 3
            """,
            {"tz": tz, "shift_hour": shift_hour, "days": days},
        )
        return cur.fetchall()


def insert_tickets_created(rows: list[dict]) -> int:
    """Bulk insert created tickets. Keyed on conversation_id, so a re-ingest of an
    overlapping window is a no-op."""
    if not rows:
        return 0
    with cursor() as cur:
        cur.executemany(
            """
            INSERT INTO ticket_created_events
                (conversation_id, created_at, user_id, channel_name, reopened, source)
            VALUES (%(conversation_id)s, %(created_at)s, %(user_id)s, %(channel_name)s,
                    %(reopened)s, 'extract')
            ON CONFLICT (conversation_id) DO NOTHING
            """,
            rows,
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


def ticket_seen_live(conversation_id: str, created_at, channel_name=None,
                     user_id=None) -> bool:
    """Record a ticket spotted live from the webhook stream. True if it was new."""
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO ticket_created_events
                (conversation_id, created_at, user_id, channel_name, reopened, source)
            VALUES (%(cid)s, %(created_at)s, %(user_id)s, %(channel)s, false, 'webhook')
            ON CONFLICT (conversation_id) DO NOTHING
            RETURNING conversation_id
            """,
            {"cid": conversation_id, "created_at": created_at,
             "user_id": user_id, "channel": channel_name},
        )
        return cur.fetchone() is not None


def ticket_known(conversation_id: str) -> bool:
    with cursor() as cur:
        cur.execute("SELECT 1 FROM ticket_created_events WHERE conversation_id = %s",
                    (conversation_id,))
        return cur.fetchone() is not None


def ticket_flow(days: int, tz: str) -> list[dict]:
    """Per local day: tickets raised, tickets resolved, and how many of the
    tickets raised that day were themselves resolved.

    Resolved-same-day is counted by joining on conversation_id rather than by
    comparing daily totals, because a ticket raised late at night is often
    resolved the next morning and subtracting totals would hide that.
    """
    with cursor() as cur:
        cur.execute(
            """
            WITH created AS (
                SELECT conversation_id,
                       (created_at AT TIME ZONE %(tz)s)::date AS d
                FROM ticket_created_events
                WHERE created_at >= now() - make_interval(days => %(days)s)
            ),
            solved AS (
                SELECT DISTINCT conversation_id FROM resolve_events
            ),
            resolved_by_day AS (
                SELECT (resolved_at AT TIME ZONE %(tz)s)::date AS d, COUNT(*) AS n
                FROM resolve_events
                WHERE resolved_at >= now() - make_interval(days => %(days)s)
                GROUP BY 1
            )
            SELECT c.d::text                                      AS bucket,
                   COUNT(*)                                       AS raised,
                   COUNT(s.conversation_id)                       AS raised_and_solved,
                   COALESCE(MAX(r.n), 0)                          AS resolved_that_day
            FROM created c
            LEFT JOIN solved s ON s.conversation_id = c.conversation_id
            LEFT JOIN resolved_by_day r ON r.d = c.d
            GROUP BY c.d
            ORDER BY c.d DESC
            """,
            {"tz": tz, "days": days},
        )
        return cur.fetchall()


def ticket_flow_stats() -> dict:
    with cursor() as cur:
        cur.execute("""SELECT COUNT(*) AS tickets, MIN(created_at) AS oldest,
                              MAX(created_at) AS newest, MAX(ingested_at) AS last_ingest
                       FROM ticket_created_events""")
        return cur.fetchone() or {}


def message_stats() -> dict:
    with cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS messages,
                   COUNT(DISTINCT agent_id) AS agents,
                   MIN(sent_at) AS oldest,
                   MAX(sent_at) AS newest,
                   MAX(ingested_at) AS last_ingest
            FROM agent_message_events
            """
        )
        return cur.fetchone() or {}


def availability_stats() -> dict:
    """Coverage summary: is capture actually working, and since when?"""
    with cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS events,
                   COUNT(DISTINCT agent_id) AS agents,
                   COUNT(*) FILTER (WHERE status = 'unknown')   AS unknown_status,
                   COUNT(*) FILTER (WHERE agent_id IS NULL)     AS no_agent_id,
                   COUNT(*) FILTER (WHERE changed_at IS NULL)   AS no_timestamp,
                   -- If this stays 0, these webhooks do not carry login state and
                   -- "at work" cannot be separated from "ready for chats".
                   COUNT(login_status)                          AS with_login_status,
                   MIN(received_at) AS first_at,
                   MAX(received_at) AS last_at
            FROM agent_availability_events
            """
        )
        return cur.fetchone() or {}


def update_enrichment(row_id: int, fields: dict, error: str | None = None) -> None:
    sets = ", ".join(f"{k} = %({k})s" for k in fields)
    with cursor() as cur:
        cur.execute(
            f"""
            UPDATE resolve_events
               SET {sets + ',' if sets else ''}
                   -- The ::text casts are required: on the failure path `fields`
                   -- is empty, so %(err)s is the only parameter and Postgres
                   -- cannot infer its type ("could not determine data type of
                   -- parameter $1"). That made the error handler itself throw,
                   -- which silently left rows unenriched with no error recorded.
                   enriched_at = CASE WHEN %(err)s::text IS NULL THEN now() ELSE NULL END,
                   enrich_attempts = enrich_attempts + 1,
                   enrich_error = %(err)s::text
             WHERE id = %(row_id)s
            """,
            {**fields, "row_id": row_id, "err": error},
        )


def pending_enrichment(limit: int = 25) -> list[dict]:
    with cursor() as cur:
        cur.execute(
            """
            SELECT id, conversation_id, raw FROM resolve_events
             WHERE enriched_at IS NULL AND enrich_attempts < 6
             ORDER BY received_at
             LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def get_order_dim(order_id: str) -> dict | None:
    with cursor() as cur:
        cur.execute(
            "SELECT store, fulfillment, n_batches, is_cross_store FROM order_dim WHERE order_id = %s",
            (order_id,),
        )
        return cur.fetchone()


def put_order_dim(order_id: str, dim: dict) -> None:
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO order_dim (order_id, store, fulfillment, n_batches, is_cross_store)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE
              SET store = EXCLUDED.store, fulfillment = EXCLUDED.fulfillment,
                  n_batches = EXCLUDED.n_batches, is_cross_store = EXCLUDED.is_cross_store,
                  fetched_at = now()
            """,
            (order_id, dim.get("store"), dim.get("fulfillment"),
             dim.get("n_batches"), dim.get("is_cross_store")),
        )


# --------------------------------------------------------------------------
# Aggregates
# --------------------------------------------------------------------------

ISSUE_COLUMNS = {"issue_type", "llm_issue"}


def _filter_sql(issue_column: str = "issue_type") -> str:
    """WHERE clause shared by every aggregate.

    Every optional parameter is cast to ::text. Without the cast, a filter left
    as NULL is the only thing Postgres has to infer a type from, and the query
    fails with "could not determine data type of parameter $N".

    ``issue_column`` makes the issue filter follow whichever issue dimension the
    caller is viewing -- filtering by an LLM category while the WHERE clause
    checks the agent's tag would silently return nothing.
    """
    assert issue_column in ISSUE_COLUMNS, issue_column
    return f"""
  WHERE resolved_at >= %(dt_from)s AND resolved_at < %(dt_to)s
    AND (%(store)s::text        IS NULL OR store        = %(store)s::text)
    AND (%(fulfillment)s::text  IS NULL OR fulfillment  = %(fulfillment)s::text)
    AND (%(issue)s::text        IS NULL OR {issue_column} = %(issue)s::text)
    AND (%(agent)s::text        IS NULL OR COALESCE(resolver_agent_name, agent_name) = %(agent)s::text)
    AND (%(resolution)s::text   IS NULL OR llm_resolution = %(resolution)s::text)
    AND (%(sentiment)s::text    IS NULL OR llm_sentiment  = %(sentiment)s::text)
    AND (%(cross_store)s::bool  IS NULL OR is_cross_store = %(cross_store)s::bool)
    AND (%(sla_breached)s::bool IS NULL OR
         (sla_response_breached OR sla_resolution_breached) = %(sla_breached)s::bool)
    AND (%(reopened)s::bool     IS NULL OR (resolve_seq > 1) = %(reopened)s::bool)
    AND (%(search)s::text       IS NULL OR
         order_id ILIKE %(search)s::text OR llm_summary ILIKE %(search)s::text
         OR conversation_id ILIKE %(search)s::text)
"""


def _params(f: dict) -> dict:
    search = f.get("search")
    return {
        "dt_from": f["dt_from"],
        "dt_to": f["dt_to"],
        "store": f.get("store"),
        "fulfillment": f.get("fulfillment"),
        "issue": f.get("issue"),
        "agent": f.get("agent"),
        "resolution": f.get("resolution"),
        "sentiment": f.get("sentiment"),
        "cross_store": f.get("cross_store"),
        "sla_breached": f.get("sla_breached"),
        "reopened": f.get("reopened"),
        "search": f"%{search}%" if search else None,
        "tz": config.TIMEZONE,
    }


COUNT_MODES = {
    # "One CART id can have 2 tickets with different agents" -- these let the
    # dashboard answer "how much work did we do" vs "how many distinct
    # customers/orders did we actually serve".
    "tickets": "COUNT(*)",
    "customers": "COUNT(DISTINCT COALESCE(customer_ref, conversation_id))",
    "orders": "COUNT(DISTINCT COALESCE(order_id, conversation_id))",
}


def count_expr(mode: str) -> str:
    return COUNT_MODES.get(mode, COUNT_MODES["tickets"])


def summary(f: dict, issue_column: str = "issue_type", count_mode: str = "tickets") -> dict:
    with cursor() as cur:
        cur.execute(
            f"""
            SELECT {count_expr(count_mode)}                                        AS resolved,
                   COUNT(*)                                                        AS tickets,
                   COUNT(DISTINCT COALESCE(customer_ref, conversation_id))          AS customers,
                   COUNT(DISTINCT order_id) FILTER (WHERE order_id IS NOT NULL)    AS orders,
                   COUNT(DISTINCT COALESCE(resolver_agent_id, agent_id))           AS agents,
                   COUNT(*) FILTER (WHERE resolve_seq > 1)                         AS reopened,
                   COUNT(*) FILTER (WHERE sla_response_breached)                   AS sla_response_breached,
                   COUNT(*) FILTER (WHERE sla_resolution_breached)                 AS sla_resolution_breached,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_response_secs)
                       FILTER (WHERE first_response_secs IS NOT NULL)              AS median_first_response_secs,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_secs)
                       FILTER (WHERE resolution_secs IS NOT NULL)                  AS median_secs,
                   AVG(resolution_secs)                                            AS avg_secs,
                   COUNT(*) FILTER (WHERE order_id IS NULL)                        AS no_order_id
              FROM resolve_events {_filter_sql(issue_column)}
            """,
            _params(f),
        )
        return cur.fetchone()


BREAKDOWN_COLUMNS = {
    "issue_type",
    "store",
    "fulfillment",
    "agent_name",
    "resolver_agent_name",
    "llm_issue",
    "llm_resolution",
    "llm_sentiment",
    "llm_sentiment_start",
    "llm_answered_ask",
    "llm_commitment",
    "llm_empathy",
    "llm_proactive",
    "priority",
    "is_cross_store",
}


def breakdown(f: dict, column: str, issue_column: str = "issue_type",
              count_mode: str = "tickets") -> list[dict]:
    assert column in BREAKDOWN_COLUMNS, f"not a breakdown column: {column}"
    with cursor() as cur:
        cur.execute(
            f"""
            SELECT COALESCE({column}::text, '(unattributed)') AS label,
                   {count_expr(count_mode)}                  AS count,
                   COUNT(*)                                  AS tickets,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_secs)
                       FILTER (WHERE resolution_secs IS NOT NULL) AS median_secs
              FROM resolve_events {_filter_sql(issue_column)}
             GROUP BY 1
             ORDER BY count DESC
            """,
            _params(f),
        )
        return cur.fetchall()


SERIES_COLUMNS = {
    "llm_issue", "issue_type", "fulfillment", "store",
    "agent_name", "resolver_agent_name", "llm_resolution", "llm_sentiment",
    "llm_answered_ask", "llm_commitment", "llm_empathy",
}


def median_by_day(f: dict, series_column: str, top_n: int,
                  issue_column: str = "issue_type") -> list[dict]:
    """Median resolution per (day, series), long tail collapsed into 'Other'.

    Page 3 uses this with ``series_column='fulfillment'`` and an agent filter:
    date down the Y axis, median time along X, one bar per instant/deferred.
    """
    assert series_column in SERIES_COLUMNS, series_column
    p = {**_params(f), "top_n": top_n}
    with cursor() as cur:
        cur.execute(
            f"""
            WITH scoped AS (
                SELECT (resolved_at AT TIME ZONE %(tz)s::text)::date AS day,
                       COALESCE({series_column}::text, 'Unknown')    AS series,
                       resolution_secs
                  FROM resolve_events {_filter_sql(issue_column)}
            ),
            ranked AS (
                SELECT series FROM scoped GROUP BY series
                 ORDER BY COUNT(*) DESC LIMIT %(top_n)s
            )
            SELECT day,
                   CASE WHEN s.series IN (SELECT series FROM ranked)
                        THEN s.series ELSE 'Other' END AS series,
                   COUNT(*) AS count,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_secs)
                       FILTER (WHERE resolution_secs IS NOT NULL) AS median_secs
              FROM scoped s
             GROUP BY day, 2
             ORDER BY day, 2
            """,
            p,
        )
        return cur.fetchall()


# Page 1: the ticket list. Ordered by an allow-listed column so the UI can sort.
SORTABLE = {
    "resolved_at", "resolution_secs", "first_response_secs", "agent_name",
    "resolver_agent_name", "issue_type", "llm_issue", "store", "fulfillment",
    "order_id", "msg_total", "resolve_seq", "max_dead_air_secs", "handoffs",
}


def tickets(f: dict, limit: int = 100, offset: int = 0,
            sort: str = "resolved_at", direction: str = "desc",
            issue_column: str = "issue_type") -> list[dict]:
    if sort not in SORTABLE:
        sort = "resolved_at"
    dirn = "ASC" if str(direction).lower() == "asc" else "DESC"
    with cursor() as cur:
        cur.execute(
            f"""
            SELECT conversation_id, conversation_url, resolved_at, conv_created_at,
                   resolution_secs, first_response_secs,
                   agent_name, resolver_agent_name,
                   customer_ref,
                   order_id, store, fulfillment, is_cross_store, n_batches,
                   issue_type, priority,
                   llm_issue, llm_resolution, llm_sentiment, llm_escalated,
                   llm_root_cause, llm_summary,
                   msg_total, msg_user, msg_agent, msg_private,
                   sla_response_breached, sla_resolution_breached,
                   max_dead_air_secs, handoffs, agent_inactive, repeated_canned,
                   closed_without_confirmation, personalised,
                   llm_sentiment_start, llm_answered_ask, llm_commitment,
                   llm_empathy, llm_proactive, llm_escalation_risk, llm_escalation_handled,
                   resolve_seq, enriched_at
              FROM resolve_events {_filter_sql(issue_column)}
             ORDER BY {sort} {dirn} NULLS LAST, id DESC
             LIMIT {int(limit)} OFFSET {int(offset)}
            """,
            _params(f),
        )
        return cur.fetchall()


def total_rows(f: dict, issue_column: str = "issue_type") -> int:
    with cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM resolve_events {_filter_sql(issue_column)}",
                    _params(f))
        return cur.fetchone()["n"]


def distinct_values() -> dict:
    """Filter options, taken from what is actually present in the data."""
    out: dict = {}
    with cursor() as cur:
        for key, col in (("issues_agent", "issue_type"), ("issues_llm", "llm_issue"),
                         ("stores", "store"), ("agents", "resolver_agent_name"),
                         ("resolutions", "llm_resolution")):
            cur.execute(
                f"SELECT DISTINCT {col} AS v FROM resolve_events "
                f"WHERE {col} IS NOT NULL ORDER BY 1"
            )
            out[key] = [r["v"] for r in cur.fetchall()]
    out["fulfillments"] = ["instant", "deferred"]
    out["count_modes"] = list(COUNT_MODES)
    return out
