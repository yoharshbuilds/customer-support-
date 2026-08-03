"""Self-healing for missed resolution webhooks.

Measured problem: comparing our resolve_events against Freshchat's own
Conversation-Resolved export, we held only 88.4% of resolutions since capture
began -- 441 of 499. Two bad days did it: 31 Jul at 86.4% (17 missed) and 1 Aug
at 74.2% (41 missed), with 30 Jul, 2 Aug and 3 Aug all at 100%. Zero false
positives, so nothing we store is wrong; events were simply lost.

The likely cause is the host: a free-tier instance sleeps when idle and a restart
drops whatever webhook was in flight. Nothing in the webhook path can fix that,
because the request never arrives. So instead of trying to catch every delivery,
this reconciles after the fact: pull the authoritative list from the Extract API,
insert anything missing, and enrich it on the same code path the webhook uses.

Deliberately does NOT send Slack alerts. alerts.maybe_alert is only called from
the webhook path, so recovering a two-day-old resolve cannot page anyone about a
chat that has long since moved on.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from . import db, enrich, extract

log = logging.getLogger(__name__)

# Result of the most recent pass, surfaced on /healthz. A gap is only visible by
# comparison, so without this the next one would be as invisible as the last.
LAST: dict = {"ran_at": None, "seen": 0, "missing": 0, "recovered": 0}

# How far back each pass looks. Wider than the message ingest window because a
# missed resolve is only discoverable by comparison, and a gap can last hours if
# the host was down. Re-inserting is free -- insert_event dedupes on
# (conversation_id, resolved_at).
RECONCILE_LOOKBACK_HOURS = 24
RECONCILE_INTERVAL_SECONDS = 60 * 60
# Enrichment calls Freshchat and the model per row, so recovery is paced rather
# than firing hundreds of requests when a long gap is first discovered.
MAX_PER_PASS = 60
CONCURRENCY = 4


def payload_for(row: dict) -> dict:
    """Rebuild the webhook body Freshchat would have sent for this resolve.

    Same shape scripts/backfill.py builds, so a recovered row is
    indistinguishable from a live one -- which is the point.
    """
    actor_id = row.get("actor_id") or row.get("agent_id") or None
    actor_type = (row.get("actor_type") or "agent").lower()
    return {
        "actor": {"actor_type": actor_type, "actor_id": actor_id},
        "action": "conversation_resolution",
        "action_time": row["created_at"],
        "data": {
            "resolve": {
                "resolver": actor_type,
                "resolver_id": actor_id,
                "conversation": {
                    "conversation_id": row["conversation_id"],
                    "status": "resolved",
                },
            }
        },
        "_source": "reconcile",
    }


async def _recover_one(client, llm, sem, row, counters) -> None:
    payload = payload_for(row)
    cid = payload["data"]["resolve"]["conversation"]["conversation_id"]
    resolved_at = enrich.resolved_at_from(payload)
    row_id = db.insert_event({
        "conversation_id": cid,
        "resolved_at": resolved_at,
        "resolver_agent_id": payload["actor"]["actor_id"],
        "raw": payload,
    })
    if row_id is None:
        counters["already_had"] += 1
        return
    counters["recovered"] += 1
    async with sem:
        try:
            fields = await enrich.enrich(client, cid, resolved_at, llm=llm,
                                         payload_actor=payload["actor"])
            db.update_enrichment(row_id, fields)
            counters["enriched"] += 1
        except Exception as e:  # noqa: BLE001 -- row stays; the sweeper retries
            db.update_enrichment(row_id, {}, error=str(e)[:500])
            counters["enrich_failed"] += 1


async def run_once(client: httpx.AsyncClient, llm, hours: int = RECONCILE_LOOKBACK_HOURS,
                   max_rows: int = MAX_PER_PASS) -> dict:
    """Compare against Freshchat and recover anything we are missing."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    rows = await extract.fetch(client, extract.EVENT_CONVERSATION_RESOLVED,
                              start.strftime(fmt), now.strftime(fmt))
    # Oldest first: resolve_seq counts prior resolves of the same conversation,
    # so loading newest-first would label a reopen as the original.
    rows.sort(key=lambda r: r.get("created_at") or "")

    # Work out what is actually missing BEFORE spending the per-pass budget.
    # The first version processed rows oldest-first and burned 46 of its 80 slots
    # re-checking resolves we already held, so a real gap two days later never got
    # reached.
    # Match with a tolerance, NOT on an exact timestamp. Freshchat's export
    # created_at and the webhook's action_time describe the same resolve but differ
    # by around 110ms, so exact matching declared 25 resolves missing that we
    # already held and inserted a second copy of each. The UNIQUE constraint could
    # not catch it either, since the timestamps genuinely differ.
    with db.cursor() as cur:
        cur.execute("""
            SELECT conversation_id, resolved_at
            FROM resolve_events
            WHERE resolved_at >= %(start)s - interval '1 hour'
        """, {"start": start})
        have: set[tuple[str, int]] = set()
        for r in cur.fetchall():
            sec = int(r["resolved_at"].timestamp())
            # Register the neighbouring seconds too, so a sub-second difference
            # either side of a second boundary still matches.
            for d in (-1, 0, 1):
                have.add((r["conversation_id"], sec + d))

    counters = {"seen": len(rows), "already_had": 0, "recovered": 0,
                "enriched": 0, "enrich_failed": 0, "skipped_cap": 0}
    missing = []
    for r in rows:
        if not r.get("conversation_id") or not r.get("created_at"):
            continue
        ts = enrich.resolved_at_from(payload_for(r))
        if (r["conversation_id"], int(ts.timestamp())) in have:
            counters["already_had"] += 1
            continue
        missing.append(r)
    counters["missing"] = len(missing)
    if len(missing) > max_rows:
        counters["skipped_cap"] = len(missing) - max_rows
        missing = missing[:max_rows]

    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [_recover_one(client, llm, sem, r, counters) for r in missing]
    if tasks:
        await asyncio.gather(*tasks)

    LAST.clear()
    LAST.update({"ran_at": now.isoformat(timespec="seconds"),
                 "lookback_hours": hours, **counters})

    if counters["recovered"]:
        log.warning("reconcile recovered %d missed resolutions (%d enriched, %d failed)",
                    counters["recovered"], counters["enriched"], counters["enrich_failed"])
    else:
        log.info("reconcile: no gaps in the last %dh (%d checked)", hours, counters["seen"])
    return counters
