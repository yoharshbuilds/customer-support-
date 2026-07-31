"""Backfill resolve events from a Freshchat "Conversation-Resolved" report.

The report has one row per resolve click -- the same event the webhook fires on
-- so we rebuild the webhook payload from it and run the identical insert and
enrichment path. Backfilled rows are therefore indistinguishable from live ones,
which is the whole point: what you review here is what you get in production.

    python -m scripts.backfill <report.csv> [--limit N] [--concurrency 4]

Rows are inserted oldest-first because ``resolve_seq`` counts prior resolves of
the same conversation; inserting newest-first would label the reopen as the
original.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time

import httpx
from anthropic import AsyncAnthropic

from app import db, enrich


def payload_for(row: dict) -> dict:
    """Rebuild the webhook body Freshchat would have sent for this resolve."""
    actor_id = row.get("actor_id") or row.get("agent_id") or None
    return {
        "actor": {"actor_type": (row.get("actor_type") or "agent").lower(), "actor_id": actor_id},
        "action": "conversation_resolution",
        "action_time": row["created_at"],
        "data": {
            "resolve": {
                "resolver": (row.get("actor_type") or "agent").lower(),
                "resolver_id": actor_id,
                "conversation": {
                    "conversation_id": row["conversation_id"],
                    "status": "resolved",
                },
            }
        },
        # Marks the row's provenance without changing how anything reads it.
        "_source": "backfill",
    }


async def one(client, llm, sem, row, counters) -> None:
    payload = payload_for(row)
    conversation_id = payload["data"]["resolve"]["conversation"]["conversation_id"]
    resolved_at = enrich.resolved_at_from(payload)

    row_id = db.insert_event(
        {
            "conversation_id": conversation_id,
            "resolved_at": resolved_at,
            "resolver_agent_id": payload["actor"]["actor_id"],
            "raw": payload,
        }
    )
    if row_id is None:
        counters["duplicate"] += 1
        return

    async with sem:
        try:
            fields = await enrich.enrich(
                client, conversation_id, resolved_at, llm=llm, payload_actor=payload["actor"]
            )
            db.update_enrichment(row_id, fields)
            counters["ok"] += 1
            print(f"  ok   {conversation_id}  store={fields.get('store')}  "
                  f"fulfil={fields.get('fulfillment')}  order={fields.get('order_id')}  "
                  f"issue={fields.get('llm_issue')}")
        except Exception as e:  # noqa: BLE001 — the sweeper retries these in production
            counters["failed"] += 1
            db.update_enrichment(row_id, {}, error=str(e)[:500])
            print(f"  FAIL {conversation_id}: {e}")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("report")
    ap.add_argument("--limit", type=int, default=0, help="0 = all rows")
    ap.add_argument("--concurrency", type=int, default=4)
    args = ap.parse_args()

    with open(args.report) as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("conversation_id") and r.get("created_at")]
    rows.sort(key=lambda r: r["created_at"])  # oldest first, for resolve_seq
    if args.limit:
        rows = rows[-args.limit:]  # most recent N, which is what you want to eyeball

    print(f"{len(rows)} resolve events to load (concurrency {args.concurrency})")
    db.init()

    llm = AsyncAnthropic() if os.getenv("ANTHROPIC_API_KEY") else None
    if llm is None:
        print("  ANTHROPIC_API_KEY unset — llm_* columns will be null")

    counters = {"ok": 0, "failed": 0, "duplicate": 0}
    sem = asyncio.Semaphore(args.concurrency)
    t0 = time.time()
    async with httpx.AsyncClient(timeout=60) as client:
        await asyncio.gather(*(one(client, llm, sem, r, counters) for r in rows))
    if llm is not None:
        await llm.close()

    print(f"\n{counters}  in {time.time() - t0:.1f}s")
    return 1 if counters["failed"] else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
